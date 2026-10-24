"""Owned PostgreSQL controls for the parent lifecycle authority boundary."""

import time
import uuid
from datetime import UTC, datetime, timedelta

import jwt
import pytest
from automl_api.api.routes import attempt_control
from automl_api.db.session import get_session_factory
from automl_api.models.enums import AttemptStatus, RunStatus
from automl_api.models.runs import Metric, ModelRun
from automl_api.models.workflows import (
    WorkflowAttempt,
    WorkflowAttemptCredentialGrant,
    WorkflowEvent,
)
from automl_api.services.parent_control import PUBLISHER
from automl_api.training import pipeline
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import event, func, select, text
from sqlalchemy.orm import Session, sessionmaker
from test_attempt_identity import identity as identity
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_publication_postgres import publication as publication


@pytest.fixture
def control(identity, monkeypatch):
    scope, token, k8s, review, pod, auth = identity
    engine, run_id, attempt_id, _, _ = scope
    app = FastAPI()
    app.include_router(attempt_control.router)
    factory = sessionmaker(bind=engine)
    app.state.parent_factory = factory
    app.dependency_overrides[get_session_factory] = lambda: factory
    monkeypatch.setattr(attempt_control, "KubernetesTrainingClient", lambda: k8s)
    client = TestClient(app)
    owner = uuid.uuid4().hex

    def call(action, *, payload=None, bearer=None):
        return client.post(
            f"/internal/attempts/{attempt_id}/parent/{action}",
            json={
                "run_id": str(run_id),
                "worker_id": owner,
                **({"run_kind": call.run_kind} if action == "start" else {}),
                **(payload or {}),
            },
            headers={"Authorization": "Bearer " + (token if bearer is None else bearer)},
        )

    call.run_kind = "training"
    yield scope, call, client, owner, (review, pod, auth)
    client.close()


def test_context_start_heartbeat_and_owner_replay(control):
    scope, call, _, _, _ = control
    engine, run_id, attempt_id, _, _ = scope
    context = call("context")
    assert context.status_code == 200
    assert context.headers["cache-control"] == "no-store"
    assert context.json()["run_kind"] == "training"
    assert call("start").status_code == 200
    assert call("start").status_code == 200
    assert call("heartbeat").status_code == 200
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, attempt_id)
        assert attempt.status == AttemptStatus.RUNNING
        assert attempt.lease_owner.startswith("parent-")
        assert attempt.lease_expires_at > datetime.now(UTC)
        assert db.get(ModelRun, run_id).status == RunStatus.RUNNING
        assert (
            len(
                list(
                    db.scalars(
                        select(WorkflowEvent).where(
                            WorkflowEvent.attempt_id == attempt_id,
                            WorkflowEvent.event_key == PUBLISHER,
                        )
                    )
                )
            )
            == 1
        )


def test_other_worker_instance_cannot_take_lease_or_report_failure(control):
    scope, call, _, _, _ = control
    assert call("start").status_code == 200
    other = {"worker_id": str(uuid.uuid4())}
    for action in ("start", "heartbeat", "failure"):
        payload = {**other, **({"failure": "foreign failure"} if action == "failure" else {})}
        assert call(action, payload=payload).status_code == 401
    with Session(scope[0]) as db:
        assert db.get(WorkflowAttempt, scope[2]).status == AttemptStatus.RUNNING


def test_another_pod_cannot_reuse_the_worker_nonce(control):
    scope, call, _, _, native = control
    review, pod, _ = native
    assert call("start").status_code == 200
    pod.metadata.uid = "another-pod-uid"
    review.status.user.extra["authentication.kubernetes.io/pod-uid"] = [pod.metadata.uid]
    token = jwt.encode(
        {"exp": int(time.time()) + 601, "nonce": "another-pod"},
        "unit-test-only-key",
        algorithm="HS256",
    )
    assert call("heartbeat", bearer=token).status_code == 401
    with Session(scope[0]) as db:
        assert db.get(WorkflowAttempt, scope[2]).status == AttemptStatus.RUNNING


def test_token_rotation_preserves_same_pod_owner(control):
    _, call, _, _, _ = control
    assert call("start").status_code == 200
    token = jwt.encode({"exp": int(time.time()) + 650}, "unit-test-only-key", algorithm="HS256")
    assert call("heartbeat", bearer=token).status_code == 200


def test_native_verification_holds_no_sql_connection_or_parent_lock(control):
    scope, call, _, _, native = control
    engine, run_id, attempt_id, _, _ = scope
    _, _, auth = native
    original = auth.create_token_review.return_value
    checked = []

    def review(*args, **kwargs):
        assert engine.pool.checkedout() == 0
        with Session(engine) as db, db.begin():
            db.scalar(
                select(WorkflowAttempt)
                .where(WorkflowAttempt.id == attempt_id)
                .with_for_update(nowait=True)
            )
            db.scalar(select(ModelRun).where(ModelRun.id == run_id).with_for_update(nowait=True))
        checked.append(True)
        return original

    auth.create_token_review.side_effect = review
    assert call("start").status_code == 200
    assert checked == [True]


def test_cancellation_during_native_verification_prevents_claim(control):
    scope, call, _, _, native = control
    engine, run_id, attempt_id, _, _ = scope
    _, _, auth = native
    original = auth.create_token_review.return_value

    def cancel(*args, **kwargs):
        with Session(engine) as db, db.begin():
            db.get(WorkflowAttempt, attempt_id).status = AttemptStatus.CANCELLED
            db.get(ModelRun, run_id).status = RunStatus.CANCELLED
        return original

    auth.create_token_review.side_effect = cancel
    assert call("start").status_code == 401
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).lease_owner is None
        assert (
            db.scalar(
                select(WorkflowEvent.id).where(
                    WorkflowEvent.attempt_id == attempt_id, WorkflowEvent.event_key == PUBLISHER
                )
            )
            is None
        )


def test_partial_winner_cannot_be_completed_without_result_receipt(control):
    scope, call, _, _, _ = control
    assert call("start").status_code == 200
    with Session(scope[0]) as db, db.begin():
        run = db.get(ModelRun, scope[1])
        run.tags = {**run.tags, "winner_model_artifact_uri": "s3://fixture/partial"}
    assert call("complete").status_code == 401
    with Session(scope[0]) as db:
        assert db.get(WorkflowAttempt, scope[2]).terminal_cas_version == 0
        assert db.get(ModelRun, scope[1]).status == RunStatus.RUNNING


def test_result_completion_and_two_replays_preserve_one_terminal_fact(control):
    scope, call, _, _, _ = control
    engine, run_id, attempt_id, _, result = scope
    assert call("start").status_code == 200
    assert pipeline._persist_training_success(run_id, result, "owned-tracking-id")
    first = call("complete")
    assert first.status_code == 200 and first.json()["completed"] is True
    for _ in range(2):
        replay = call("complete")
        assert replay.status_code == 200 and replay.json()["completed"] is True
        assert replay.json()["changed"] is False
        assert "s3://test/winner" not in replay.text
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 1
        assert db.get(ModelRun, run_id).status == RunStatus.SUCCEEDED
        assert (
            db.scalar(select(func.count()).select_from(Metric).where(Metric.model_run_id == run_id))
            == 1
        )
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowAttemptCredentialGrant)
                .where(WorkflowAttemptCredentialGrant.attempt_id == attempt_id)
            )
            == 0
        )


def test_known_result_recovers_after_lease_expiry_and_revocation_without_new_work(control):
    scope, call, _, _, _ = control
    engine, run_id, attempt_id, _, result = scope
    assert call("start").status_code == 200
    assert pipeline._persist_training_success(run_id, result, "owned-tracking-id")
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, attempt_id).lease_expires_at = datetime.now(UTC) - timedelta(
            seconds=1
        )
        db.add(
            WorkflowEvent(
                project_id=db.get(ModelRun, run_id).project_id,
                attempt_id=attempt_id,
                sequence=20,
                event_key="attempt.credentials.revoked",
                event_type="attempt.credentials.revoked",
                payload={},
            )
        )
    assert call("heartbeat").status_code == 401
    reply = call("receipt")
    assert reply.status_code == 200 and reply.json()["completed"] is True
    assert call("start").json()["completed"] is True


def test_failure_acknowledgement_replays_without_rewriting_reason(control):
    scope, call, _, _, _ = control
    assert call("start").status_code == 200
    first = call("failure", payload={"failure": "fit failed"})
    assert first.status_code == 200 and first.json()["changed"] is True
    replay = call("failure", payload={"failure": "another explanation"})
    assert replay.status_code == 200 and replay.json()["changed"] is False
    with Session(scope[0]) as db:
        assert db.get(ModelRun, scope[1]).failure_message == "fit failed"
        assert db.get(WorkflowAttempt, scope[2]).terminal_cas_version == 1


@pytest.mark.parametrize("invalid", ["extra", "wrong_run", "bad_worker", "failure_on_start"])
def test_invalid_scope_or_closed_metadata_never_starts(control, invalid):
    scope, call, _, _, _ = control
    payload = {"extra": {"object_uri": "s3://forbidden/raw"}}
    expected = 422
    if invalid == "wrong_run":
        payload, expected = {"run_id": str(uuid.uuid4())}, 401
    elif invalid == "bad_worker":
        payload = {"worker_id": "not-a-uuid"}
    elif invalid == "failure_on_start":
        payload, expected = {"failure": "unexpected"}, 401
    reply = call("start", payload=payload)
    assert reply.status_code == expected
    assert reply.headers["cache-control"] == "no-store"
    with Session(scope[0]) as db:
        assert db.get(WorkflowAttempt, scope[2]).lease_owner is None


def test_oversized_metadata_is_rejected_before_authentication(control):
    scope, _, client, _, native = control
    _, _, auth = native
    reply = client.post(f"/internal/attempts/{scope[2]}/parent/start", content=b"x" * 8193)
    assert reply.status_code == 413
    auth.create_token_review.assert_not_called()


@pytest.mark.parametrize("kind", ["validation", "explainability", "drift"])
def test_analysis_terminal_receipt_is_recoverable_through_parent_control(control, kind):
    scope, call, _, _, _ = control
    engine, run_id, attempt_id, _, _ = scope
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        run.run_kind = kind
        db.get(WorkflowAttempt, attempt_id).logical_key = f"analysis-run:{run_id}"
    call.run_kind = kind
    assert call("context").json()["run_kind"] == kind
    assert call("start").status_code == 200
    with Session(engine) as db, db.begin():
        run, attempt = db.get(ModelRun, run_id), db.get(WorkflowAttempt, attempt_id)
        uri = f"s3://owned-fixture/{run_id}/analysis.json"
        run.tags = {
            **run.tags,
            "artifact_uri": uri,
            "analysis_result_receipt": {
                "attempt_id": str(attempt_id),
                "digest": "c" * 64,
                "artifact_uri": uri,
            },
        }
        run.status = RunStatus.SUCCEEDED
        attempt.status = AttemptStatus.SUCCEEDED
        attempt.terminal_cas_version, attempt.checkpoint_uri = 1, uri
    for action in ("receipt", "complete", "start"):
        reply = call(action)
        assert reply.status_code == 200 and reply.json()["completed"] is True
        assert reply.json()["changed"] is False
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 1


@pytest.mark.parametrize("invalid", ["attempt", "digest", "uri", "extra"])
def test_malformed_stored_result_cannot_authorize_completion(control, invalid):
    scope, call, _, _, _ = control
    engine, run_id, attempt_id, _, result = scope
    assert call("start").status_code == 200
    assert pipeline._persist_training_success(run_id, result, "owned-tracking")
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        receipt = dict(run.tags["training_result_receipt"])
        if invalid == "attempt":
            receipt["attempt_id"] = str(uuid.uuid4())
        elif invalid == "digest":
            receipt["digest"] = "invalid"
        elif invalid == "extra":
            receipt["new_argument"] = "forbidden"
        run.tags = {
            **run.tags,
            "training_result_receipt": receipt,
            **({"winner_model_artifact_uri": None} if invalid == "uri" else {}),
        }
    assert call("complete").status_code == 401
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 0
        assert db.get(ModelRun, run_id).status == RunStatus.RUNNING


def test_concurrent_parent_claims_choose_one_worker(control):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    scope, call, _, _, _ = control
    barrier = Barrier(2)

    def start(owner):
        barrier.wait(timeout=5)
        return call("start", payload={"worker_id": str(owner)}).status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        statuses = sorted(pool.map(start, [uuid.uuid4(), uuid.uuid4()]))
    assert statuses == [200, 401]
    with Session(scope[0]) as db:
        assert (
            len(
                list(
                    db.scalars(
                        select(WorkflowEvent).where(
                            WorkflowEvent.attempt_id == scope[2],
                            WorkflowEvent.event_key == PUBLISHER,
                        )
                    )
                )
            )
            == 1
        )


def test_real_postgresql_loss_before_claim_commit_rolls_back_owner_and_publisher(control):
    scope, call, client, _, _ = control
    engine, _, attempt_id, _, _ = scope
    factory = client.app.state.parent_factory
    killed = []

    def disconnect(session):
        if killed:
            return
        parents = [
            row
            for row in session.identity_map.values()
            if isinstance(row, WorkflowAttempt)
            and row.id == attempt_id
            and (row.lease_owner or "").startswith("parent-")
        ]
        if not parents:
            return
        pid = session.connection().scalar(text("SELECT pg_backend_pid()"))
        with engine.connect() as other:
            assert other.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid}) is True
        killed.append(pid)

    event.listen(factory.class_, "before_commit", disconnect)
    try:
        assert call("start").status_code == 503
    finally:
        event.remove(factory.class_, "before_commit", disconnect)
    assert len(killed) == 1
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).lease_owner is None
        assert (
            db.scalar(
                select(WorkflowEvent.id).where(
                    WorkflowEvent.attempt_id == attempt_id, WorkflowEvent.event_key == PUBLISHER
                )
            )
            is None
        )
    assert call("start").status_code == 200


def test_lost_postgresql_commit_ack_replays_same_parent_claim(control):
    scope, call, client, _, _ = control
    engine, _, attempt_id, _, _ = scope
    factory = client.app.state.parent_factory
    lost = []

    def lose_ack(session):
        if lost:
            return
        with Session(engine) as observed:
            current = observed.get(WorkflowAttempt, attempt_id)
            if not (current.lease_owner or "").startswith("parent-"):
                return
        lost.append(True)
        raise ConnectionError("owned fixture lost commit acknowledgement")

    event.listen(factory.class_, "after_commit", lose_ack)
    try:
        assert call("start").status_code == 503
    finally:
        event.remove(factory.class_, "after_commit", lose_ack)
    assert lost == [True]
    assert call("start").status_code == 200
    with Session(engine) as db:
        assert (
            len(
                list(
                    db.scalars(
                        select(WorkflowEvent).where(
                            WorkflowEvent.attempt_id == attempt_id,
                            WorkflowEvent.event_key == PUBLISHER,
                        )
                    )
                )
            )
            == 1
        )


def test_existing_analysis_failure_is_acknowledged_without_rewriting_history(control, monkeypatch):
    from automl_api.training import analysis

    scope, call, _, _, _ = control
    engine, run_id, attempt_id, _, _ = scope
    with Session(engine) as db, db.begin():
        db.get(ModelRun, run_id).run_kind = "validation"
        db.get(WorkflowAttempt, attempt_id).logical_key = f"analysis-run:{run_id}"
    call.run_kind = "validation"
    monkeypatch.setattr(analysis, "get_session_factory", lambda: lambda: Session(engine))
    assert call("start").status_code == 200
    analysis._mark_analysis_failed(run_id, RuntimeError("column mismatch"))
    reply = call("failure", payload={"failure": "late error acknowledgement"})
    assert reply.status_code == 200 and reply.json()["changed"] is False
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        assert run.failure_code == "ANALYSIS_FAILED" and run.failure_message == "column mismatch"
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 1


def test_result_target_changed_during_identity_verification_cannot_be_completed(control):
    import copy

    scope, call, _, _, native = control
    engine, run_id, attempt_id, _, result = scope
    _, _, auth = native
    assert call("start").status_code == 200
    assert pipeline._persist_training_success(run_id, result, "owned-tracking")
    original = auth.create_token_review.return_value

    def substitute(*args, **kwargs):
        with Session(engine) as db, db.begin():
            run = db.get(ModelRun, run_id)
            tags = copy.deepcopy(run.tags)
            tags["winner_model_artifact_uri"] = "s3://foreign/replacement"
            tags["leaderboard"][0]["model_artifact_uri"] = tags["winner_model_artifact_uri"]
            run.tags = tags
        return original

    auth.create_token_review.side_effect = substitute
    assert call("complete").status_code == 401
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 0


def test_kind_change_between_context_and_start_cannot_acquire_parent(control):
    scope, call, _, _, _ = control
    assert call("context").json()["run_kind"] == "training"
    with Session(scope[0]) as db, db.begin():
        db.get(ModelRun, scope[1]).run_kind = "validation"
        db.get(WorkflowAttempt, scope[2]).logical_key = f"analysis-run:{scope[1]}"
    assert call("start").status_code == 401
    with Session(scope[0]) as db:
        assert db.get(WorkflowAttempt, scope[2]).lease_owner is None
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowEvent)
                .where(WorkflowEvent.attempt_id == scope[2], WorkflowEvent.event_key == PUBLISHER)
            )
            == 0
        )
