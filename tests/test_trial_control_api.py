"""Owned PostgreSQL checks for the Pod-bound trial authority boundary."""

import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier

import pytest
from automl_api.api.routes import attempt_control
from automl_api.db.session import get_session_factory
from automl_api.models.enums import AttemptStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    TrainingCandidate,
    WorkflowAttempt,
    WorkflowEvent,
)
from automl_api.services.evaluation_publication import _append
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import event, func, select, text
from sqlalchemy.orm import Session, sessionmaker
from test_attempt_identity import identity as identity
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_publication_postgres import publication as publication
from test_training_trial_enrollment import trial_scope as trial_scope


@pytest.fixture
def trial_api(identity, trial_scope, monkeypatch):
    _, token, k8s, review, pod, auth = identity
    engine, run_id, parent_id, _, _ = trial_scope
    app = FastAPI()
    app.include_router(attempt_control.router)
    factory = sessionmaker(bind=engine)
    app.state.trial_factory = factory
    app.dependency_overrides[get_session_factory] = lambda: factory
    monkeypatch.setattr(attempt_control, "KubernetesTrainingClient", lambda: k8s)
    client = TestClient(app)
    owner = str(uuid.uuid4())

    def call(action, *, child=None, owner_id=None, extra=None, bearer=None):
        data = {"run_id": str(run_id), "worker_id": owner_id or owner}
        if action == "enroll":
            data.update(candidate_name="Ridge", suggestion_id="cv", parameters={"alpha": 1})
        if child is not None:
            data["child_attempt_id"] = str(child)
        return client.post(
            f"/internal/attempts/{parent_id}/trials/{action}",
            json={**data, **(extra or {})},
            headers={"Authorization": "Bearer " + (token if bearer is None else bearer)},
        )

    yield trial_scope, call, client, (review, pod, auth)
    client.close()


def enroll(api, **kwargs):
    response = api[1]("enroll", **kwargs)
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    assert "fencing_token" not in response.text and "DATABASE_URL" not in response.text
    return uuid.UUID(response.json()["child_attempt_id"])


def terminal_rows(db, child):
    return list(
        db.scalars(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == child, WorkflowEvent.event_key == "trial.terminal"
            )
        )
    )


def test_enrollment_dispatch_identity_and_two_terminal_replays(trial_api):
    scope, call, _, _ = trial_api
    child = enroll(trial_api, extra={"dispatch_id": "tune-0"})
    assert enroll(trial_api, extra={"dispatch_id": "tune-0"}) == child
    assert call("start", child=child).json()["terminal"] is None
    assert call("heartbeat", child=child).status_code == 200
    result = {"scores": [0.9, 0.8]}
    response = call("finish", child=child, extra={"result": result})
    assert response.status_code == 200
    for _ in range(2):
        assert call("start", child=child).json()["terminal"] == {
            "status": "succeeded",
            "result": result,
        }
        assert call("finish", child=child, extra={"result": result}).status_code == 200
    with Session(scope[0]) as db:
        attempt = db.get(WorkflowAttempt, child)
        assert attempt.status == AttemptStatus.SUCCEEDED and attempt.terminal_cas_version == 1
        assert len(terminal_rows(db, child)) == 1
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowEvent)
                .where(
                    WorkflowEvent.attempt_id == child,
                    WorkflowEvent.event_key == "trial.dispatch:tune-0",
                )
            )
            == 1
        )


def test_competing_worker_and_other_pod_cannot_renew_or_publish(trial_api):
    _, call, _, (review, pod, _) = trial_api
    child = enroll(trial_api)
    assert call("start", child=child).status_code == 200
    for action, extra in (("start", {}), ("heartbeat", {}), ("finish", {"result": {"score": 0}})):
        assert call(action, child=child, owner_id=str(uuid.uuid4()), extra=extra).status_code == 401
    pod.metadata.uid = "another-pod"
    review.status.user.extra["authentication.kubernetes.io/pod-uid"] = ["another-pod"]
    for action, extra in (("start", {}), ("heartbeat", {}), ("finish", {"failure": "late"})):
        assert call(action, child=child, extra=extra).status_code == 401


def test_native_identity_io_releases_connections_and_row_locks(trial_api):
    scope, call, _, (review, _, auth) = trial_api
    engine, run_id, parent_id, _, _ = scope

    def verify(*args, **kwargs):
        assert engine.pool.checkedout() == 0
        with Session(engine) as db, db.begin():
            db.execute(
                select(WorkflowAttempt)
                .where(WorkflowAttempt.id == parent_id)
                .with_for_update(nowait=True)
            )
            db.execute(select(ModelRun).where(ModelRun.id == run_id).with_for_update(nowait=True))
        return review

    auth.create_token_review.side_effect = verify
    child = enroll(trial_api)
    assert call("start", child=child).status_code == 200
    assert call("finish", child=child, extra={"result": {"score": 1}}).status_code == 200


@pytest.mark.parametrize(
    "change",
    [
        "cancel",
        "revoke",
        "lease",
        "candidate",
        "run_params",
        "run_kind",
        "child_fence",
        "child_revoke",
    ],
)
def test_authority_changed_during_tokenreview_cannot_start(trial_api, change):
    scope, call, _, (review, _, auth) = trial_api
    engine, run_id, parent_id, _, candidate_id = scope
    child = enroll(trial_api)

    def verify(*args, **kwargs):
        with Session(engine) as db, db.begin():
            parent = db.get(WorkflowAttempt, parent_id)
            if change == "cancel":
                parent.status = AttemptStatus.CANCELLED
            elif change == "revoke":
                _append(
                    db,
                    parent,
                    "attempt.credentials.revoked",
                    {"reason": "owned trial control test"},
                )
            elif change == "lease":
                parent.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif change == "candidate":
                db.get(TrainingCandidate, candidate_id).params = {"altered": True}
            elif change == "run_params":
                db.get(ModelRun, run_id).params = {"altered": True}
            elif change == "run_kind":
                db.get(ModelRun, run_id).run_kind = "validation"
            elif change == "child_fence":
                db.get(WorkflowAttempt, child).fencing_token = "changed-child-fence"
            else:
                _append(db, db.get(WorkflowAttempt, child), "attempt.credentials.revoked", {})
        return review

    auth.create_token_review.side_effect = verify
    assert call("start", child=child).status_code == 401
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, child)
        assert attempt.status == AttemptStatus.PENDING and attempt.lease_owner is None
        assert attempt.retry_count == 0 and not terminal_rows(db, child)


@pytest.mark.parametrize(
    "extra",
    [
        {"run_id": str(uuid.UUID(int=1))},
        {"candidate_name": "Missing"},
        {"child_attempt_id": str(uuid.UUID(int=2))},
        {"fencing_token": "secret"},
        {"result": {}},
        {"parameters": None},
        {"dispatch_id": " "},
        {"parameters": {"bad": float("nan")}},
    ],
)
def test_closed_or_invalid_enrollment_cannot_create_child(trial_api, extra):
    scope, call, client, _ = trial_api
    # Serialize nonfinite data directly; the closed server schema must reject it.
    if "parameters" in extra and isinstance(extra["parameters"], dict):
        response = client.post(
            f"/internal/attempts/{scope[2]}/trials/enroll",
            content=json.dumps(
                {
                    "run_id": str(scope[1]),
                    "worker_id": str(uuid.uuid4()),
                    "candidate_name": "Ridge",
                    "suggestion_id": "cv",
                    **extra,
                }
            ),
        )
    else:
        response = call("enroll", extra=extra)
    assert response.status_code in {401, 422}
    with Session(scope[0]) as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowAttempt)
                .where(WorkflowAttempt.run_attempt_id == scope[2])
            )
            == 0
        )


def test_foreign_child_and_oversized_request_are_rejected(trial_api):
    scope, call, client, (_, _, auth) = trial_api
    assert call("start", child=uuid.uuid4()).status_code == 401
    auth.reset_mock()
    response = client.post(f"/internal/attempts/{scope[2]}/trials/enroll", content=b"x" * 262145)
    assert response.status_code == 413 and response.headers["cache-control"] == "no-store"
    auth.create_token_review.assert_not_called()


def test_concurrent_start_selects_one_owner(trial_api):
    scope, call, _, _ = trial_api
    child = enroll(trial_api)
    barrier = Barrier(2)

    def start(owner):
        barrier.wait(timeout=5)
        return call("start", child=child, owner_id=str(owner)).status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(start, [uuid.uuid4(), uuid.uuid4()])) == [200, 401]
    with Session(scope[0]) as db:
        assert db.get(WorkflowAttempt, child).retry_count == 1


def test_failure_receipt_is_immutable_and_replays(trial_api):
    scope, call, _, _ = trial_api
    child = enroll(trial_api)
    assert call("start", child=child).status_code == 200
    assert call("finish", child=child, extra={"failure": "fold invalid"}).status_code == 200
    assert call("start", child=child).json()["terminal"] == {
        "status": "failed",
        "failure": "fold invalid",
    }
    assert call("finish", child=child, extra={"failure": "different"}).status_code == 401
    with Session(scope[0]) as db:
        assert len(terminal_rows(db, child)) == 1
        assert db.get(WorkflowAttempt, child).terminal_cas_version == 1


def test_real_sql_loss_before_start_commit_rolls_back_claim(trial_api):
    scope, call, client, _ = trial_api
    engine = scope[0]
    child = enroll(trial_api)
    factory = client.app.state.trial_factory
    killed = []

    def disconnect(session):
        if killed or not any(
            isinstance(row, WorkflowAttempt) and row.id == child and row.lease_owner
            for row in session.identity_map.values()
        ):
            return
        pid = session.connection().scalar(text("SELECT pg_backend_pid()"))
        with engine.connect() as other:
            assert other.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid}) is True
        killed.append(pid)

    event.listen(factory.class_, "before_commit", disconnect)
    try:
        assert call("start", child=child).status_code == 503
    finally:
        event.remove(factory.class_, "before_commit", disconnect)
    assert len(killed) == 1
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, child)
        assert attempt.status == AttemptStatus.PENDING and attempt.retry_count == 0
    assert call("start", child=child).status_code == 200


def test_lost_terminal_commit_acknowledgement_replays_one_result(trial_api):
    scope, call, client, _ = trial_api
    engine = scope[0]
    child = enroll(trial_api)
    assert call("start", child=child).status_code == 200
    factory = client.app.state.trial_factory
    lost = []

    def lose_ack(session):
        if lost:
            return
        with Session(engine) as db:
            if db.get(WorkflowAttempt, child).terminal_cas_version != 1:
                return
        lost.append(True)
        raise ConnectionError("owned fixture lost SQL commit acknowledgement")

    event.listen(factory.class_, "after_commit", lose_ack)
    try:
        assert call("finish", child=child, extra={"result": {"score": 1}}).status_code == 503
    finally:
        event.remove(factory.class_, "after_commit", lose_ack)
    assert lost == [True]
    assert call("finish", child=child, extra={"result": {"score": 1}}).status_code == 200
    assert call("start", child=child).json()["terminal"]["result"] == {"score": 1}
    with Session(engine) as db:
        assert len(terminal_rows(db, child)) == 1
        assert db.get(WorkflowAttempt, child).terminal_cas_version == 1


def test_corrupt_terminal_cas_is_not_replayed(trial_api):
    scope, call, _, _ = trial_api
    child = enroll(trial_api)
    assert call("start", child=child).status_code == 200
    assert call("finish", child=child, extra={"result": {"score": 1}}).status_code == 200
    with Session(scope[0]) as db, db.begin():
        db.get(WorkflowAttempt, child).terminal_cas_version = 0
    assert call("start", child=child).status_code == 401


def test_existing_foreign_project_run_cannot_enroll(trial_api, monkeypatch):
    scope, call, _, _ = trial_api
    tenant = committed_tenant.__wrapped__(scope[0])
    other_owner = next(tenant)
    try:
        with monkeypatch.context() as patch:
            foreign = publication.__wrapped__(scope[0], other_owner, patch)
        with Session(scope[0]) as db:
            assert db.get(ModelRun, foreign[1]).project_id == other_owner[1]
        assert call("enroll", extra={"run_id": str(foreign[1])}).status_code == 401
        with Session(scope[0]) as db:
            assert (
                db.scalar(
                    select(func.count())
                    .select_from(WorkflowAttempt)
                    .where(WorkflowAttempt.run_attempt_id.in_([scope[2], foreign[2]]))
                )
                == 0
            )
    finally:
        with pytest.raises(StopIteration):
            next(tenant)


def test_existing_child_of_another_parent_cannot_start(trial_api):
    from automl_api.services.training_trials import enroll_candidate_evaluation

    scope, call, _, _ = trial_api
    engine, run_id, parent_id, _, _ = scope
    with Session(engine) as db, db.begin():
        parent = db.get(WorkflowAttempt, parent_id)
        other = WorkflowAttempt(
            project_id=parent.project_id,
            model_run_id=run_id,
            stage="training_run",
            logical_key="owned-other-parent",
            generation=1,
            fencing_token=uuid.uuid4().hex,
            workload_identity=parent.workload_identity,
            status=AttemptStatus.RUNNING,
        )
        db.add(other)
        db.flush()
        context = enroll_candidate_evaluation(
            db,
            run_id=run_id,
            project_id=parent.project_id,
            candidate_name="Ridge",
            suggestion_id="other-cv",
            parameters={"alpha": 1},
            parent=(other.id, other.fencing_token),
        )
        child = context["attempt_id"]
    assert call("start", child=child).status_code == 401
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, child)
        assert attempt.status == AttemptStatus.PENDING and attempt.retry_count == 0
        assert attempt.lease_owner is None
