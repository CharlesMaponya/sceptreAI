"""Sealed PostgreSQL input-graph controls for the training context boundary."""

import time
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import jwt
import pytest
from automl_api.api.routes import attempt_control
from automl_api.db.session import get_session_factory
from automl_api.models.enums import AttemptStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import DatasetSplitRevision, WorkflowAttempt
from automl_api.services import attempt_identity
from automl_api.services.evaluation_publication import _append
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import event, select, text
from sqlalchemy.orm import Session, sessionmaker
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_selected_identity import capacity_client as capacity_client
from test_selected_identity import selected_scope as selected_scope
from test_typed_feature_contracts import bound_run as bound_run


@pytest.fixture
def context_api(bound_run, monkeypatch):
    engine, run_id = bound_run
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        run.k8s_namespace = "context-test"
        attempt = db.scalar(select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == run_id))
        attempt.status, attempt.ray_job_name = AttemptStatus.SUBMITTED, "owned-context"
        attempt_id, fence, project_id = attempt.id, attempt.fencing_token, attempt.project_id
        pod = SimpleNamespace(
            metadata=SimpleNamespace(
                uid="context-pod",
                name="context-pod",
                namespace=run.k8s_namespace,
                deletion_timestamp=None,
                labels={
                    "automl.platform/project-id": str(project_id),
                    "automl.platform/attempt-id": str(attempt_id),
                    "automl.platform/generation": str(attempt.generation),
                },
                owner_references=[],
            ),
            spec=SimpleNamespace(service_account_name=attempt.workload_identity),
        )
        user = SimpleNamespace(
            username=f"system:serviceaccount:{run.k8s_namespace}:{attempt.workload_identity}",
            extra={
                "authentication.kubernetes.io/pod-name": [pod.metadata.name],
                "authentication.kubernetes.io/pod-uid": [pod.metadata.uid],
            },
        )
    token = jwt.encode(
        {"exp": int(time.time()) + 600},
        "context-unit-test-key-at-least-32-bytes",
        algorithm="HS256",
    )
    review = SimpleNamespace(
        status=SimpleNamespace(
            authenticated=True,
            audiences=[attempt_identity.attempt_audience(attempt_id, fence)],
            user=user,
        )
    )
    auth, core = MagicMock(), MagicMock()
    auth.create_token_review.return_value = review
    core.read_namespaced_pod.return_value = pod
    monkeypatch.setattr(attempt_identity.client, "AuthenticationV1Api", lambda: auth)
    monkeypatch.setattr(
        attempt_control,
        "KubernetesTrainingClient",
        lambda: SimpleNamespace(_configured=True, core=core),
    )
    factory = sessionmaker(bind=engine)
    app = FastAPI()
    app.include_router(attempt_control.router)
    app.dependency_overrides[get_session_factory] = lambda: factory
    app.state.context_factory = factory
    client = TestClient(app)
    headers = {"Authorization": "Bearer " + token}
    response = client.post(
        f"/internal/attempts/{attempt_id}/parent/start",
        json={"run_id": str(run_id), "worker_id": str(uuid.uuid4()), "run_kind": "training"},
        headers=headers,
    )
    assert response.status_code == 200, response.text

    def call(*, extra=None):
        return client.post(
            f"/internal/attempts/{attempt_id}/training/context",
            json={"run_id": str(run_id), **(extra or {})},
            headers=headers,
        )

    yield (engine, run_id, attempt_id, fence, project_id), call, client, (review, pod, auth)
    client.close()


def test_sealed_context_is_closed_safe_and_replayable(context_api):
    scope, call, _, _ = context_api
    with Session(scope[0]) as db, db.begin():
        run = db.get(ModelRun, scope[1])
        run.params = {
            **run.params,
            "DATABASE_URL": "never-export",
            "final_input": "s3://forbidden/final",
        }
        run.tags = {**run.tags, "final_uri": "s3://forbidden/final"}
        entries = [
            {**entry, "worker_pod": "preserved-pod", "unapproved_location": "s3://forbidden/final"}
            for entry in run.tags["leaderboard"]
        ]
        run.tags = {**run.tags, "leaderboard": entries}
        started = run.started_at
    for _ in range(3):
        response = call()
        assert response.status_code == 200, response.text
        assert response.headers["cache-control"] == "no-store"
        data = response.json()
        assert data["approved_columns"] == ["x", "time", "target"]
        assert data["run"]["tags"]["leaderboard"][0]["worker_pod"] == "preserved-pod"
        assert data["run"]["id"] == str(scope[1]) and data["attempt_id"] == str(scope[2])
        assert set(data["split"]["split_counts"]) == {"train", "validation"}
        assert "s3://identity-fixture/final" not in response.text
        assert "never-export" not in response.text and "forbidden/final" not in response.text
    with Session(scope[0]) as db:
        assert db.get(ModelRun, scope[1]).started_at == started


def test_native_identity_verification_holds_no_sql_connection_or_locks(context_api):
    scope, call, _, (review, _, auth) = context_api

    def verify(*args, **kwargs):
        assert scope[0].pool.checkedout() == 0
        with Session(scope[0]) as db, db.begin():
            db.execute(
                select(WorkflowAttempt)
                .where(WorkflowAttempt.id == scope[2])
                .with_for_update(nowait=True)
            )
            db.execute(select(ModelRun).where(ModelRun.id == scope[1]).with_for_update(nowait=True))
        return review

    auth.create_token_review.side_effect = verify
    assert call().status_code == 200


@pytest.mark.parametrize("change", ["cancel", "revoke", "lease", "run_params", "split", "catalog"])
def test_changed_authority_during_native_io_cannot_return_plan(context_api, change):
    scope, call, _, (review, _, auth) = context_api

    def verify(*args, **kwargs):
        with Session(scope[0]) as db, db.begin():
            attempt = db.get(WorkflowAttempt, scope[2])
            run = db.get(ModelRun, scope[1])
            if change == "cancel":
                attempt.status = AttemptStatus.CANCELLED
            elif change == "revoke":
                _append(db, attempt, "attempt.credentials.revoked", {})
            elif change == "lease":
                attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif change == "run_params":
                run.params = {**run.params, "sample_tier_rows": 123}
            elif change == "split":
                split = db.get(DatasetSplitRevision, uuid.UUID(run.params["split_revision_id"]))
                split.specification = {**split.specification, "row_set_digest": "changed-source"}
            else:
                run.params = {**run.params, "catalog_content_digest": "0" * 64}
        return review

    auth.create_token_review.side_effect = verify
    assert call().status_code == 401


def test_another_valid_pod_cannot_read_the_parent_publishers_context(context_api):
    _, call, _, (review, pod, _) = context_api
    pod.metadata.uid = "other-valid-pod"
    review.status.user.extra["authentication.kubernetes.io/pod-uid"] = [pod.metadata.uid]
    assert call().status_code == 401


@pytest.mark.parametrize(
    "extra",
    [
        {"run_id": str(uuid.UUID(int=1))},
        {"project_id": str(uuid.UUID(int=2))},
        {"fencing_token": "not-a-request-field"},
    ],
)
def test_wrong_or_open_request_does_not_disclose_context(context_api, extra):
    _, call, _, _ = context_api
    assert call(extra=extra).status_code in {401, 422}


def test_body_budget_is_enforced_before_authentication(context_api):
    scope, _, client, (_, _, auth) = context_api
    auth.reset_mock()
    response = client.post(f"/internal/attempts/{scope[2]}/training/context", content=b"x" * 4097)
    assert response.status_code == 413 and response.headers["cache-control"] == "no-store"
    auth.create_token_review.assert_not_called()


def test_real_sql_loss_before_initialization_commit_rolls_back(context_api):
    scope, call, client, _ = context_api
    with Session(scope[0]) as db, db.begin():
        db.get(ModelRun, scope[1]).failure_code = "owned-diagnostic"
    factory = client.app.state.context_factory
    killed = []

    def before_flush(session, *_):
        if any(
            isinstance(row, ModelRun) and row.id == scope[1] and row.failure_code is None
            for row in session.dirty
        ):
            session.info["context_initialized"] = True

    def disconnect(session):
        if killed or not session.info.get("context_initialized"):
            return
        pid = session.connection().scalar(text("SELECT pg_backend_pid()"))
        with scope[0].connect() as other:
            assert other.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid}) is True
        killed.append(pid)

    event.listen(factory.class_, "before_flush", before_flush)
    event.listen(factory.class_, "before_commit", disconnect)
    try:
        assert call().status_code == 503
    finally:
        event.remove(factory.class_, "before_commit", disconnect)
        event.remove(factory.class_, "before_flush", before_flush)
    assert len(killed) == 1
    with Session(scope[0]) as db:
        assert db.get(ModelRun, scope[1]).failure_code == "owned-diagnostic"
    assert call().status_code == 200


def test_lost_initialization_commit_ack_replays_without_resetting_start(context_api):
    scope, call, client, _ = context_api
    with Session(scope[0]) as db, db.begin():
        run = db.get(ModelRun, scope[1])
        run.failure_code = "owned-diagnostic"
        started = run.started_at
    factory = client.app.state.context_factory
    lost = []

    def lose_ack(session):
        if lost:
            return
        with Session(scope[0]) as db:
            if db.get(ModelRun, scope[1]).failure_code is not None:
                return
        lost.append(True)
        raise ConnectionError("owned initialization acknowledgement lost")

    event.listen(factory.class_, "after_commit", lose_ack)
    try:
        assert call().status_code == 503
    finally:
        event.remove(factory.class_, "after_commit", lose_ack)
    assert lost == [True] and call().status_code == 200
    with Session(scope[0]) as db:
        assert db.get(ModelRun, scope[1]).started_at == started


@pytest.mark.parametrize(
    "missing",
    ["estimator_catalog_revision_id", "experiment_spec_revision_id", "feature_recipe_revision_id"],
)
def test_api_requires_bound_graph_even_without_worker_environment(context_api, missing):
    scope, call, _, _ = context_api
    with Session(scope[0]) as db, db.begin():
        run = db.get(ModelRun, scope[1])
        run.params = {key: value for key, value in run.params.items() if key != missing}
    assert call().status_code == 401


def test_context_cannot_be_read_without_parent_publisher(context_api):
    from automl_api.models.workflows import WorkflowEvent
    from automl_api.services.parent_control import PUBLISHER
    from sqlalchemy import delete

    scope, call, _, _ = context_api
    with Session(scope[0]) as db, db.begin():
        db.execute(
            delete(WorkflowEvent).where(
                WorkflowEvent.attempt_id == scope[2], WorkflowEvent.event_key == PUBLISHER
            )
        )
    assert call().status_code == 401


def test_context_does_not_restart_a_stored_result(context_api):
    scope, call, _, _ = context_api
    with Session(scope[0]) as db, db.begin():
        run = db.get(ModelRun, scope[1])
        run.tags = {
            **run.tags,
            "training_result_receipt": {"attempt_id": str(scope[2]), "digest": "a" * 64},
        }
    assert call().status_code == 401


def test_provider_marker_cannot_fall_back_to_unscoped_reads(context_api):
    scope, call, _, _ = context_api
    with Session(scope[0]) as db, db.begin():
        run = db.get(ModelRun, scope[1])
        split = db.get(DatasetSplitRevision, uuid.UUID(run.params["split_revision_id"]))
        split.specification = {**split.specification, "provider_import_id": str(uuid.uuid4())}
    assert call().status_code == 401


def test_oversized_context_is_rejected_before_native_identity_io(context_api):
    scope, call, _, (_, _, auth) = context_api
    with Session(scope[0]) as db, db.begin():
        run = db.get(ModelRun, scope[1])
        entries = [
            {**entry, "diagnostics": {"oversized": "x" * (16 * 1024 * 1024)}}
            for entry in run.tags["leaderboard"]
        ]
        run.tags = {**run.tags, "leaderboard": entries}
    auth.reset_mock()
    assert call().status_code == 401
    auth.create_token_review.assert_not_called()


def test_existing_foreign_project_run_cannot_supply_context(context_api, monkeypatch):
    from test_training_publication_postgres import publication

    scope, call, _, _ = context_api
    other = committed_tenant.__wrapped__(scope[0])
    owner = next(other)
    try:
        with monkeypatch.context() as patch:
            foreign = publication.__wrapped__(scope[0], owner, patch)
        with Session(scope[0]) as db:
            assert db.get(ModelRun, foreign[1]).project_id == owner[1]
        assert call(extra={"run_id": str(foreign[1])}).status_code == 401
    finally:
        with pytest.raises(StopIteration):
            next(other)


@pytest.mark.parametrize(
    "alias",
    [
        "train_final",
        "validation_final",
        "encoded_final",
        "same_safe_roles",
        "directory_final",
        "directory_safe",
        "trailing_slash",
    ],
)
def test_aliased_prepared_roles_cannot_disclose_final_locations(context_api, alias):
    scope, call, _, (_, _, auth) = context_api
    with Session(scope[0]) as db, db.begin():
        run = db.get(ModelRun, scope[1])
        split = db.get(DatasetSplitRevision, uuid.UUID(run.params["split_revision_id"]))
        uris = dict(split.specification["uris"])
        if alias == "train_final":
            uris["train"] = uris["final_input"]
        elif alias == "validation_final":
            uris["validation"] = uris["final_input"]
        elif alias == "encoded_final":
            uris["train"] = uris["final_input"].replace("/final", "/%66inal")
        elif alias == "directory_final":
            uris["train"] = "s3://identity-fixture/"
        elif alias == "directory_safe":
            uris["train"] = uris["validation"].rsplit("/", 1)[0]
        elif alias == "trailing_slash":
            uris["train"] = uris["final_input"] + "/"
        else:
            uris["validation"] = uris["train"]
        split.specification = {**split.specification, "uris": uris}
    auth.reset_mock()
    assert call().status_code == 401
    auth.create_token_review.assert_not_called()
