"""Committed HTTP monitoring requests preserve observation identity and sequence."""

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from queue import Queue
from threading import Event
from time import monotonic, sleep
from types import SimpleNamespace

import pytest
from automl_api.api.deps import get_current_user
from automl_api.api.routes import monitoring
from automl_api.db.session import get_db
from automl_api.main import app as production_app
from automl_api.models.datasets import Dataset, DatasetVersion
from automl_api.models.enums import ProjectRole, RunKind, RunStatus, TaskType
from automl_api.models.iam import User
from automl_api.models.projects import ProjectMembership
from automl_api.models.runs import Metric, ModelRun
from automl_api.models.workflows import WorkflowCommand
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient
from sqlalchemy import delete, event, select, text
from sqlalchemy.orm import Session

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.fixture
def ingestion(phase1_engine, committed_tenant):
    engine = phase1_engine
    user_id, project_id = committed_tenant
    with Session(engine) as db, db.begin():
        dataset = Dataset(project_id=project_id, created_by_id=user_id, name="monitoring fixture")
        db.add(dataset)
        db.flush()
        version = DatasetVersion(
            project_id=project_id,
            dataset_id=dataset.id,
            created_by_id=user_id,
            version_number=1,
            format="CSV",
            object_uri="embedded://fixture/source.csv",
            content_hash="a" * 64,
        )
        db.add(version)
        db.flush()
        deployment = ModelRun(
            project_id=project_id,
            dataset_version_id=version.id,
            created_by_id=user_id,
            run_kind=RunKind.DEPLOYMENT,
            task_type=TaskType.REGRESSION,
            status=RunStatus.SUCCEEDED,
            params={},
            tags={},
        )
        db.add(deployment)
        db.flush()
        deployment_id = deployment.id
    c = SimpleNamespace(
        engine=engine,
        user_id=user_id,
        project_id=project_id,
        deployment_id=deployment_id,
        pids=Queue(),
        capture_pids=False,
        extra_users=[],
    )
    app = FastAPI()
    app.add_exception_handler(
        RequestValidationError, production_app.exception_handlers[RequestValidationError]
    )
    app.include_router(monitoring.router, prefix="/api/v1")

    def database():
        with Session(engine, autoflush=False, expire_on_commit=False) as db:
            if c.capture_pids:
                c.pids.put(db.scalar(text("SELECT pg_backend_pid()")))
            yield db

    def user():
        with Session(engine) as db:
            return db.get(User, c.user_id)

    app.dependency_overrides[get_db] = database
    app.dependency_overrides[get_current_user] = user
    c.path = f"/api/v1/projects/{project_id}/operations/deployments/{deployment_id}/monitoring"

    def send(payload, key="canonical", *, configuration=False, raw=False):
        with TestClient(app, raise_server_exceptions=False) as client:
            kwargs = {"content": payload} if raw else {"json": payload}
            return client.request(
                "PUT" if configuration else "POST",
                c.path + ("/config" if configuration else "/metrics"),
                headers={"Idempotency-Key": key, "Content-Type": "application/json"},
                **kwargs,
            )

    c.send = send
    try:
        yield c
    finally:
        with Session(engine) as db, db.begin():
            db.execute(delete(User).where(User.id.in_(c.extra_users)))


def payload(**changes):
    return {"name": "error_rate", "value": 0.01, "sample_count": 10, **changes}


def rows(c, model):
    with Session(c.engine) as db:
        return list(db.scalars(select(model).where(model.project_id == c.project_id)))


def test_monitoring_http_canonical_replay_and_conflict(ingestion):
    c = ingestion
    first = c.send(payload())
    assert first.status_code == 201, first.text
    assert c.send(payload()).json() == first.json()
    conflict = c.send(payload(value=0.02))
    assert (
        conflict.status_code == 409
        and conflict.json()["detail"]["code"] == "idempotency_key_reused"
    )
    assert len(rows(c, Metric)) == len(rows(c, WorkflowCommand)) == 1


def test_monitoring_http_body_key_cannot_hide_changed_payload(ingestion):
    c = ingestion
    first = c.send(payload(idempotency_key="observation"), "first")
    assert first.status_code == 201
    same = c.send(payload(idempotency_key="observation"), "second")
    assert same.status_code == 201 and same.json() == first.json()
    changed = c.send(payload(value=0.02, idempotency_key="observation"), "third")
    assert changed.status_code == 409, changed.text
    assert len(rows(c, Metric)) == 1 and len(rows(c, WorkflowCommand)) == 2
    assert rows(c, Metric)[0].value == 0.01


@pytest.mark.parametrize(
    "changed",
    [
        {"kind": "drift"},
        {"sample_count": 11},
        {"status": "warning"},
        {"higher_is_better": True},
        {"recorded_at": "2026-10-19T10:00:00Z"},
        {"metadata": {"source": "changed"}},
    ],
)
def test_monitoring_body_key_hash_covers_all_request_fields(ingestion, changed):
    c = ingestion
    request = payload(idempotency_key="observation")
    assert c.send(request).status_code == 201
    response = c.send({**request, **changed}, "different-header")
    assert response.status_code == 409, response.text
    assert len(rows(c, Metric)) == len(rows(c, WorkflowCommand)) == 1


@pytest.mark.parametrize(
    "recorded_at",
    [
        "2026-10-19T10:00:00",
        "2026-10-19T10:00:00Z",
        "2026-10-19T12:00:00+02:00",
    ],
)
def test_monitoring_preserves_supplied_timestamps_on_replay(ingestion, recorded_at):
    c = ingestion
    request = payload(idempotency_key="timestamp", recorded_at=recorded_at)
    first = c.send(request)
    assert first.status_code == 201, first.text
    assert c.send(request).json() == first.json()
    replay = c.send(request, "another-header")
    assert replay.status_code == 201, replay.text
    assert replay.json()["id"] == first.json()["id"]
    assert len(rows(c, Metric)) == 1


def test_monitoring_client_cannot_choose_its_stored_request_hash(ingestion):
    c = ingestion
    request = payload(idempotency_key="hash", metadata={"_ingestion_request_hash": "forged"})
    first = c.send(request)
    assert first.status_code == 201
    assert "_ingestion_request_hash" not in first.json()["metadata"]
    assert rows(c, Metric)[0].value_json["_ingestion_request_hash"] != "forged"
    assert c.send({**request, "value": 0.2}, "different-header").status_code == 409


@pytest.mark.parametrize("fault", ["duplicate", "changed_value"])
def test_monitoring_body_replay_requires_one_matching_persisted_point(ingestion, fault):
    c = ingestion
    request = payload(idempotency_key="persisted")
    first = c.send(request)
    assert first.status_code == 201
    with Session(c.engine) as db, db.begin():
        point = db.get(Metric, uuid.UUID(first.json()["id"]))
        if fault == "changed_value":
            point.value = 0.9
        else:
            db.add(
                Metric(
                    **{
                        name: getattr(point, name)
                        for name in (
                            "project_id",
                            "model_run_id",
                            "name",
                            "kind",
                            "split",
                            "value",
                            "recorded_at",
                            "higher_is_better",
                            "value_json",
                        )
                    },
                    step=point.step + 1,
                )
            )
    before = {p.id: (p.value, p.value_json) for p in rows(c, Metric)}
    response = c.send(request, "different-header")
    assert response.status_code == 409, response.text
    assert {p.id: (p.value, p.value_json) for p in rows(c, Metric)} == before
    assert len(rows(c, WorkflowCommand)) == 1


def test_monitoring_legacy_body_key_does_not_invent_request_evidence(ingestion):
    c = ingestion
    request = payload(idempotency_key="legacy")
    first = c.send(request)
    assert first.status_code == 201
    with Session(c.engine) as db, db.begin():
        point = db.get(Metric, uuid.UUID(first.json()["id"]))
        point.value_json = {
            k: v for k, v in point.value_json.items() if k != "_ingestion_request_hash"
        }
        before = dict(point.value_json)
    assert c.send(request).json() == first.json()
    response = c.send(request, "new-header")
    assert response.status_code == 409, response.text
    assert rows(c, Metric)[0].value_json == before
    assert len(rows(c, WorkflowCommand)) == 1


@pytest.mark.parametrize("field", ["value", "metadata", "deployment"])
def test_monitoring_cached_response_must_match_its_request(ingestion, field):
    c = ingestion
    request = payload(metadata={"source": None})
    original = c.send(request)
    assert original.status_code == 201
    with Session(c.engine) as db, db.begin():
        command = db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.project_id == c.project_id)
        )
        cached = dict(command.response_payload)
        if field == "value":
            cached["value"] = 0.99
        elif field == "metadata":
            cached["metadata"] = {k: v for k, v in cached["metadata"].items() if k != "source"}
        else:
            cached["metadata"] = {**cached["metadata"], "deployment_run_id": str(uuid.uuid4())}
        command.response_payload = cached
    denied = c.send(request)
    assert denied.status_code == 409, denied.text
    assert rows(c, WorkflowCommand)[0].response_payload == cached
    assert len(rows(c, Metric)) == 1


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
@pytest.mark.parametrize("location", ["value", "metadata"])
def test_monitoring_nonfinite_json_is_rejected_before_writes(ingestion, value, location):
    c = ingestion
    data = (
        ('{"name":"error_rate","value":' + value + "}")
        if location == "value"
        else ('{"name":"error_rate","value":0.01,"metadata":{"nested":[' + value + "]}}")
    )
    response = c.send(data, raw=True)
    assert response.status_code == 422, response.text
    assert rows(c, Metric) == rows(c, WorkflowCommand) == []


@pytest.mark.parametrize(
    "kind",
    ["distinct", "body_same", "body_changed", "header_same", "header_changed", "configuration"],
)
def test_monitoring_competing_writes_preserve_identity_and_sequence(ingestion, kind):
    c = ingestion
    ready, release = Event(), Event()
    c.capture_pids = True

    def hold_first(db, _):
        metric = any(
            isinstance(point, Metric) and point.value_json.get("writer") == "first"
            for point in db.new
        )
        configuration = any(
            isinstance(run, ModelRun)
            and run.params.get("monitoring", {}).get("metrics") == ["first"]
            for run in db.dirty
        )
        if metric or configuration:
            ready.set()
            assert release.wait(15)

    event.listen(Session, "after_flush", hold_first)
    first = payload(metadata={"writer": "first"})
    second = payload(value=0.02, metadata={"writer": "second"})
    if kind.startswith("body"):
        first["idempotency_key"] = second["idempotency_key"] = "observation"
    if kind in {"body_same", "header_same"}:
        second = dict(first)
    configuration = kind == "configuration"
    if configuration:
        first, second = {"metrics": ["first"]}, {"metrics": ["second"]}
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            one = pool.submit(c.send, first, "one", configuration=configuration)
            two = None
            try:
                assert ready.wait(15)
                c.pids.get(timeout=5)
                two = pool.submit(
                    c.send,
                    second,
                    "one" if kind.startswith("header") else "two",
                    configuration=configuration,
                )
                pid, deadline = c.pids.get(timeout=5), monotonic() + 5
                with c.engine.connect() as observer:
                    while True:
                        wait = observer.scalar(
                            text("SELECT wait_event_type FROM pg_stat_activity WHERE pid=:pid"),
                            {"pid": pid},
                        )
                        observer.rollback()
                        if wait == "Lock":
                            break
                        assert monotonic() < deadline and not two.done()
                        sleep(0.02)
            finally:
                release.set()
            assert two is not None
            a, b = one.result(timeout=20), two.result(timeout=20)
        assert a.status_code == (200 if configuration else 201), a.text
        expected = 409 if kind in {"body_changed", "header_changed"} else a.status_code
        assert b.status_code == expected, b.text
        if configuration:
            assert (a.json()["revision"], b.json()["revision"]) == (1, 2)
        else:
            points = rows(c, Metric)
            assert sorted(p.step for p in points) == ([0, 1] if kind == "distinct" else [0])
            if kind in {"body_same", "header_same"}:
                assert a.json() == b.json()
    finally:
        event.remove(Session, "after_flush", hold_first)


@pytest.mark.parametrize("role", [ProjectRole.VIEWER, ProjectRole.EDITOR, ProjectRole.ADMIN])
def test_monitoring_replay_rechecks_current_permission(ingestion, role):
    c = ingestion
    first = c.send(payload())
    assert first.status_code == 201
    with Session(c.engine) as db, db.begin():
        colleague = User(email=uuid.uuid4().hex + "@example.test", global_role="member")
        db.add(colleague)
        db.flush()
        c.extra_users.append(colleague.id)
        db.add(
            ProjectMembership(
                project_id=c.project_id,
                user_id=colleague.id,
                role=role,
                accepted_at=datetime.now(UTC),
            )
        )
        c.user_id = colleague.id
    response = c.send(payload())
    assert response.status_code == (201 if role == ProjectRole.ADMIN else 403), response.text
    if role == ProjectRole.ADMIN:
        assert response.json() == first.json()
    assert len(rows(c, Metric)) == len(rows(c, WorkflowCommand)) == 1


def test_monitoring_configuration_refreshes_cached_deployment_before_revision(ingestion):
    from automl_api.schemas.monitoring import MonitoringConfigurationUpdate
    from automl_api.services.monitoring import deployment_run, update_monitoring_configuration

    c = ingestion
    with Session(c.engine) as db:
        cached = deployment_run(db, c.project_id, c.deployment_id)
        assert "monitoring" not in cached.params
        response = c.send({"metrics": ["first"]}, configuration=True)
        assert response.status_code == 200 and response.json()["revision"] == 1
        assert "monitoring" not in cached.params
        updated = update_monitoring_configuration(
            db,
            db.get(User, c.user_id),
            c.project_id,
            c.deployment_id,
            MonitoringConfigurationUpdate(metrics=["second"]),
        )
        assert updated.revision == 2 and updated.metrics == ["second"]
        db.commit()


def test_monitoring_body_replay_refreshes_cached_point_before_validation(ingestion):
    from automl_api.schemas.monitoring import MonitoringMetricPointCreate
    from automl_api.services.monitoring import record_monitoring_metric
    from fastapi import HTTPException

    c = ingestion
    request = payload(idempotency_key="cached")
    first = c.send(request)
    assert first.status_code == 201
    point_id = uuid.UUID(first.json()["id"])
    with Session(c.engine) as db:
        cached = db.get(Metric, point_id)
        with Session(c.engine) as remote, remote.begin():
            remote.get(Metric, point_id).value = 0.9
        assert cached.value == 0.01
        with pytest.raises(HTTPException) as denied:
            record_monitoring_metric(
                db,
                db.get(User, c.user_id),
                c.project_id,
                c.deployment_id,
                MonitoringMetricPointCreate(**request),
            )
        assert denied.value.status_code == 409
        assert cached.value == 0.9
    assert len(rows(c, Metric)) == 1


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
@pytest.mark.parametrize("field", ["warning", "critical"])
def test_monitoring_configuration_nonfinite_thresholds_are_rejected(ingestion, value, field):
    c = ingestion
    bounds = {"warning": "0.1", "critical": "0.2", field: value}
    raw = (
        '{"thresholds":{"error_rate":{"direction":"above","warning":'
        + bounds["warning"]
        + ',"critical":'
        + bounds["critical"]
        + "}}}"
    )
    response = c.send(raw, configuration=True, raw=True)
    assert response.status_code == 422, response.text
    with Session(c.engine) as db:
        assert db.get(ModelRun, c.deployment_id).params == {}
