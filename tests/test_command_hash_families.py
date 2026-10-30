"""Real public handlers must reject changed requests before their runtime effects."""

import copy
import uuid
from types import SimpleNamespace

import pytest
from automl_api.api.deps import get_current_user
from automl_api.api.routes import datasets, monitoring, operations, training, validation
from automl_api.db.session import get_db
from automl_api.models.iam import User
from automl_api.models.workflows import OutboxEntry, WorkflowCommand
from automl_api.services import training as training_service
from automl_api.services.workflow_state import begin_command, canonical_request_hash
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine

CASES = {
    "upload": (datasets, "_upload_response", "dataset.upload.complete", "datasets/upload"),
    "training": (
        training_service,
        "_resolve_estimate_identity",
        "training.launch",
        "training/runs",
    ),
    "estimate": (
        training_service,
        "_resolve_estimate_identity",
        "training.estimate.reserve",
        "training/estimate",
    ),
    "validation": (
        validation,
        "launch_validation_run",
        "validation.launch",
        "training/runs/{target}/validations",
    ),
    "explanation": (
        validation,
        "launch_explainability_run",
        "explainability.launch",
        "training/runs/{target}/explanations",
    ),
    "deployment": (
        operations,
        "deploy_registered_model",
        "deployment.launch",
        "operations/registry/{target}/deployments",
    ),
    "drift": (
        operations,
        "launch_drift_check",
        "drift.launch",
        "operations/registry/{target}/drift",
    ),
    "governance": (
        monitoring,
        "generate_governance_report",
        "governance.report.generate",
        "operations/deployments/{target}/governance/reports",
    ),
    "monitoring": (
        monitoring,
        "record_monitoring_metric",
        "monitoring.metric.ingest",
        "operations/deployments/{target}/monitoring/metrics",
    ),
    "cleanup": (operations, "cleanup_project_resources", "cleanup.execute", "operations/cleanup"),
}


class EffectReached(RuntimeError):
    pass


@pytest.fixture(params=CASES)
def family(phase1_engine, committed_tenant, monkeypatch, request):
    name = request.param
    effect_module, effect_name, operation, suffix = CASES[name]
    actor_id, project_id = committed_tenant
    c = SimpleNamespace(
        engine=phase1_engine,
        actor_id=actor_id,
        project_id=project_id,
        operation=operation,
        key="family-" + uuid.uuid4().hex,
        captured=[],
        name=name,
    )
    c.target = uuid.uuid4()
    c.path = f"/projects/{project_id}/" + suffix.format(target=c.target)
    c.other_path = f"/projects/{project_id}/" + suffix.format(target=uuid.uuid4())
    c.payload = {
        "training": {"dataset_version_id": str(uuid.uuid4()), "target_column": "target"},
        "estimate": {
            "dataset_version_id": str(uuid.uuid4()),
            "target_column": "target",
            "reserve_capacity": True,
        },
        "validation": {"model_name": "Ridge", "dataset_version_id": str(uuid.uuid4())},
        "explanation": {"model_name": "Ridge"},
        "deployment": {"replicas": 1},
        "drift": {"dataset_version_id": str(uuid.uuid4())},
        "monitoring": {"name": "accuracy", "value": 0.9},
        "cleanup": {"older_than_days": 30, "dry_run": True},
    }.get(name)
    app = FastAPI()
    for router in (
        datasets.router,
        training.router,
        validation.router,
        operations.router,
        monitoring.router,
    ):
        app.include_router(router)

    def database():
        with Session(c.engine) as db:
            yield db

    def user():
        with Session(c.engine) as db:
            return db.get(User, actor_id)

    app.dependency_overrides[get_db] = database
    app.dependency_overrides[get_current_user] = user

    def effect(db, *_args, **_kwargs):
        command = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == project_id,
                WorkflowCommand.operation == operation,
                WorkflowCommand.idempotency_key == c.key,
            )
        )
        assert command is not None
        assert command.request_hash == canonical_request_hash(command.request_payload)
        c.captured.append(copy.deepcopy(command.request_payload))
        raise EffectReached("Stopped before runtime effect")

    monkeypatch.setattr(effect_module, effect_name, effect)

    def send(*, changed=False, other_target=False):
        path = c.other_path if other_target else c.path
        arguments = {}
        if name == "upload":
            arguments = dict(
                data={"dataset_name": "hash-fixture"},
                files={"file": ("source.csv", b"x\n2\n" if changed else b"x\n1\n", "text/csv")},
            )
        elif c.payload is not None:
            body = copy.deepcopy(c.payload)
            if changed:
                if name == "monitoring":
                    body["value"] = 0.8
                elif name == "cleanup":
                    body["older_than_days"] = 31
                elif name == "deployment":
                    body["replicas"] = 2
                else:
                    body["expected_minutes"] = 11
            arguments["json"] = body
        with TestClient(app) as client:
            return client.post(path, headers={"Idempotency-Key": c.key}, **arguments)

    c.send = send
    return c


def test_public_family_persists_hash_and_rejects_changed_request_before_effects(family):
    c = family
    with pytest.raises(EffectReached, match="Stopped before runtime effect"):
        c.send()
    assert len(c.captured) == 1
    with Session(c.engine) as db:
        assert not list(
            db.scalars(select(WorkflowCommand).where(WorkflowCommand.project_id == c.project_id))
        )
    with Session(c.engine) as db, db.begin():
        command, replayed = begin_command(
            db,
            project_id=c.project_id,
            actor_id=c.actor_id,
            operation=c.operation,
            idempotency_key=c.key,
            payload=c.captured[0],
        )
        assert not replayed
        original_id, original_hash = command.id, command.request_hash
    identical = [c.send() for _ in range(2)]
    assert all(response.status_code == 409 for response in identical)
    assert identical[0].json() == identical[1].json()
    assert identical[0].json()["detail"]["code"] == "idempotency_response_pending"
    changes = []
    if c.payload is not None or c.name == "upload":
        changes.append(dict(changed=True))
    if c.path != c.other_path:
        changes.append(dict(other_target=True))
    assert changes
    for change in changes:
        responses = [c.send(**change) for _ in range(2)]
        assert all(response.status_code == 409 for response in responses)
        assert responses[0].json() == responses[1].json()
        assert responses[0].json() != identical[0].json()
        detail = responses[0].json()["detail"]
        assert (
            detail.get("code") == "idempotency_key_reused"
            if isinstance(detail, dict)
            else "Idempotency-Key" in detail and "different request" in detail
        )
    assert len(c.captured) == 1
    with Session(c.engine) as db:
        commands = list(
            db.scalars(select(WorkflowCommand).where(WorkflowCommand.project_id == c.project_id))
        )
        assert len(commands) == 1 and commands[0].id == original_id
        assert commands[0].request_hash == original_hash
        assert commands[0].request_payload == c.captured[0]
        assert not list(
            db.scalars(select(OutboxEntry).where(OutboxEntry.project_id == c.project_id))
        )
