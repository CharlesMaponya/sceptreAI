"""A readable URI and matching size cannot replace original producer custody."""

import uuid
from typing import Annotated

import pytest
from automl_api.api.deps import get_current_user
from automl_api.api.routes.operations import router
from automl_api.db.session import get_db
from automl_api.models.iam import User
from automl_api.models.runs import ModelRegistryEntry
from automl_api.models.workflows import WorkflowEvent
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from registry_custody_fixture import qualified_model as qualified_model
from sqlalchemy import delete
from sqlalchemy.orm import Session
from test_training_model_output_api import bound_run as bound_run
from test_training_model_output_api import capacity_client as capacity_client
from test_training_model_output_api import committed_tenant as committed_tenant
from test_training_model_output_api import context_api as context_api
from test_training_model_output_api import model_api as model_api
from test_training_model_output_api import phase1_engine as phase1_engine
from test_training_model_output_api import selected_scope as selected_scope


@pytest.fixture
def custody_registration(qualified_model):
    c = qualified_model
    app = FastAPI()
    app.include_router(router)

    def database():
        with Session(c.engine, expire_on_commit=False) as db:
            yield db

    def principal(db: Annotated[Session, Depends(get_db)]):
        return db.get(User, c.user_id)

    app.dependency_overrides[get_db] = database
    app.dependency_overrides[get_current_user] = principal

    def send():
        with TestClient(app) as client:
            return client.post(
                f"/projects/{c.project_id}/operations/registry",
                json={"training_run_id": str(c.run_id), "model_name": c.model_name},
            )

    c.send = send
    try:
        yield c
    finally:
        with c.factory() as db, db.begin():
            db.execute(
                delete(ModelRegistryEntry).where(ModelRegistryEntry.project_id == c.project_id)
            )


def test_registration_requires_original_producer_custody(custody_registration):
    c = custody_registration
    with c.factory() as db, db.begin():
        db.execute(
            delete(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.producer.scope[2],
                WorkflowEvent.event_key == f"training.model.grant.{c.producer.candidate_id}",
            )
        )
    response = c.send()
    assert response.status_code == 409, response.text
    assert c.native_counts == []


def test_registration_refuses_corrupt_native_model_bytes(custody_registration):
    c = custody_registration
    c.state["body"] = b"x" * len(c.payload)
    response = c.send()
    assert response.status_code == 409, response.text


def test_registration_records_original_version_and_native_proof(custody_registration):
    from automl_api.models.runs import RunArtifact
    from automl_api.services.workflow_state import canonical_request_hash
    from sqlalchemy import select

    c = custody_registration
    response = c.send()
    assert response.status_code == 201, response.text
    with c.factory() as db:
        row = db.scalars(
            select(ModelRegistryEntry).where(ModelRegistryEntry.project_id == c.project_id)
        ).one()
        artifact = db.get(RunArtifact, row.model_artifact_id)
        proof = artifact.artifact_metadata["model_custody"]
        assert proof["completion"]["object_version"] == "fixture-v1"
        assert proof["completion"]["sha256"] == c.entry["model_artifact_sha256"]
        assert proof["completion"]["byte_size"] == len(c.payload)
        assert proof["reference"]["candidate_id"] == str(c.producer.candidate_id)
        assert row.registry_metadata["native_custody_digest"] == canonical_request_hash(proof)
    assert c.native_counts == [0, 0]


@pytest.mark.parametrize("part", ["slot", "completion", "tracking", "metadata", "grant"])
def test_changed_original_history_cannot_register(custody_registration, part):
    from automl_api.models.workflows import WorkflowAttemptCredentialGrant
    from sqlalchemy import select

    c = custody_registration
    with c.factory() as db, db.begin():
        if part == "grant":
            db.get(
                WorkflowAttemptCredentialGrant, uuid.UUID(c.producer.receipt().json()["grant_id"])
            ).grant_digest = "c" * 64
        else:
            prefix = {
                "slot": "training.model.grant.",
                "completion": "attempt.output.completed.",
                "tracking": "training.tracking.published.",
                "metadata": "training.metadata.ready.",
            }[part]
            rows = list(
                db.scalars(
                    select(WorkflowEvent).where(
                        WorkflowEvent.attempt_id == c.producer.scope[2],
                        WorkflowEvent.event_key.startswith(prefix),
                    )
                )
            )
            assert rows
            for row in rows:
                row.result_digest = "c" * 64
    assert c.send().status_code == 409 and c.native_counts == []


@pytest.mark.parametrize("body", [b"", b"short", b"0123456789EXTRA"])
def test_truncated_or_extra_native_bytes_cannot_register(custody_registration, body):
    c = custody_registration
    c.state["body"] = body
    assert c.send().status_code == 409


def test_registration_uses_original_version_when_latest_is_unavailable(custody_registration):
    c = custody_registration
    c.store.size = lambda *_: (_ for _ in ()).throw(AssertionError("mutable latest size read"))
    c.store.exists = lambda *_: False
    response = c.send()
    assert response.status_code == 201, response.text
    assert c.native_counts == [0, 0]


def test_rehashed_history_change_cannot_rebind_existing_registration(custody_registration):
    from automl_api.services.workflow_state import canonical_request_hash
    from sqlalchemy import select

    c = custody_registration
    assert c.send().status_code == 201
    c.native_counts.clear()
    with c.factory() as db, db.begin():
        slot = db.scalars(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.producer.scope[2],
                WorkflowEvent.event_key == f"training.model.grant.{c.producer.candidate_id}",
            )
        ).one()
        slot.payload = {**slot.payload, "input_digest": "c" * 64}
        slot.result_digest = canonical_request_hash(slot.payload)
    assert c.send().status_code == 409 and c.native_counts == []


@pytest.mark.parametrize("fault", [None, "allowlist", "lineage"])
def test_registration_uses_only_explicit_predecessor_custody(custody_registration, fault):
    from automl_api.models.workflows import WorkflowAttempt
    from automl_api.services import reconciler

    c = custody_registration
    with c.factory() as db, db.begin():
        old = db.get(WorkflowAttempt, c.producer.scope[2])
        new = reconciler._replace_lost_ray_attempt(db, old, reason="owned registry custody test")
        assert new is not None
        if fault == "allowlist":
            new.predecessor_checkpoint_allowlist = []
        elif fault == "lineage":
            new.predecessor_attempt_id = None
    response = c.send()
    assert response.status_code == (201 if fault is None else 409), response.text
    assert c.native_counts == ([0, 0] if fault is None else [])


def test_original_completion_remains_usable_after_run_and_lease_finish(custody_registration):
    from datetime import UTC, datetime, timedelta

    from automl_api.models.enums import AttemptStatus, RunStatus
    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import WorkflowAttempt

    c = custody_registration
    with c.factory() as db, db.begin():
        db.get(ModelRun, c.run_id).status = RunStatus.SUCCEEDED
        attempt = db.get(WorkflowAttempt, c.producer.scope[2])
        attempt.status, attempt.lease_expires_at = (
            AttemptStatus.SUCCEEDED,
            datetime.now(UTC) - timedelta(seconds=1),
        )
    response = c.send()
    assert response.status_code == 201, response.text


def test_native_verification_deadline_fails_closed(custody_registration, monkeypatch):
    from automl_api.services import model_custody

    c = custody_registration
    clock = iter([0, 61])
    from types import SimpleNamespace

    monkeypatch.setattr(model_custody, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    from automl_api.models.iam import User
    from automl_api.schemas.operations import RegistryCreateRequest
    from automl_api.services.operations import inspect_model_registration, register_model
    from fastapi import HTTPException

    with c.factory() as db:
        selection = register_model(
            db,
            db.get(User, c.user_id),
            c.project_id,
            RegistryCreateRequest(training_run_id=c.run_id, model_name=c.model_name),
        )
        db.commit()
    with pytest.raises(HTTPException) as raised:
        inspect_model_registration(selection)
    assert raised.value.status_code == 409
