"""Deployment downloads keep the original registry model's custody and version."""

import io
import uuid
from threading import local
from types import SimpleNamespace

import pytest
from automl_api.api.routes import serving_artifacts
from automl_api.db.session import get_db
from automl_api.models.enums import ModelStage, ProjectStatus, RunKind, RunStatus
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRegistryEntry, ModelRun, RunArtifact
from automl_api.models.workflows import DeletionTombstone, WorkflowEvent
from automl_api.schemas.operations import RegistryCreateRequest
from automl_api.security.deployments import deployment_token
from automl_api.services import operations
from fastapi import FastAPI
from fastapi.testclient import TestClient
from registry_custody_fixture import qualified_model as qualified_model
from sqlalchemy import delete, event
from sqlalchemy.orm import Session
from test_training_model_output_api import bound_run as bound_run
from test_training_model_output_api import capacity_client as capacity_client
from test_training_model_output_api import committed_tenant as committed_tenant
from test_training_model_output_api import context_api as context_api
from test_training_model_output_api import model_api as model_api
from test_training_model_output_api import phase1_engine as phase1_engine
from test_training_model_output_api import selected_scope as selected_scope


@pytest.fixture
def serving_model(qualified_model, monkeypatch):
    from automl_api.models.iam import User
    from automl_api.services import inference_identity
    from automl_api.services.inference_tls import Authority
    from test_inference_tls import authority_material

    c = qualified_model
    c.inference_authority = Authority.from_pem(*authority_material())
    monkeypatch.setattr(inference_identity, "authority", lambda *_: c.inference_authority)
    monkeypatch.setattr(
        inference_identity,
        "control_base_url",
        lambda *_: "https://control.invalid/api/v1/internal/deployments",
    )
    request = RegistryCreateRequest(training_run_id=c.run_id, model_name=c.model_name)
    with c.factory() as db:
        selection = operations.register_model(db, db.get(User, c.user_id), c.project_id, request)
        db.commit()
        inspection = operations.inspect_model_registration(selection)
        entry = operations.register_model(
            db, db.get(User, c.user_id), c.project_id, request, inspection=inspection
        )
        db.commit()
        source = db.get(ModelRun, c.run_id)
        deployment = ModelRun(
            project_id=c.project_id,
            dataset_version_id=source.dataset_version_id,
            created_by_id=c.user_id,
            run_kind=RunKind.DEPLOYMENT,
            status=RunStatus.RUNNING,
            task_type=source.task_type,
            target_column=source.target_column,
            k8s_job_name="owned-serving-fixture",
            params={},
            tags={
                "registry_entry_id": str(entry.id),
                "model_artifact_id": str(entry.model_artifact_id),
                "model_custody_digest": entry.registry_metadata["native_custody_digest"],
            },
        )
        db.add(deployment)
        db.commit()
        c.deployment_id, c.registry_id = deployment.id, entry.id
        c.artifact_id = entry.model_artifact_id
    c.native_counts.clear()
    c.read_sizes, c.versions, c.closed = [], [], []
    activity = local()

    def checkout(*_):
        activity.connections = getattr(activity, "connections", 0) + 1

    def checkin(*_):
        activity.connections -= 1

    event.listen(c.engine, "checkout", checkout)
    event.listen(c.engine, "checkin", checkin)

    def opened(uri, byte_range=None, *, version=None):
        assert uri == c.uri and byte_range is None
        c.versions.append(version)
        c.native_counts.append(getattr(activity, "connections", 0))
        if callback := c.state.get("during_open"):
            callback()
        if c.state.get("unavailable"):
            raise OSError("owned object unavailable")
        body = io.BytesIO(c.state.get("body", c.payload))

        class Stream:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                body.close()
                c.closed.append(True)

            def read(self, size):
                c.read_sizes.append(size)
                if callback := c.state.pop("during_read", None):
                    callback()
                if c.state.get("read_failure"):
                    raise OSError("owned stream interrupted")
                return body.read(size)

        return Stream()

    monkeypatch.setattr(
        serving_artifacts, "get_object_store", lambda: SimpleNamespace(open_stream=opened)
    )
    app = FastAPI()
    app.include_router(serving_artifacts.router)

    def database():
        with Session(c.engine, expire_on_commit=False) as db:
            yield db

    app.dependency_overrides[get_db] = database

    def download(token=None):
        with TestClient(app) as client:
            return client.get(
                f"/internal/deployments/{c.deployment_id}/model",
                headers={"X-Sceptre-Deployment-Token": token or deployment_token(c.deployment_id)},
            )

    c.download = download
    try:
        yield c
    finally:
        event.remove(c.engine, "checkout", checkout)
        event.remove(c.engine, "checkin", checkin)
        with c.factory() as db, db.begin():
            db.execute(
                delete(DeletionTombstone).where(DeletionTombstone.project_id == c.project_id)
            )
            db.execute(delete(ModelRun).where(ModelRun.id == c.deployment_id))
            db.execute(
                delete(ModelRegistryEntry).where(ModelRegistryEntry.project_id == c.project_id)
            )


def test_serving_model_native_read_releases_sql(serving_model):
    c = serving_model
    response = c.download()
    assert response.status_code == 200 and response.content == c.payload
    assert c.native_counts == [0]


def test_serving_model_corruption_is_rejected_before_disclosure(serving_model):
    c = serving_model
    c.state["body"] = b"x" * len(c.payload)
    response = c.download()
    assert response.status_code == 409
    assert c.state["body"] not in response.content


def test_serving_model_requires_original_receipts(serving_model):
    c = serving_model
    with c.factory() as db, db.begin():
        db.execute(
            delete(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.producer.scope[2],
                WorkflowEvent.event_key == f"training.model.grant.{c.producer.candidate_id}",
            )
        )
    response = c.download()
    assert response.status_code == 409
    assert c.native_counts == []


def test_serving_model_pins_version_and_bounds_reads(serving_model):
    c = serving_model
    response = c.download()
    assert response.status_code == 200 and response.content == c.payload
    assert c.versions == ["fixture-v1"] and c.closed == [True]
    assert c.read_sizes and max(c.read_sizes) <= 65536
    assert response.headers["content-length"] == str(len(c.payload))
    assert response.headers["cache-control"] == "no-store"


def change_source(c, change):
    with c.factory() as db, db.begin():
        run = db.get(ModelRun, c.deployment_id)
        registry = db.get(ModelRegistryEntry, c.registry_id)
        artifact = db.get(RunArtifact, c.artifact_id)
        if change == "stopped":
            run.status = RunStatus.CANCELLED
        elif change == "binding":
            run.tags = {**run.tags, "model_custody_digest": "0" * 64}
        elif change == "missing_binding":
            run.tags = {k: v for k, v in run.tags.items() if k != "model_custody_digest"}
        elif change == "registry":
            registry.registry_metadata = {
                **registry.registry_metadata,
                "native_custody_digest": "0" * 64,
            }
        elif change == "artifact":
            artifact.object_uri += ".changed"
        elif change == "project":
            db.get(Project, c.project_id).status = ProjectStatus.ARCHIVED
        elif change == "malformed_id":
            run.tags = {**run.tags, "model_artifact_id": "malformed"}
        elif change in {"delete_project", "delete_source", "delete_artifact"}:
            kind, identity = {
                "delete_project": ("project", c.project_id),
                "delete_source": ("run", c.run_id),
                "delete_artifact": ("run_artifact", c.artifact_id),
            }[change]
            db.add(
                DeletionTombstone(
                    project_id=c.project_id,
                    requested_by_id=c.user_id,
                    resource_type=kind,
                    resource_id=identity,
                    reason="owned serving control",
                )
            )
        else:
            raise AssertionError(change)


@pytest.mark.parametrize(
    "change", ["missing_binding", "binding", "registry", "artifact", "project", "malformed_id"]
)
def test_serving_model_rejects_unqualified_authority_without_native_read(serving_model, change):
    c = serving_model
    change_source(c, change)
    response = c.download()
    assert response.status_code in {404, 409}
    assert c.native_counts == []


@pytest.mark.parametrize(
    "change",
    [
        "stopped",
        "binding",
        "registry",
        "artifact",
        "project",
        "delete_project",
        "delete_source",
        "delete_artifact",
    ],
)
def test_serving_model_revalidates_after_native_read(serving_model, change):
    c = serving_model
    c.state["during_read"] = lambda: change_source(c, change)
    response = c.download()
    assert response.status_code in {404, 409}
    assert response.content != c.payload and c.native_counts == [0] and c.closed == [True]


@pytest.mark.parametrize("body", [b"", b"012345678", b"0123456789extra"])
def test_serving_model_rejects_inexact_native_size(serving_model, body):
    c = serving_model
    c.state["body"] = body
    response = c.download()
    assert response.status_code == 409
    assert c.closed == [True]


@pytest.mark.parametrize("fault", ["read_failure", "unavailable", "spool_write_failure"])
def test_serving_model_native_failures_close_temporary_files(serving_model, monkeypatch, fault):
    c = serving_model
    original = serving_artifacts.tempfile.SpooledTemporaryFile
    files = []

    def temporary(**kwargs):
        assert kwargs == {"max_size": 8 * 1024 * 1024, "mode": "w+b"}
        file = original(**kwargs)
        if fault == "spool_write_failure":

            def denied(_):
                raise OSError("owned temporary storage failure")

            file.write = denied
        files.append(file)
        return file

    monkeypatch.setattr(serving_artifacts.tempfile, "SpooledTemporaryFile", temporary)
    c.state[fault] = True
    assert c.download().status_code == 409
    assert len(files) == 1 and files[0].closed


def test_serving_model_rollover_closes_verified_spool(serving_model, monkeypatch):
    c = serving_model
    original = serving_artifacts.tempfile.SpooledTemporaryFile
    files = []

    def temporary(**kwargs):
        assert kwargs == {"max_size": 8 * 1024 * 1024, "mode": "w+b"}
        file = original(max_size=1, mode="w+b")
        files.append(file)
        return file

    monkeypatch.setattr(serving_artifacts.tempfile, "SpooledTemporaryFile", temporary)
    response = c.download()
    assert response.status_code == 200 and response.content == c.payload
    assert len(files) == 1 and files[0]._rolled and files[0].closed


def test_serving_model_credential_denial_does_not_touch_storage(serving_model):
    c = serving_model
    assert c.download(token=deployment_token(uuid.uuid4())).status_code == 401
    assert c.native_counts == []


def test_new_deployment_binds_registry_custody(serving_model):
    from automl_api.core.config import Settings
    from automl_api.models.iam import User
    from automl_api.schemas.operations import ModelDeploymentRequest
    from automl_api.services.kubernetes_training import KubernetesTrainingClient

    c = serving_model
    client = object.__new__(KubernetesTrainingClient)
    client.settings = Settings(inference_image="owned-inference@sha256:fixture")
    with c.factory() as db, db.begin():
        db.execute(delete(ModelRun).where(ModelRun.id == c.deployment_id))
        registry = db.get(ModelRegistryEntry, c.registry_id)
        registry.stage = ModelStage.STAGING
        result = operations.deploy_registered_model(
            db,
            db.get(User, c.user_id),
            c.project_id,
            c.registry_id,
            ModelDeploymentRequest(),
            client,
        )
        c.deployment_id = result.run.id
        assert (
            result.run.tags["model_custody_digest"]
            == registry.registry_metadata["native_custody_digest"]
        )
        environment = result.manifests["deployment"]["spec"]["template"]["spec"]["containers"][0][
            "env"
        ]
        assert {item["name"]: item.get("value") for item in environment}["MODEL_SHA256"] == c.entry[
            "model_artifact_sha256"
        ]
    # New TLS deployments must never regain the legacy download credential.
    assert c.download().status_code == 401
    assert c.native_counts == []


def test_legacy_deployment_cannot_restart_without_custody(serving_model):
    from unittest.mock import MagicMock

    from automl_api.models.iam import User
    from fastapi import HTTPException

    c = serving_model
    change_source(c, "missing_binding")
    change_source(c, "stopped")
    client = MagicMock()
    with c.factory() as db, db.begin():
        with pytest.raises(HTTPException) as failure:
            operations.start_model_deployment(
                db, db.get(User, c.user_id), c.project_id, c.deployment_id, client
            )
        assert failure.value.status_code == 409
    client.start_model_deployment.assert_not_called()


def test_qualified_deployment_can_restart_and_download(serving_model, monkeypatch):
    from unittest.mock import MagicMock

    from automl_api.models.iam import User

    custody = {"unit_native_dependency_only": True}
    monkeypatch.setattr(operations, "lifecycle_custody", lambda *_: custody)
    c = serving_model
    change_source(c, "stopped")
    client = MagicMock()
    with c.factory() as db, db.begin():
        result = operations.start_model_deployment(
            db, db.get(User, c.user_id), c.project_id, c.deployment_id, client
        )
        assert result.status == RunStatus.QUEUED
    client.start_model_deployment.assert_not_called()
    assert c.download().content == c.payload


@pytest.mark.parametrize("fault", ["registry", "artifact", "missing_receipt"])
def test_unqualified_model_cannot_create_deployment(serving_model, fault):
    from automl_api.core.config import Settings
    from automl_api.models.iam import User
    from automl_api.schemas.operations import ModelDeploymentRequest
    from automl_api.services.kubernetes_training import KubernetesTrainingClient
    from fastapi import HTTPException
    from sqlalchemy import func, select

    c = serving_model
    client = object.__new__(KubernetesTrainingClient)
    client.settings = Settings(inference_image="owned-inference@sha256:fixture")
    if fault == "missing_receipt":
        with c.factory() as db, db.begin():
            db.execute(
                delete(WorkflowEvent).where(
                    WorkflowEvent.attempt_id == c.producer.scope[2],
                    WorkflowEvent.event_key == f"training.model.grant.{c.producer.candidate_id}",
                )
            )
    else:
        change_source(c, fault)
    with c.factory() as db, db.begin():
        db.execute(delete(ModelRun).where(ModelRun.id == c.deployment_id))
        db.get(ModelRegistryEntry, c.registry_id).stage = ModelStage.STAGING
    with c.factory() as db:
        with pytest.raises(HTTPException) as failure:
            operations.deploy_registered_model(
                db,
                db.get(User, c.user_id),
                c.project_id,
                c.registry_id,
                ModelDeploymentRequest(),
                client,
            )
        assert failure.value.status_code == 409
        db.rollback()
        assert (
            db.scalar(
                select(func.count())
                .select_from(ModelRun)
                .where(ModelRun.project_id == c.project_id, ModelRun.run_kind == RunKind.DEPLOYMENT)
            )
            == 0
        )
    assert c.native_counts == []


def test_cached_legacy_deployment_cannot_restore_removed_binding(serving_model):
    from unittest.mock import MagicMock

    from automl_api.models.iam import User
    from fastapi import HTTPException

    c = serving_model
    change_source(c, "stopped")
    client = MagicMock()
    with c.factory(expire_on_commit=False) as db:
        cached = db.get(ModelRun, c.deployment_id)
        assert cached.tags["model_custody_digest"]
        db.commit()
        change_source(c, "missing_binding")
        with pytest.raises(HTTPException) as failure:
            operations.start_model_deployment(
                db, db.get(User, c.user_id), c.project_id, c.deployment_id, client
            )
        assert failure.value.status_code == 409
        db.rollback()
    client.start_model_deployment.assert_not_called()


def test_cached_registry_stage_cannot_admit_an_archived_model(serving_model):
    from automl_api.core.config import Settings
    from automl_api.models.iam import User
    from automl_api.schemas.operations import ModelDeploymentRequest
    from automl_api.services.kubernetes_training import KubernetesTrainingClient
    from fastapi import HTTPException

    c = serving_model
    client = object.__new__(KubernetesTrainingClient)
    client.settings = Settings(inference_image="owned-inference@sha256:fixture")
    with c.factory() as db, db.begin():
        db.execute(delete(ModelRun).where(ModelRun.id == c.deployment_id))
        db.get(ModelRegistryEntry, c.registry_id).stage = ModelStage.STAGING
    with c.factory(expire_on_commit=False) as db:
        cached = db.get(ModelRegistryEntry, c.registry_id)
        assert cached.stage == ModelStage.STAGING
        db.commit()
        with c.factory() as other, other.begin():
            other.get(ModelRegistryEntry, c.registry_id).stage = ModelStage.ARCHIVED
        with pytest.raises(HTTPException) as failure:
            operations.deploy_registered_model(
                db,
                db.get(User, c.user_id),
                c.project_id,
                c.registry_id,
                ModelDeploymentRequest(),
                client,
            )
        assert failure.value.status_code == 409
        assert "Only staging or production" in failure.value.detail
        db.rollback()
