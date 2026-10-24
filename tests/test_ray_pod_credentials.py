"""Current-controller Ray roles receive only the credentials their code uses."""

import uuid
from unittest.mock import MagicMock

import pytest
from automl_api.core.config import Settings
from automl_api.models.enums import WorkflowStage
from automl_api.models.workflows import OutboxEntry, WorkflowCommand
from automl_api.services import reconciler
from automl_api.services.workflow_state import StaleFence, enqueue_outbox
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_phase1_reconciler import _attempt, _run
from test_phase3_preparation import _dataset_attempt, _dataset_entry, _profile_and_version
from test_ray_submit_identity import submission as submission

PROTOCOL = "ray-pod-env-v1"
TOPICS = ("ray.training.submit", "ray.dataset.split.submit", "ray.dataset.prepare.submit")


@pytest.fixture(params=["training_run", "splitter", "preparation"])
def stage(request):
    return request.param


@pytest.fixture(params=["s3_compatible", "aws_s3", "gcs", "azure_blob"])
def manifests(stage, request, monkeypatch):
    settings = Settings(
        training_namespace="credential-test",
        database_secret_name="application-database",
        database_secret_key="APPLICATION_URL",
        object_store_secret_name="provider-credentials",
        object_store_type=request.param,
        object_store_endpoint="https://objects.example.test",
        mlflow_tracking_uri="https://test-user:test-secret@tracking.example.test",
    )
    monkeypatch.setattr(reconciler, "get_settings", lambda: settings)
    if stage == "training_run":
        run = _run()
        attempt = _attempt(run)

        def build(**kwargs):
            return reconciler.build_ray_job_manifest(run, attempt, name="credential-test", **kwargs)
    else:
        attempt = _dataset_attempt(WorkflowStage(stage))
        profile, version = _profile_and_version(attempt)

        def build(**kwargs):
            return reconciler.build_dataset_ray_job_manifest(
                profile, version, attempt, name="credential-test", **kwargs
            )

    return build, attempt, stage, settings


def pods(manifest):
    spec = manifest["spec"]
    return (
        spec["submitterPodTemplate"]["spec"],
        spec["rayClusterSpec"]["headGroupSpec"]["template"]["spec"],
        spec["rayClusterSpec"]["workerGroupSpecs"][0]["template"]["spec"],
    )


def env(pod):
    return {item["name"]: item for item in pod["containers"][0]["env"]}


def test_new_submitter_has_no_database_storage_or_tracking_configuration(manifests):
    build, _, _, _ = manifests
    submitter, _, _ = pods(build(credential_protocol=PROTOCOL))
    names = set(env(submitter))
    assert "DATABASE_URL" not in names
    assert "MLFLOW_TRACKING_URI" not in names
    assert not any(name.startswith("OBJECT_STORE_") for name in names)
    assert not names & {"GCS_PROJECT", "AZURE_STORAGE_ACCOUNT"}
    assert "envFrom" not in submitter["containers"][0]


def test_dataset_compute_needs_no_database_while_training_compute_still_does(manifests):
    build, _, stage, _ = manifests
    _, _, worker = pods(build(credential_protocol=PROTOCOL))
    assert ("DATABASE_URL" in env(worker)) == (stage == "training_run")


def test_new_driver_and_projected_attempt_identity_remain_operational(manifests):
    build, attempt, stage, settings = manifests
    submitter, head, worker = pods(build(credential_protocol=PROTOCOL))
    for pod in (submitter, head, worker):
        values = env(pod)
        assert values["AUTOML_FENCING_TOKEN"]["value"] == attempt.fencing_token
        assert values["AUTOML_ATTEMPT_ID"]["value"] == str(attempt.id)
        assert values["AUTOML_PROJECT_ID"]["value"] == str(attempt.project_id)
        assert values["AUTOML_IDENTITY_TOKEN_FILE"]["value"].endswith("/identity/token")
        assert any(
            "serviceAccountToken" in source
            for volume in pod["volumes"]
            for source in volume.get("projected", {}).get("sources", [])
        )
    assert env(head)["DATABASE_URL"]["valueFrom"]["secretKeyRef"] == {
        "name": settings.database_secret_name,
        "key": settings.database_secret_key,
    }
    assert "OBJECT_STORE_TYPE" in env(head) and "OBJECT_STORE_TYPE" in env(worker)
    assert (
        submitter["serviceAccountName"] == worker["serviceAccountName"] == attempt.workload_identity
    )


def test_explicit_legacy_policy_preserves_the_complete_original_manifest(manifests):
    build, _, _, _ = manifests
    assert build(credential_protocol=None) == build()
    submitter, _, worker = pods(build())
    assert "DATABASE_URL" in env(submitter) and "DATABASE_URL" in env(worker)
    assert "OBJECT_STORE_TYPE" in env(submitter)


@pytest.mark.parametrize("topic", TOPICS)
def test_new_outbox_records_server_selected_credential_protocol(
    phase1_engine, committed_tenant, topic
):
    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db, db.begin():
        command = WorkflowCommand(
            project_id=project_id,
            actor_id=user_id,
            operation="test.credential-policy",
            idempotency_key=uuid.uuid4().hex,
            request_hash="a" * 64,
        )
        db.add(command)
        db.flush()
        original = {"attempt_id": str(uuid.uuid4()), "fencing_token": "fixture-fence"}
        entry = enqueue_outbox(
            db,
            command,
            topic=topic,
            aggregate_type="model_run",
            aggregate_id=uuid.uuid4(),
            payload=original,
        )
        assert entry.payload == {**original, "credential_protocol": "ray-training-model-read-v10"}
        assert "credential_protocol" not in original


@pytest.mark.parametrize("topic", TOPICS)
def test_existing_outbox_replay_never_rewrites_legacy_policy(
    phase1_engine, committed_tenant, topic
):
    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db, db.begin():
        command = WorkflowCommand(
            project_id=project_id,
            actor_id=user_id,
            operation="test.legacy-credential-policy",
            idempotency_key=uuid.uuid4().hex,
            request_hash="b" * 64,
        )
        db.add(command)
        db.flush()
        payload = {"attempt_id": str(uuid.uuid4()), "fencing_token": "original-fence"}
        entry = OutboxEntry(
            project_id=project_id,
            command_id=command.id,
            event_key=uuid.uuid4().hex,
            topic=topic,
            aggregate_type="model_run",
            aggregate_id=uuid.uuid4(),
            payload=payload,
        )
        db.add(entry)
        db.flush()
        replay = enqueue_outbox(
            db,
            command,
            topic=topic,
            aggregate_type=entry.aggregate_type,
            aggregate_id=entry.aggregate_id,
            event_key=entry.event_key,
            payload=payload,
        )
        assert replay.id == entry.id and replay.payload == payload
        assert "credential_protocol" not in replay.payload


@pytest.mark.parametrize("protocol", ["", "unknown", True, {}])
def test_builder_rejects_unknown_protocol_before_building_credentials(protocol):
    run = _run()
    with pytest.raises(ValueError, match="credential protocol"):
        reconciler.build_ray_job_manifest(
            run, _attempt(run), name="invalid-protocol", credential_protocol=protocol
        )


@pytest.mark.parametrize("protocol", [None, "unknown", True, {}])
def test_training_dispatch_rejects_explicit_unknown_protocol_before_native_work(
    submission, protocol
):
    db, _, attempt, entry, k8s = submission
    before = attempt.status
    entry.payload["credential_protocol"] = protocol
    with pytest.raises(StaleFence, match="credential protocol"):
        reconciler.submit_training_ray_job(db, entry, k8s)
    assert attempt.status == before
    k8s.ensure_service_account.assert_not_called()
    k8s.create_ray_job.assert_not_called()


def test_training_dispatch_uses_the_saved_credential_policy(submission):
    db, _, _, entry, k8s = submission
    entry.payload["credential_protocol"] = PROTOCOL
    reconciler.submit_training_ray_job(db, entry, k8s)
    submitter, head, worker = pods(k8s.create_ray_job.call_args.args[0])
    assert "DATABASE_URL" not in env(submitter)
    assert "DATABASE_URL" in env(head) and "DATABASE_URL" in env(worker)


@pytest.mark.parametrize("stage", [WorkflowStage.SPLITTER, WorkflowStage.PREPARATION])
def test_dataset_dispatch_uses_the_saved_credential_policy(stage):
    attempt = _dataset_attempt(stage)
    profile, version = _profile_and_version(attempt)
    entry = _dataset_entry(attempt, profile.id)
    entry.payload["credential_protocol"] = PROTOCOL
    db = MagicMock()
    values = iter([attempt, profile, version])
    db.scalar.side_effect = lambda statement: (
        None
        if statement.column_descriptions[0]["entity"].__name__ == "DatasetProviderImport"
        else next(values)
    )
    db.get.return_value = version
    k8s = MagicMock()
    k8s.create_ray_job.side_effect = lambda body: body
    reconciler.submit_dataset_ray_job(db, entry, k8s)
    submitter, head, worker = pods(k8s.create_ray_job.call_args.args[0])
    assert "DATABASE_URL" not in env(submitter) and "DATABASE_URL" not in env(worker)
    assert "DATABASE_URL" in env(head)
