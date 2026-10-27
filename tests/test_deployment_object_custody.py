"""Deployment object writes retain original custody across later-stage failures."""

import hashlib
import io
import json
import uuid
from datetime import UTC, datetime, timedelta
from threading import local
from types import SimpleNamespace

import pytest
from automl_api.core.config import Settings
from automl_api.models.enums import ArtifactKind, CommandStatus, ModelStage, OutboxStatus
from automl_api.models.iam import User
from automl_api.models.runs import ModelRegistryEntry, ModelRun, RunArtifact
from automl_api.models.workflows import DeletionTombstone, OutboxEntry, WorkflowCommand
from automl_api.schemas.operations import ModelDeploymentRequest
from automl_api.services import operations, reconciler
from automl_api.services.deployment_objects import TOPIC
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.workflow_state import begin_command, enqueue_outbox
from automl_api.storage.contracts import ObjectMetadata
from automl_api.workflow_reconciler import run_once
from sqlalchemy import delete, event, select, update
from test_serving_model_custody import bound_run as bound_run
from test_serving_model_custody import capacity_client as capacity_client
from test_serving_model_custody import committed_tenant as committed_tenant
from test_serving_model_custody import context_api as context_api
from test_serving_model_custody import model_api as model_api
from test_serving_model_custody import phase1_engine as phase1_engine
from test_serving_model_custody import qualified_model as qualified_model
from test_serving_model_custody import selected_scope as selected_scope
from test_serving_model_custody import serving_model as serving_model


@pytest.fixture
def deployment_object(serving_model, monkeypatch):
    c = serving_model
    builder = object.__new__(KubernetesTrainingClient)
    builder.settings = Settings(inference_image="owned-inference@sha256:fixture")
    with c.factory() as db, db.begin():
        db.execute(delete(ModelRun).where(ModelRun.id == c.deployment_id))
        db.get(ModelRegistryEntry, c.registry_id).stage = ModelStage.STAGING
        launch = operations.deploy_registered_model(
            db,
            db.get(User, c.user_id),
            c.project_id,
            c.registry_id,
            ModelDeploymentRequest(),
            builder,
        )
        c.deployment_id = launch.run.id
        c.admission_tags = dict(launch.run.tags)
        run = db.get(ModelRun, c.deployment_id)
        run.tags = {**run.tags, "deployment_write_protocol": "deployment-dockerfile-v1"}
        artifact = db.scalars(
            select(RunArtifact).where(
                RunArtifact.model_run_id == c.deployment_id,
                RunArtifact.kind == ArtifactKind.DEPLOYMENT_IMAGE,
            )
        ).one()
        c.dockerfile_id = artifact.id
        c.dockerfile_bytes = artifact.artifact_metadata["content"].encode()
        db.execute(
            update(OutboxEntry)
            .where(
                OutboxEntry.project_id == c.project_id, OutboxEntry.topic == "ray.training.submit"
            )
            .values(status=OutboxStatus.DELIVERED, lease_owner=None, lease_expires_at=None)
        )
        command, _ = begin_command(
            db,
            project_id=c.project_id,
            actor_id=c.user_id,
            operation="deployment.launch",
            idempotency_key=uuid.uuid4().hex,
            payload={
                "entry_id": str(c.registry_id),
                **ModelDeploymentRequest().model_dump(mode="json"),
            },
        )
        command.status = CommandStatus.RUNNING
        command.resource_type = "deployment.launch"
        command.resource_id = c.deployment_id
        queued = enqueue_outbox(
            db,
            command,
            topic=TOPIC,
            aggregate_type="model_run",
            aggregate_id=c.deployment_id,
            payload={
                "run_id": str(c.deployment_id),
                "dockerfile_uri": launch.dockerfile_uri,
                "manifests": launch.manifests,
            },
        )
        db.flush()
        c.command_id, c.entry_id = command.id, queued.id
    c.put_calls, c.kubernetes_calls, c.object_reads = [], [], []
    c.objects, c.object_state = {}, {}
    activity = local()

    def checkout(*_):
        activity.connections = getattr(activity, "connections", 0) + 1

    def checkin(*_):
        activity.connections -= 1

    event.listen(c.engine, "checkout", checkout)
    event.listen(c.engine, "checkin", checkin)

    def save_object(key, body, metadata, conditional):
        uri = "s3://owned/" + key
        c.put_calls.append(
            {
                "uri": uri,
                "body": body,
                "metadata": metadata,
                "conditional": conditional,
                "sql_connections": getattr(activity, "connections", 0),
            }
        )
        if conditional and uri in c.objects:
            from botocore.exceptions import ClientError

            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
        version = "dockerfile-v" + str(len(c.put_calls))
        c.objects[uri] = dict(body=body, metadata=metadata, version=version)
        if callback := c.object_state.pop("during_write", None):
            callback()
        if c.object_state.get("lose_put_ack"):
            raise OSError("owned native write acknowledgement lost")
        return version

    def put_bytes(key, body):
        save_object(key, body, {}, False)
        return ObjectMetadata(uri="s3://owned/" + key, byte_size=len(body))

    def put_object(**kwargs):
        assert kwargs["Bucket"] == "owned" and kwargs["IfNoneMatch"] == "*"
        body = kwargs["Body"].read()
        assert len(body) == kwargs["ContentLength"]
        version = save_object(kwargs["Key"], body, kwargs["Metadata"], True)
        return {"VersionId": version}

    def stat(uri):
        c.object_reads.append(getattr(activity, "connections", 0))
        if c.object_state.get("stat_unavailable"):
            raise OSError("owned native inspection unavailable")
        if uri not in c.objects:
            raise FileNotFoundError(uri)
        value = c.objects[uri]
        return ObjectMetadata(
            uri=uri,
            byte_size=len(value["body"]),
            provider_headers={
                "version_id": value["version"],
                "sceptre_grant": value["metadata"].get("sceptre_grant", ""),
            },
        )

    def stream(uri, byte_range=None, *, version=None):
        c.object_reads.append(getattr(activity, "connections", 0))
        value = c.objects[uri]
        assert version == value["version"] and byte_range is None
        return io.BytesIO(value["body"])

    c.object_store = SimpleNamespace(
        driver_name="s3_compatible",
        bucket="owned",
        put_bytes=put_bytes,
        stat=stat,
        open_stream=stream,
        uri_for_key=lambda key: "s3://owned/" + key,
        _key_from_uri=lambda uri: uri.removeprefix("s3://owned/"),
        client=SimpleNamespace(
            put_object=put_object,
            meta=SimpleNamespace(config=SimpleNamespace(retries={"total_max_attempts": 1})),
        ),
    )

    import copy

    from kubernetes.client import ApiException

    c.native_resources, c.native_posts, c.native_reads = {}, [], []
    c.namespace_uid = "owned-namespace-lifetime"

    def native_create(kind, **kwargs):
        c.native_posts.append((kind, getattr(activity, "connections", 0)))
        if kind in c.native_resources:
            raise ApiException(status=409)
        body = copy.deepcopy(kwargs["body"])
        body["metadata"]["uid"] = str(uuid.uuid4())
        c.native_resources[kind] = body
        if callback := c.object_state.pop("during_native_create", None):
            callback(kind)
        if c.object_state.pop("lose_native_ack", False):
            raise OSError("Owned native acknowledgement lost")
        return copy.deepcopy(body)

    def native_read(kind, **kwargs):
        c.native_reads.append((kind, getattr(activity, "connections", 0)))
        if kind not in c.native_resources:
            raise ApiException(status=404)
        return copy.deepcopy(c.native_resources[kind])

    def namespace_read(*args, **kwargs):
        c.native_reads.append(("namespace", getattr(activity, "connections", 0)))
        return SimpleNamespace(
            metadata=SimpleNamespace(
                name=builder.settings.training_namespace,
                uid=c.namespace_uid,
                deletion_timestamp=None,
            )
        )

    native = object.__new__(KubernetesTrainingClient)
    native.settings = builder.settings
    native.apps = SimpleNamespace(
        create_namespaced_deployment=lambda **kw: native_create("deployment", **kw),
        read_namespaced_deployment=lambda **kw: native_read("deployment", **kw),
    )
    native.core = SimpleNamespace(
        read_namespace=namespace_read,
        create_namespaced_service=lambda **kw: native_create("service", **kw),
        read_namespaced_service=lambda **kw: native_read("service", **kw),
    )
    native.networking = SimpleNamespace(
        create_namespaced_ingress=lambda **kw: native_create("ingress", **kw),
        read_namespaced_ingress=lambda **kw: native_read("ingress", **kw),
    )

    def create(manifests, **kwargs):
        c.kubernetes_calls.append(manifests)
        KubernetesTrainingClient.create_model_deployment(native, manifests, **kwargs)
        if c.object_state.get("kubernetes_failures", 0):
            c.object_state["kubernetes_failures"] -= 1
            raise OSError("owned later Kubernetes effect failed")

    native.create_model_deployment = create
    c.kubernetes = native
    monkeypatch.setattr(reconciler, "get_object_store", lambda: c.object_store)
    monkeypatch.setattr(reconciler, "KubernetesTrainingClient", lambda: c.kubernetes)
    c.deliver = lambda: run_once(c.factory, worker_id="deployment-object-owner")

    def retry():
        with c.factory() as db, db.begin():
            db.get(OutboxEntry, c.entry_id).available_at = datetime.now(UTC) - timedelta(seconds=1)
        return c.deliver()

    c.retry = retry
    try:
        yield c
    finally:
        event.remove(c.engine, "checkout", checkout)
        event.remove(c.engine, "checkin", checkin)
        with c.factory() as db, db.begin():
            db.execute(delete(ModelRun).where(ModelRun.id == c.deployment_id))


def test_deployment_dockerfile_write_releases_sql(deployment_object):
    c = deployment_object
    assert c.deliver() == 1
    assert len(c.put_calls) == 1
    assert c.put_calls[0]["body"] == c.dockerfile_bytes
    assert c.put_calls[0]["sql_connections"] == 0
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED


def test_later_kubernetes_failure_cannot_repeat_dockerfile_write(deployment_object):
    c = deployment_object
    c.object_state["kubernetes_failures"] = 1
    assert c.deliver() == 1
    assert c.retry() == 1
    assert len(c.kubernetes_calls) == 2
    assert len(c.put_calls) == 1
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED


def test_deployment_admission_and_receipt_bind_original_write(deployment_object):
    from automl_api.services.deployment_objects import PROTOCOL, RECORD
    from automl_api.services.workflow_state import canonical_request_hash

    c = deployment_object
    assert c.admission_tags["deployment_write_protocol"] == PROTOCOL
    assert c.deliver() == 1
    with c.factory() as db:
        artifact = db.get(RunArtifact, c.dockerfile_id)
        assert artifact.artifact_metadata["write_protocol"] == PROTOCOL
        record = artifact.artifact_metadata[RECORD]
        assert record["intent"]["sha256"] == hashlib.sha256(c.dockerfile_bytes).hexdigest()
        assert record["receipt"]["intent_digest"] == canonical_request_hash(record["intent"])
        assert record["receipt"]["object_version"] == c.objects[artifact.object_uri]["version"]
        assert c.put_calls[0]["metadata"]["sceptre_grant"] == canonical_request_hash(
            record["intent"]
        )
    assert c.put_calls[0]["conditional"] and c.object_reads and set(c.object_reads) == {0}


def test_lost_native_ack_is_recovered_without_a_second_put(deployment_object):
    c = deployment_object
    c.object_state["lose_put_ack"] = True
    assert c.deliver() == 1
    assert len(c.put_calls) == 1 and len(c.kubernetes_calls) == 1
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
    assert c.object_reads and set(c.object_reads) == {0}


def start_unknown_write(c):
    c.object_state.update(lose_put_ack=True, stat_unavailable=True)
    assert c.deliver() == 1 and len(c.put_calls) == 1 and c.kubernetes_calls == []


def test_unknown_native_write_retries_only_read_original_receipt(deployment_object):
    from automl_api.services.deployment_objects import has_unretired_deployment_writes

    c = deployment_object
    start_unknown_write(c)
    with c.factory() as db:
        before = db.get(OutboxEntry, c.entry_id).delivery_attempts
        assert has_unretired_deployment_writes(db, c.project_id, [c.deployment_id])
    assert c.retry() == 1 and len(c.put_calls) == 1
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).delivery_attempts == before
    c.object_state["stat_unavailable"] = False
    assert c.retry() == 1 and len(c.put_calls) == 1 and len(c.kubernetes_calls) == 1
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
        # The object write is complete; active native resources still need UID-bound retirement.
        assert has_unretired_deployment_writes(db, c.project_id, [c.deployment_id])


@pytest.mark.parametrize("fault", ["grant", "body", "missing"])
def test_unknown_native_history_never_authorizes_another_put(deployment_object, fault):
    from automl_api.services.deployment_objects import has_unretired_deployment_writes

    c = deployment_object
    start_unknown_write(c)
    c.object_state["stat_unavailable"] = False
    value = next(iter(c.objects.values()))
    if fault == "grant":
        value["metadata"]["sceptre_grant"] = "0" * 64
    elif fault == "body":
        value["body"] = b"x" * len(value["body"])
    else:
        c.objects.clear()
    assert c.retry() == 1 and len(c.put_calls) == 1 and not c.kubernetes_calls
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED
        assert has_unretired_deployment_writes(db, c.project_id, [c.deployment_id])


def test_cancelled_deployment_records_native_fact_without_kubernetes(deployment_object):
    from automl_api.models.enums import RunStatus
    from automl_api.services.deletion import _require_stopped_workflows
    from automl_api.services.deployment_objects import RECORD, has_unretired_deployment_writes

    c = deployment_object

    def cancel():
        with c.factory() as db, db.begin():
            db.get(ModelRun, c.deployment_id).status = RunStatus.CANCELLED

    c.object_state["during_write"] = cancel
    assert c.deliver() == 1 and len(c.put_calls) == 1 and not c.kubernetes_calls
    with c.factory() as db:
        assert db.get(ModelRun, c.deployment_id).status == RunStatus.CANCELLED
        assert db.get(RunArtifact, c.dockerfile_id).artifact_metadata[RECORD]["receipt"]
        assert not has_unretired_deployment_writes(db, c.project_id, [c.deployment_id])
        _require_stopped_workflows(db, c.project_id, [c.deployment_id])


@pytest.mark.parametrize("same_owner", [False, True])
def test_successor_delivery_cannot_be_acknowledged_by_old_writer(deployment_object, same_owner):
    from automl_api.services.deployment_objects import reconcile_deployment_object

    c = deployment_object
    owner = "deployment-object-owner" if same_owner else "successor-object-owner"

    def replace():
        with c.factory() as db, db.begin():
            row = db.get(OutboxEntry, c.entry_id)
            row.lease_owner = owner
            row.delivery_attempts += 1
            row.lease_expires_at = datetime.now(UTC) + timedelta(minutes=2)

    c.object_state["during_write"] = replace
    assert c.deliver() == 1 and not c.kubernetes_calls
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.CLAIMED
    reconcile_deployment_object(c.factory, c.entry_id, owner)
    assert len(c.put_calls) == 1 and len(c.kubernetes_calls) == 1
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED


@pytest.mark.parametrize("field", ["params", "model_binding"])
def test_changed_source_after_native_write_cannot_create_kubernetes(deployment_object, field):
    c = deployment_object

    def changed():
        with c.factory() as db, db.begin():
            run = db.get(ModelRun, c.deployment_id)
            if field == "params":
                run.params = {**run.params, "replicas": 2}
            else:
                run.tags = {**run.tags, "model_custody_digest": "0" * 64}

    c.object_state["during_write"] = changed
    assert c.deliver() == 1 and len(c.put_calls) == 1 and not c.kubernetes_calls
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED


@pytest.mark.parametrize("kind", ["run", "artifact"])
def test_missing_original_protocol_cannot_upload_a_legacy_object(deployment_object, kind):
    c = deployment_object
    with c.factory() as db, db.begin():
        if kind == "run":
            row = db.get(ModelRun, c.deployment_id)
            row.tags = {k: v for k, v in row.tags.items() if k != "deployment_write_protocol"}
        else:
            row = db.get(RunArtifact, c.dockerfile_id)
            row.artifact_metadata = {
                k: v for k, v in row.artifact_metadata.items() if k != "write_protocol"
            }
    assert c.deliver() == 1 and not c.put_calls and not c.kubernetes_calls


def test_unresolved_write_blocks_deletion_and_cleanup(deployment_object):
    from automl_api.schemas.operations import ArtifactCleanupRequest
    from automl_api.services.deletion import _require_stopped_workflows
    from fastapi import HTTPException

    c = deployment_object
    start_unknown_write(c)
    with c.factory() as db:
        with pytest.raises(HTTPException, match="deployment object write retirement"):
            _require_stopped_workflows(db, c.project_id, [c.deployment_id])
        _require_stopped_workflows(db, c.project_id, [])
        artifact = db.get(RunArtifact, c.dockerfile_id)
        artifact.created_at = datetime.now(UTC) - timedelta(days=100)
        db.commit()
        result = operations.cleanup_project_resources(
            db,
            db.get(User, c.user_id),
            c.project_id,
            ArtifactCleanupRequest(older_than_days=1, dry_run=True, cleanup_finished_jobs=False),
        )
        assert c.dockerfile_id not in result.artifact_ids


def test_unresolved_write_blocks_previously_queued_cleanup(deployment_object):
    from automl_api.storage.contracts import DeletionPending

    c = deployment_object
    start_unknown_write(c)
    with c.factory() as db, db.begin():
        db.add(
            DeletionTombstone(
                project_id=c.project_id,
                requested_by_id=c.user_id,
                resource_type="run_artifact",
                resource_id=c.dockerfile_id,
                reason="owned queued cleanup",
            )
        )
        db.flush()
        entry = SimpleNamespace(
            project_id=c.project_id, payload={"artifact_ids": [str(c.dockerfile_id)]}
        )
        with pytest.raises(DeletionPending, match="verified retirement"):
            reconciler.reconcile_cleanup(db, entry, c.kubernetes)
        assert db.get(RunArtifact, c.dockerfile_id) is not None
    assert len(c.objects) == 1


def test_real_sql_connection_loss_recovers_the_original_native_write(deployment_object):
    from sqlalchemy import text

    c = deployment_object
    updates = []
    killed = []

    def terminate(connection, _cursor, statement, parameters, _context, _many):
        if not statement.lstrip().lower().startswith("update run_artifacts "):
            return
        values = parameters.values() if isinstance(parameters, dict) else parameters
        if not any(str(value) == str(c.dockerfile_id) for value in values):
            return
        updates.append(True)
        if len(updates) == 2:
            pid = connection.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
            with c.engine.begin() as control:
                assert control.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
            killed.append(pid)

    event.listen(c.engine, "before_cursor_execute", terminate)
    try:
        assert c.deliver() == 1
        assert len(killed) == 1 and len(c.put_calls) == 1 and not c.kubernetes_calls
        assert c.retry() == 1
        assert len(c.put_calls) == 1 and len(c.kubernetes_calls) == 1
        with c.factory() as db:
            assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
    finally:
        event.remove(c.engine, "before_cursor_execute", terminate)


def test_cancelled_command_still_recovers_only_its_native_completion(deployment_object):
    from automl_api.models.enums import RunStatus
    from automl_api.services.deployment_objects import has_unretired_deployment_writes

    c = deployment_object
    start_unknown_write(c)
    with c.factory() as db, db.begin():
        db.get(WorkflowCommand, c.command_id).status = CommandStatus.CANCELLED
        db.get(ModelRun, c.deployment_id).status = RunStatus.CANCELLED
    c.object_state["stat_unavailable"] = False
    assert c.retry() == 1 and len(c.put_calls) == 1 and not c.kubernetes_calls
    with c.factory() as db:
        assert db.get(WorkflowCommand, c.command_id).status == CommandStatus.CANCELLED
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
        assert not has_unretired_deployment_writes(db, c.project_id, [c.deployment_id])


@pytest.mark.parametrize("field", ["resource", "operation", "request_hash", "aggregate"])
def test_misbound_command_cannot_start_a_native_write(deployment_object, field):
    c = deployment_object
    with c.factory() as db, db.begin():
        command = db.get(WorkflowCommand, c.command_id)
        if field == "resource":
            command.resource_id = uuid.uuid4()
        elif field == "operation":
            command.operation = "different.operation"
        elif field == "request_hash":
            command.request_hash = "0" * 64
        else:
            db.get(OutboxEntry, c.entry_id).aggregate_id = uuid.uuid4()
    assert c.deliver() == 1 and not c.put_calls and not c.kubernetes_calls


@pytest.mark.parametrize("field", ["missing_record", "malformed_receipt"])
def test_malformed_started_write_cannot_be_retired(deployment_object, field):
    from automl_api.services.deployment_objects import RECORD, has_unretired_deployment_writes

    c = deployment_object
    start_unknown_write(c)
    with c.factory() as db, db.begin():
        artifact = db.get(RunArtifact, c.dockerfile_id)
        metadata = dict(artifact.artifact_metadata)
        if field == "missing_record":
            metadata.pop(RECORD)
        else:
            metadata[RECORD] = {**metadata[RECORD], "receipt": {"object_version": "null"}}
        artifact.artifact_metadata = metadata
    assert c.retry() == 1 and len(c.put_calls) == 1 and not c.kubernetes_calls
    with c.factory() as db:
        assert has_unretired_deployment_writes(db, c.project_id, [c.deployment_id])


def test_deployment_api_selects_versioned_object_dispatch(monkeypatch):
    from unittest.mock import MagicMock

    from automl_api.api.routes import operations as routes
    from automl_api.services.deployment_objects import TOPIC

    response = object()
    durable = MagicMock(return_value=response)
    monkeypatch.setattr(routes, "durable_mutation", durable)
    db = MagicMock()
    result = routes.deploy(
        uuid.uuid4(),
        uuid.uuid4(),
        ModelDeploymentRequest(),
        db,
        SimpleNamespace(),
        idempotency_key="owned-routing-control",
    )
    assert result is response
    assert durable.call_args.kwargs["outbox_topic"] == TOPIC == "deployment.reconcile.v4"
    db.commit.assert_called_once()


def test_current_producer_declares_pod_tls_without_platform_credentials(deployment_object):
    from automl_api.services.inference_tls import PROTOCOL, private_key_pem, serving_declaration

    c = deployment_object
    with c.factory() as db:
        run = db.get(ModelRun, c.deployment_id)
        entry = db.get(OutboxEntry, c.entry_id)
        assert entry.topic == TOPIC == "deployment.reconcile.v4"
        assert run.tags["inference_identity_protocol"] == PROTOCOL
        model = operations.deployment_model_custody(db, run)["completion"]
        values = serving_declaration(
            entry.payload["manifests"],
            project_id=c.project_id,
            run_id=c.deployment_id,
            model_digest=model["sha256"],
            model_uri=model["uri"],
            ca_digest=c.inference_authority.digest,
        )
        assert values["MODEL_DOWNLOAD_URL"].startswith("https://control.invalid/")
        encoded = json.dumps(entry.payload)
        assert "INFERENCE_GATEWAY_TOKEN" not in encoded and "DATABASE_URL" not in encoded
        assert private_key_pem(c.inference_authority.private_key).decode() not in encoded
        assert 'CMD ["python", "-m", "automl_api.inference.runtime"]' in c.dockerfile_bytes.decode()


@pytest.mark.parametrize(
    "fault",
    [
        "static-token",
        "database-env",
        "legacy-command",
        "http-data",
        "health-on-data",
        "api-token",
        "host-network",
    ],
)
def test_new_topic_cannot_submit_legacy_or_broader_credentials(deployment_object, fault):
    import copy

    c = deployment_object
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        payload = copy.deepcopy(entry.payload)
        pod = payload["manifests"]["deployment"]["spec"]["template"]["spec"]
        container = pod["containers"][0]
        if fault == "static-token":
            container["env"].append(dict(name="INFERENCE_GATEWAY_TOKEN", value="foreign"))
        elif fault == "database-env":
            container["env"].append(
                dict(
                    name="DATABASE_URL",
                    valueFrom=dict(secretKeyRef=dict(name="platform", key="database-url")),
                )
            )
        elif fault == "legacy-command":
            container["command"] = ["uvicorn", "automl_api.inference.app:app"]
        elif fault == "http-data":
            container["ports"][0] = dict(name="http", containerPort=8080)
        elif fault == "health-on-data":
            container["readinessProbe"]["httpGet"]["port"] = "https"
        elif fault == "api-token":
            pod["automountServiceAccountToken"] = True
        else:
            pod["hostNetwork"] = True
        entry.payload = payload
    assert c.deliver() == 1
    assert not c.put_calls and not c.native_posts and not c.kubernetes_calls


@pytest.mark.parametrize("provider", ["s3", "gcs", "azure"])
@pytest.mark.parametrize("grant", [None, "a" * 64])
def test_native_stat_preserves_original_write_metadata(provider, grant):
    metadata = {"sceptre_grant": grant} if grant else {}
    if provider == "s3":
        from automl_api.storage.s3 import S3ObjectStoreDriver

        client = SimpleNamespace(
            head_object=lambda **_: dict(ContentLength=1, VersionId="v1", Metadata=metadata)
        )
        store = S3ObjectStoreDriver(bucket="owned", client=client)
    elif provider == "gcs":
        from automl_api.storage.gcs import GCSObjectStoreDriver

        store = object.__new__(GCSObjectStoreDriver)
        store.bucket_name = "owned"
        blob = SimpleNamespace(
            name="key",
            size=1,
            generation=1,
            metageneration=1,
            etag="one",
            crc32c=None,
            md5_hash=None,
            content_type=None,
            metadata=metadata,
        )
        store.bucket = SimpleNamespace(get_blob=lambda _: blob)
    else:
        from automl_api.storage.azure import AzureBlobObjectStoreDriver

        store = object.__new__(AzureBlobObjectStoreDriver)
        store.account_name, store.container_name = "account", "owned"
        properties = SimpleNamespace(size=1, version_id="v1", etag="one", metadata=metadata)
        store.container = SimpleNamespace(
            get_blob_client=lambda _: SimpleNamespace(
                get_blob_properties=lambda: properties, blob_name="key"
            )
        )
    value = store.stat(store.uri_for_key("key"))
    assert value.provider_headers.get("sceptre_grant") == grant
