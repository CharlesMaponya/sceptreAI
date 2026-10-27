"""Lifecycle mutations must retain original native identity."""

import copy
import uuid
from types import SimpleNamespace

import pytest
from automl_api.core.config import Settings
from automl_api.models.enums import RunStatus
from automl_api.services import operations
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.workflow_state import canonical_request_hash
from fastapi import HTTPException
from kubernetes.client import ApiException
from test_deployment_object_custody import (
    bound_run as bound_run,
)
from test_deployment_object_custody import (
    capacity_client as capacity_client,
)
from test_deployment_object_custody import (
    committed_tenant as committed_tenant,
)
from test_deployment_object_custody import (
    context_api as context_api,
)
from test_deployment_object_custody import (
    deployment_object as deployment_object,
)
from test_deployment_object_custody import (
    model_api as model_api,
)
from test_deployment_object_custody import (
    phase1_engine as phase1_engine,
)
from test_deployment_object_custody import (
    qualified_model as qualified_model,
)
from test_deployment_object_custody import (
    selected_scope as selected_scope,
)
from test_deployment_object_custody import (
    serving_model as serving_model,
)


def identity_client(*, ingress=False, name=None):
    c = SimpleNamespace()
    c.project_id, c.run_id = uuid.uuid4(), uuid.uuid4()
    k8s = object.__new__(KubernetesTrainingClient)
    k8s.settings = Settings(
        training_namespace="owned-lifecycle",
        inference_ingress_enabled=ingress,
        inference_ingress_host_template="{name}.invalid",
    )
    manifests = k8s.build_model_deployment_manifest(
        identity_ca_digest="c" * 64,
        identity_control_base_url="https://control.invalid/api/v1/internal/deployments",
        deployment_id=c.run_id,
        project_id=c.project_id,
        project_name="Owned lifecycle",
        environment="test",
        model_name="Ridge",
        model_uri="s3://owned/model",
        model_digest="a" * 64,
        image="registry.invalid/never-pull:fixture",
        replicas=2,
        cpu_request="1",
        memory_request="1Gi",
    )
    if name is not None:
        for resource in manifests.values():
            resource["metadata"]["name"] = name
        if "ingress" in manifests:
            for rule in manifests["ingress"]["spec"]["rules"]:
                for path in rule["http"]["paths"]:
                    path["backend"]["service"]["name"] = name
    manifests = {kind: copy.deepcopy(body) for kind, body in manifests.items()}
    c.namespace_uid = "original-namespace-uid"
    c.target = dict(
        namespace=k8s.settings.training_namespace,
        namespace_uid=c.namespace_uid,
        intent_digest="b" * 64,
        journal_digest="c" * 64,
        artifact_id=str(uuid.uuid4()),
        manifests=manifests,
        resources={},
    )
    c.stored, c.mutations, c.reads, c.pods, c.replica_sets = {}, [], [], [], []
    c.held = False
    c.before_patch = None
    for kind, manifest in manifests.items():
        manifest["metadata"]["annotations"] = {"automl.platform/creation-intent": "d" * 64}
        current = copy.deepcopy(manifest)
        current["metadata"].update(uid="original-" + kind, resourceVersion="1")
        c.stored[kind] = current
        c.target["resources"][kind] = dict(
            uid=current["metadata"]["uid"],
            spec_digest=canonical_request_hash(current["spec"]),
            request_digest=canonical_request_hash(manifest),
            intent_digest=c.target["intent_digest"],
        )

    def read(kind, **kwargs):
        c.reads.append(kind)
        if kind not in c.stored:
            raise ApiException(status=404)
        return copy.deepcopy(c.stored[kind])

    def remove(kind, **kwargs):
        c.mutations.append((kind, "delete", kwargs))
        if kind not in c.stored:
            raise ApiException(status=404)
        body = kwargs.get("body")
        uid = body.preconditions.uid if body is not None else None
        if uid is not None and uid != c.stored[kind]["metadata"]["uid"]:
            raise ApiException(status=409)
        if not c.held:
            c.stored.pop(kind)

    def legacy_patch(**kwargs):
        c.mutations.append(("deployment", "unconditional", kwargs))
        c.stored["deployment"]["spec"]["replicas"] = kwargs["body"]["spec"]["replicas"]

    def patch(**kwargs):
        c.mutations.append(("deployment", "conditional", kwargs))
        if c.before_patch:
            c.before_patch()
        current = c.stored["deployment"]
        body = kwargs["body"]
        assert body[0] == {
            "op": "test",
            "path": "/metadata/uid",
            "value": c.target["resources"]["deployment"]["uid"],
        }
        if (
            body[0]["value"] != current["metadata"]["uid"]
            or body[1]["value"] != current["metadata"]["resourceVersion"]
        ):
            raise ApiException(status=409)
        assert body[1]["op"] == "test" and body[1]["path"] == "/metadata/resourceVersion"
        assert body[2]["op"] == "replace" and body[2]["path"] == "/spec/replicas"
        current["spec"]["replicas"] = body[2]["value"]
        current["metadata"]["resourceVersion"] = str(
            int(current["metadata"]["resourceVersion"]) + 1
        )
        return copy.deepcopy(current)

    k8s.apps = SimpleNamespace(
        read_namespaced_deployment=lambda **kw: read("deployment", **kw),
        patch_namespaced_deployment_scale=legacy_patch,
        patch_namespaced_deployment=patch,
        delete_namespaced_deployment=lambda **kw: remove("deployment", **kw),
        list_namespaced_replica_set=lambda **kw: SimpleNamespace(
            items=c.replica_sets, metadata=SimpleNamespace(_continue=None)
        ),
    )
    k8s.core = SimpleNamespace(
        read_namespace=lambda *args, **kw: SimpleNamespace(
            metadata=SimpleNamespace(
                name=k8s.settings.training_namespace, uid=c.namespace_uid, deletion_timestamp=None
            )
        ),
        read_namespaced_service=lambda **kw: read("service", **kw),
        delete_namespaced_service=lambda **kw: remove("service", **kw),
        list_namespaced_pod=lambda **kw: SimpleNamespace(
            items=c.pods, metadata=SimpleNamespace(_continue=None)
        ),
    )
    k8s.networking = SimpleNamespace(
        read_namespaced_ingress=lambda **kw: read("ingress", **kw),
        delete_namespaced_ingress=lambda **kw: remove("ingress", **kw),
    )
    c.k8s = k8s
    c.run = SimpleNamespace(
        id=c.run_id,
        project_id=c.project_id,
        k8s_namespace=k8s.settings.training_namespace,
        k8s_job_name=manifests["deployment"]["metadata"]["name"],
        status=RunStatus.RUNNING,
        tags={"desired_state": "deployment_active"},
        params={"replicas": 2},
        finished_at=None,
        failure_code=None,
        failure_message=None,
    )
    c.db = SimpleNamespace(
        scalar=lambda *_: c.run,
        flush=lambda: None,
        get=lambda *a, **kw: SimpleNamespace(settings={}),
    )
    return c


@pytest.fixture
def lifecycle(monkeypatch):
    c = identity_client()
    monkeypatch.setattr(operations, "require_project_role", lambda *_: None)
    monkeypatch.setattr(operations, "deployment_model_custody", lambda *_: None)
    monkeypatch.setattr(
        operations, "lifecycle_custody", lambda *_, **_kwargs: c.target, raising=False
    )
    monkeypatch.setattr(operations, "record_lifecycle_retirement", lambda *_: None, raising=False)
    return c


@pytest.mark.parametrize("action", ["stop", "start", "cleanup"])
def test_foreign_same_name_resource_cannot_be_mutated(lifecycle, action):
    c = lifecycle
    c.stored["deployment"]["metadata"]["uid"] = "foreign-deployment-uid"
    if action != "stop":
        c.run.status = RunStatus.CANCELLED
    before = copy.deepcopy(c.stored)
    with pytest.raises(ValueError):
        if action == "stop":
            c.k8s.shutdown_model_deployment(c.run.k8s_job_name, custody=c.target)
        elif action == "start":
            c.k8s.start_model_deployment(c.run.k8s_job_name, 2, custody=c.target)
        else:
            c.k8s.delete_model_deployment(c.run.k8s_job_name, custody=c.target)
    assert not c.mutations and c.stored == before


def test_original_uid_can_stop_and_restart_with_conditional_versions():
    c = identity_client(ingress=True)
    name = c.run.k8s_job_name
    c.k8s.shutdown_model_deployment(name, custody=c.target)
    assert c.stored["deployment"]["spec"]["replicas"] == 0
    c.k8s.start_model_deployment(name, 2, custody=c.target)
    assert c.stored["deployment"]["spec"]["replicas"] == 2
    assert [kind for kind, action, _ in c.mutations if action == "conditional"] == [
        "deployment",
        "deployment",
    ]
    assert [kwargs["body"][1]["value"] for _, _, kwargs in c.mutations] == ["1", "2"]


@pytest.mark.parametrize("field", ["uid", "resourceVersion"])
def test_scale_race_cannot_patch_a_replacement_or_newer_version(field):
    c = identity_client()

    def change():
        c.stored["deployment"]["metadata"][field] = "replacement" if field == "uid" else "2"

    c.before_patch = change
    with pytest.raises(ApiException) as failure:
        c.k8s.shutdown_model_deployment(c.run.k8s_job_name, custody=c.target)
    assert failure.value.status == 409
    assert c.stored["deployment"]["spec"]["replicas"] == 2


@pytest.mark.parametrize("action", ["start", "cleanup"])
@pytest.mark.parametrize("kind", ["deployment", "service", "ingress"])
@pytest.mark.parametrize("fault", ["uid", "spec", "intent"])
def test_every_native_target_is_checked_before_mutation(action, kind, fault):
    c = identity_client(ingress=True)
    actual = c.stored[kind]
    if fault == "uid":
        actual["metadata"]["uid"] = "foreign"
    elif fault == "intent":
        actual["metadata"]["annotations"].clear()
    elif kind == "deployment":
        actual["spec"]["template"]["spec"]["containers"][0]["image"] = "foreign/image:fixture"
    elif kind == "service":
        actual["spec"]["ports"][0]["port"] = 9999
    else:
        actual["spec"]["rules"][0]["host"] = "foreign.invalid"
    original = copy.deepcopy(c.stored)
    with pytest.raises(ValueError):
        if action == "start":
            c.k8s.start_model_deployment(c.run.k8s_job_name, 2, custody=c.target)
        else:
            c.k8s.delete_model_deployment(c.run.k8s_job_name, custody=c.target)
    assert not c.mutations and c.stored == original


@pytest.mark.parametrize("action", ["stop", "start", "cleanup"])
def test_changed_namespace_lifetime_cannot_mutate(action):
    c = identity_client()
    c.namespace_uid = "replacement-namespace"
    with pytest.raises(ValueError):
        if action == "stop":
            c.k8s.shutdown_model_deployment(c.run.k8s_job_name, custody=c.target)
        elif action == "start":
            c.k8s.start_model_deployment(c.run.k8s_job_name, 2, custody=c.target)
        else:
            c.k8s.delete_model_deployment(c.run.k8s_job_name, custody=c.target)
    assert not c.mutations


@pytest.mark.parametrize("remaining", ["resource", "replica_set", "pod", "cursor"])
def test_cleanup_does_not_finish_before_native_retirement(remaining):
    from automl_api.services.kubernetes_training import ModelDeploymentRetirementPending

    c = identity_client()
    if remaining == "resource":
        c.held = True
    elif remaining == "replica_set":
        c.replica_sets.append(object())
    elif remaining == "pod":
        c.pods.append(object())
    else:
        c.k8s.core.list_namespaced_pod = lambda **kw: SimpleNamespace(
            items=[], metadata=SimpleNamespace(_continue="more")
        )
    with pytest.raises(ModelDeploymentRetirementPending):
        c.k8s.delete_model_deployment(c.run.k8s_job_name, custody=c.target)
    assert all(
        kwargs["body"].preconditions.uid == c.target["resources"][kind]["uid"]
        and kwargs["body"].propagation_policy == "Foreground"
        for kind, _, kwargs in c.mutations
    )


def test_cleanup_returns_only_original_retirement_and_is_physically_idempotent():
    c = identity_client(ingress=True)
    receipt = c.k8s.delete_model_deployment(c.run.k8s_job_name, custody=c.target)
    assert receipt == dict(
        journal_digest=c.target["journal_digest"],
        namespace_uid=c.target["namespace_uid"],
        resource_uids={k: v["uid"] for k, v in c.target["resources"].items()},
        workload_absent=True,
    )
    assert not c.stored and len(c.mutations) == 3
    assert c.k8s.delete_model_deployment(c.run.k8s_job_name, custody=c.target) == receipt
    assert len(c.mutations) == 3


@pytest.mark.parametrize("status", [403, 500])
def test_cleanup_api_errors_do_not_become_retirement(status):
    c = identity_client()

    def fail(**kwargs):
        raise ApiException(status=status)

    c.k8s.core.delete_namespaced_service = fail
    with pytest.raises(ApiException) as failure:
        c.k8s.delete_model_deployment(c.run.k8s_job_name, custody=c.target)
    assert failure.value.status == status and "deployment" in c.stored


def test_started_marker_without_resource_history_stays_unretired():
    from automl_api.services.deployment_submission import unretired_submission

    intent = dict(
        protocol="deployment-kubernetes-v1",
        write_id=str(uuid.uuid4()),
        outbox_id=str(uuid.uuid4()),
        source_digest="a" * 64,
        namespace="owned",
        namespace_uid="original",
        manifest_digest="b" * 64,
        object_receipt_digest="c" * 64,
    )
    artifact = SimpleNamespace(
        artifact_metadata={
            "kubernetes_creation_started": True,
            "deployment_kubernetes": {"intent": intent, "resources": {}},
        }
    )
    assert unretired_submission(artifact)


@pytest.fixture
def native_lifecycle(deployment_object, monkeypatch):
    c = deployment_object
    assert c.deliver() == 1
    c.native_mutations = []
    c.native_pods = []
    for resource in c.native_resources.values():
        resource["metadata"]["resourceVersion"] = "1"

    for resource in c.native_resources.values():
        resource["metadata"]["generation"] = 1
        resource["status"] = {"observedGeneration": 1}

    def patch(**kwargs):
        body = kwargs["body"]
        value = c.native_resources["deployment"]
        assert body[0] == {"op": "test", "path": "/metadata/uid", "value": value["metadata"]["uid"]}
        assert body[1] == {
            "op": "test",
            "path": "/metadata/resourceVersion",
            "value": value["metadata"]["resourceVersion"],
        }
        old = value["spec"]["replicas"]
        for operation in body[2:]:
            if operation["path"] == "/spec/replicas":
                value["spec"]["replicas"] = operation["value"]
            else:
                value["metadata"]["annotations"][
                    operation["path"].removeprefix("/metadata/annotations/").replace("~1", "/")
                ] = operation["value"]
        c.native_mutations.append(("patch", value["spec"]["replicas"]))
        value["metadata"]["resourceVersion"] = str(int(value["metadata"]["resourceVersion"]) + 1)
        if value["spec"]["replicas"] != old:
            value["metadata"]["generation"] += 1
        value["status"]["observedGeneration"] = value["metadata"]["generation"]
        return copy.deepcopy(value)

    def remove(kind, **kwargs):
        if kind not in c.native_resources:
            raise ApiException(status=404)
        uid = c.native_resources[kind]["metadata"]["uid"]
        assert kwargs["body"].preconditions.uid == uid
        assert kwargs["body"].propagation_policy == "Foreground"
        c.native_mutations.append(("delete", kind, uid))
        c.native_resources.pop(kind)

    c.kubernetes.apps.patch_namespaced_deployment = patch
    c.kubernetes.apps.delete_namespaced_deployment = lambda **kw: remove("deployment", **kw)
    c.kubernetes.core.delete_namespaced_service = lambda **kw: remove("service", **kw)
    c.kubernetes.networking.delete_namespaced_ingress = lambda **kw: remove("ingress", **kw)
    c.kubernetes.apps.list_namespaced_replica_set = lambda **kw: SimpleNamespace(
        items=[], metadata=SimpleNamespace(_continue=None)
    )
    c.kubernetes.core.list_namespaced_pod = lambda **kw: SimpleNamespace(
        items=c.native_pods, metadata=SimpleNamespace(_continue=None)
    )
    from automl_api.services import deployment_controls

    monkeypatch.setattr(deployment_controls, "KubernetesTrainingClient", lambda: c.kubernetes)
    return c


def control(c, action):
    from automl_api.models.iam import User

    with c.factory() as db, db.begin():
        result = getattr(operations, action + "_model_deployment")(
            db, db.get(User, c.user_id), c.project_id, c.deployment_id, c.kubernetes
        )
        return SimpleNamespace(status=result.status)


def step_control(c):
    from datetime import UTC, datetime, timedelta

    from automl_api.models.workflows import OutboxEntry
    from automl_api.services.deployment_controls import TOPIC
    from automl_api.workflow_reconciler import run_once
    from sqlalchemy import update

    with c.factory() as db, db.begin():
        db.execute(
            update(OutboxEntry)
            .where(OutboxEntry.project_id == c.project_id, OutboxEntry.topic == TOPIC)
            .values(available_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    return run_once(c.factory, worker_id="lifecycle-control-owner")


def finish_control(c, action):
    from automl_api.models.runs import ModelRun

    control(c, action)
    step_control(c)
    with c.factory() as db:
        return SimpleNamespace(status=db.get(ModelRun, c.deployment_id).status)


def test_qualified_lifecycle_persists_exact_retirement_and_releases_barrier(native_lifecycle):
    from automl_api.models.runs import RunArtifact
    from automl_api.services.deployment_objects import has_unretired_deployment_writes
    from automl_api.services.deployment_submission import RETIREMENT, retirement_receipt

    c = native_lifecycle
    assert finish_control(c, "stop").status == RunStatus.CANCELLED
    assert finish_control(c, "start").status == RunStatus.RUNNING
    assert finish_control(c, "stop").status == RunStatus.CANCELLED
    assert [value for kind, value, *_ in c.native_mutations if kind == "patch"] == [0, 1, 0]
    finish_control(c, "cleanup")
    before = list(c.native_mutations)
    finish_control(c, "cleanup")
    assert c.native_mutations == before
    assert not c.native_resources
    with c.factory() as db:
        artifact = db.get(RunArtifact, c.dockerfile_id)
        assert artifact.artifact_metadata[RETIREMENT] == retirement_receipt(
            artifact.artifact_metadata["deployment_kubernetes"]
        )
        assert not has_unretired_deployment_writes(db, c.project_id, [c.deployment_id])


@pytest.mark.parametrize("fault", ["receipt", "manifest", "namespace", "aggregate"])
def test_lifecycle_requires_original_persisted_admission_and_receipts(native_lifecycle, fault):
    from automl_api.models.runs import ModelRun, RunArtifact
    from automl_api.models.workflows import OutboxEntry

    c = native_lifecycle
    with c.factory() as db, db.begin():
        if fault == "receipt":
            artifact = db.get(RunArtifact, c.dockerfile_id)
            metadata = copy.deepcopy(artifact.artifact_metadata)
            metadata["deployment_kubernetes"]["resources"]["deployment"].pop("receipt")
            artifact.artifact_metadata = metadata
        elif fault == "namespace":
            db.get(ModelRun, c.deployment_id).k8s_namespace = "foreign-namespace"
        elif fault == "aggregate":
            db.get(OutboxEntry, c.entry_id).aggregate_id = uuid.uuid4()
        else:
            entry = db.get(OutboxEntry, c.entry_id)
            payload = copy.deepcopy(entry.payload)
            payload["manifests"]["service"]["spec"]["ports"][0]["port"] = 9999
            entry.payload = payload
    with pytest.raises(HTTPException) as failure:
        control(c, "stop")
    assert failure.value.status_code == 409 and not c.native_mutations


def test_retirement_wait_keeps_metadata_and_deletion_barrier(native_lifecycle):
    from automl_api.models.runs import ModelRun, RunArtifact
    from automl_api.services.deployment_objects import has_unretired_deployment_writes
    from automl_api.services.deployment_submission import RETIREMENT

    c = native_lifecycle
    finish_control(c, "stop")
    c.native_pods.append(object())
    control(c, "cleanup")
    assert step_control(c) == 1
    assert not c.native_resources
    with c.factory() as db:
        assert not db.get(ModelRun, c.deployment_id).tags.get("runtime_cleaned_at")
        assert RETIREMENT not in db.get(RunArtifact, c.dockerfile_id).artifact_metadata
        assert has_unretired_deployment_writes(db, c.project_id, [c.deployment_id])
    c.native_pods.clear()
    assert step_control(c) == 1
    with c.factory() as db:
        assert not has_unretired_deployment_writes(db, c.project_id, [c.deployment_id])


def test_actual_sql_backend_loss_after_native_retirement_recovers_original_uids(native_lifecycle):
    from automl_api.models.runs import ModelRun
    from automl_api.services.deployment_objects import has_unretired_deployment_writes
    from sqlalchemy import event, text

    c = native_lifecycle
    finish_control(c, "stop")
    control(c, "cleanup")
    killed = []

    def terminate(connection, _cursor, statement, parameters, _context, _many):
        if killed or not statement.lstrip().lower().startswith("update run_artifacts "):
            return
        values = parameters.values() if isinstance(parameters, dict) else parameters
        if not any(str(value) == str(c.dockerfile_id) for value in values):
            return
        pid = connection.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
        with c.engine.begin() as inspector:
            assert inspector.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
        killed.append(pid)

    event.listen(c.engine, "before_cursor_execute", terminate)
    try:
        assert step_control(c) == 1
        assert len(killed) == 1 and not c.native_resources
    finally:
        event.remove(c.engine, "before_cursor_execute", terminate)
    before = list(c.native_mutations)
    with c.factory() as db:
        assert not db.get(ModelRun, c.deployment_id).tags.get("runtime_cleaned_at")
        assert has_unretired_deployment_writes(db, c.project_id, [c.deployment_id])
    assert step_control(c) == 1
    assert c.native_mutations == before
    with c.factory() as db:
        assert not has_unretired_deployment_writes(db, c.project_id, [c.deployment_id])


@pytest.mark.parametrize("fault", ["missing", "namespace", "uid", "boolean"])
def test_invalid_retirement_receipt_cannot_release_barrier(native_lifecycle, fault):
    from automl_api.models.runs import RunArtifact
    from automl_api.services.deployment_objects import has_unretired_deployment_writes
    from automl_api.services.deployment_submission import RETIREMENT, retirement_receipt

    c = native_lifecycle
    with c.factory() as db, db.begin():
        artifact = db.get(RunArtifact, c.dockerfile_id)
        metadata = copy.deepcopy(artifact.artifact_metadata)
        receipt = retirement_receipt(metadata["deployment_kubernetes"])
        if fault == "missing":
            receipt = {}
        elif fault == "namespace":
            receipt["namespace_uid"] = "foreign"
        elif fault == "uid":
            receipt["resource_uids"]["deployment"] = "foreign"
        else:
            receipt["workload_absent"] = 1
        artifact.artifact_metadata = {**metadata, RETIREMENT: receipt}
    with c.factory() as db:
        assert has_unretired_deployment_writes(db, c.project_id, [c.deployment_id])


def test_modern_unstarted_launch_can_be_cancelled_and_cleaned_without_native_calls(
    deployment_object,
):
    from automl_api.models.runs import ModelRun
    from automl_api.services.deployment_objects import has_unretired_deployment_writes

    c = deployment_object
    assert control(c, "stop").status == RunStatus.CANCELLED
    control(c, "cleanup")
    assert c.kubernetes_calls == [] and c.native_posts == []
    with c.factory() as db:
        assert db.get(ModelRun, c.deployment_id).tags["runtime_cleaned_at"]
        assert not has_unretired_deployment_writes(db, c.project_id, [c.deployment_id])
    assert c.deliver() == 1 and not c.native_posts and not c.put_calls


def test_cleanup_uses_original_ingress_even_if_current_setting_is_disabled():
    c = identity_client(ingress=True)
    from dataclasses import replace

    c.k8s.settings = replace(c.k8s.settings, inference_ingress_enabled=False)
    c.k8s.delete_model_deployment(c.run.k8s_job_name, custody=c.target)
    assert [kind for kind, _, _ in c.mutations] == ["ingress", "service", "deployment"]
