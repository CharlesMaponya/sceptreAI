"""Original deployment observations and two-pass publication fences."""

import pytest
from test_deployment_control_workflows import controls as controls
from test_deployment_lifecycle_identity import (
    bound_run as bound_run,
)
from test_deployment_lifecycle_identity import (
    capacity_client as capacity_client,
)
from test_deployment_lifecycle_identity import (
    committed_tenant as committed_tenant,
)
from test_deployment_lifecycle_identity import (
    context_api as context_api,
)
from test_deployment_lifecycle_identity import (
    control as control,
)
from test_deployment_lifecycle_identity import (
    deployment_object as deployment_object,
)
from test_deployment_lifecycle_identity import (
    model_api as model_api,
)
from test_deployment_lifecycle_identity import (
    native_lifecycle as native_lifecycle,
)
from test_deployment_lifecycle_identity import (
    phase1_engine as phase1_engine,
)
from test_deployment_lifecycle_identity import (
    qualified_model as qualified_model,
)
from test_deployment_lifecycle_identity import (
    selected_scope as selected_scope,
)
from test_deployment_lifecycle_identity import (
    serving_model as serving_model,
)


def read_deployments(c, client=None):
    from automl_api.models.iam import User
    from automl_api.services import operations

    client = client or c.kubernetes
    with c.factory() as db, db.begin():
        value = operations.list_model_deployments(db, db.get(User, c.user_id), c.project_id, client)
    if type(value).__name__ == "DeploymentInspection":
        inspected = operations.inspect_model_deployments(value, client=client)
        with c.factory() as db, db.begin():
            value = operations.list_model_deployments(
                db, db.get(User, c.user_id), c.project_id, client, inspection=inspected
            )
    return next(item for item in value if item.run.id == c.deployment_id)


@pytest.mark.parametrize("fault", ["sql-held", "foreign-uid"])
def test_deployment_observation_requires_original_uid_without_sql(controls, fault):
    import uuid

    from automl_api.models.enums import RunStatus

    c = controls

    def legacy_read(_):
        c.native_reads.append(("legacy-name-read", c.engine.pool.checkedout()))
        return "ready"

    c.kubernetes.model_deployment_state = legacy_read
    c.kubernetes.model_deployment_urls = lambda _: {}
    if fault == "foreign-uid":
        c.native_resources["deployment"]["metadata"]["uid"] = str(uuid.uuid4())
    row = read_deployments(c)
    if fault == "sql-held":
        assert c.native_reads and all(count == 0 for _, count in c.native_reads)
    else:
        assert row.runtime_state == "unavailable" and row.status == RunStatus.RUNNING


def native_workload(k8s, resources, record=lambda _: None):
    import copy
    import json
    from types import SimpleNamespace

    from kubernetes.client import ApiClient

    deployment = resources["deployment"]
    metadata, spec = deployment["metadata"], deployment["spec"]
    metadata.setdefault("generation", 1)
    deployment["status"] = dict(
        observedGeneration=metadata["generation"], availableReplicas=spec["replicas"]
    )
    replica = dict(
        apiVersion="apps/v1",
        kind="ReplicaSet",
        metadata=dict(
            name="owned-rs",
            uid="owned-rs-uid",
            namespace=metadata["namespace"],
            labels=copy.deepcopy(metadata["labels"]),
            ownerReferences=[
                dict(
                    apiVersion="apps/v1",
                    kind="Deployment",
                    name=metadata["name"],
                    uid=metadata["uid"],
                    controller=True,
                )
            ],
        ),
        spec=dict(
            replicas=spec["replicas"],
            selector=copy.deepcopy(spec["selector"]),
            template=copy.deepcopy(spec["template"]),
        ),
    )
    pods = [
        dict(
            apiVersion="v1",
            kind="Pod",
            metadata=dict(
                name=f"owned-pod-{i}",
                uid=f"owned-pod-uid-{i}",
                namespace=metadata["namespace"],
                labels=copy.deepcopy(metadata["labels"]),
                ownerReferences=[
                    dict(
                        apiVersion="apps/v1",
                        kind="ReplicaSet",
                        name="owned-rs",
                        uid="owned-rs-uid",
                        controller=True,
                    )
                ],
            ),
            spec=copy.deepcopy(spec["template"]["spec"]),
            status=dict(conditions=[dict(type="Ready", status="True")], containerStatuses=[]),
        )
        for i in range(spec["replicas"])
    ]
    state = dict(replica_sets=[replica], pods=pods, continuation=None)

    def listed(key, **kwargs):
        record(key)
        assert (
            kwargs["namespace"] == metadata["namespace"]
            and kwargs["limit"] in {1, 100}
            and kwargs["_request_timeout"] == 5
        )
        return SimpleNamespace(
            items=[
                ApiClient().deserialize(
                    SimpleNamespace(data=json.dumps(body)),
                    "V1ReplicaSet" if key == "replica_sets" else "V1Pod",
                )
                for body in state[key]
            ],
            metadata=SimpleNamespace(_continue=state["continuation"]),
        )

    k8s.apps.list_namespaced_replica_set = lambda **kw: listed("replica_sets", **kw)
    k8s.core.list_namespaced_pod = lambda **kw: listed("pods", **kw)
    return state


@pytest.fixture
def observed(controls):
    c = controls
    c.observation = native_workload(
        c.kubernetes,
        c.native_resources,
        lambda label: c.native_reads.append((label, c.engine.pool.checkedout())),
    )
    return c


def select_observation(c):
    from automl_api.models.iam import User
    from automl_api.services import operations

    with c.factory() as db, db.begin():
        selected = operations.list_model_deployments(
            db, db.get(User, c.user_id), c.project_id, c.kubernetes
        )
        assert isinstance(selected, operations.DeploymentInspection)
        return selected


def inspect_observation(c, selected):
    from automl_api.services import operations

    return operations.inspect_model_deployments(selected, client=c.kubernetes)


def publish_observation(c, inspected):
    from automl_api.models.iam import User
    from automl_api.services import operations

    with c.factory() as db, db.begin():
        rows = operations.list_model_deployments(
            db, db.get(User, c.user_id), c.project_id, c.kubernetes, inspection=inspected
        )
        return next(row for row in rows if row.run.id == c.deployment_id)


@pytest.mark.parametrize(
    "fault",
    [
        "namespace",
        "deployment-uid",
        "service-uid",
        "ingress-uid",
        "spec",
        "marker",
        "replicas",
        "replica-owner",
        "replica-template",
        "pod-owner",
        "pod-template",
        "duplicate-pod",
        "continuation",
        "resource-version",
        "native-change",
    ],
)
def test_native_observation_refuses_foreign_or_incomplete_workloads(fault):
    import copy
    import time

    from test_deployment_lifecycle_identity import identity_client

    c = identity_client(ingress=True)
    state = native_workload(c.k8s, c.stored)
    if fault == "namespace":
        c.namespace_uid = "replacement-namespace"
    elif fault.endswith("-uid"):
        c.stored[fault.removesuffix("-uid")]["metadata"]["uid"] = "replacement-uid"
    elif fault == "spec":
        c.stored["deployment"]["spec"]["template"]["spec"]["containers"][0]["image"] = (
            "foreign:model"
        )
    elif fault == "marker":
        c.stored["deployment"]["metadata"]["annotations"]["automl.platform/control-generation"] = (
            "1"
        )
    elif fault == "replicas":
        c.stored["deployment"]["spec"]["replicas"] = 3
    elif fault == "replica-owner":
        state["replica_sets"][0]["metadata"]["ownerReferences"][0]["uid"] = "foreign-deployment"
    elif fault == "replica-template":
        state["replica_sets"][0]["spec"]["template"]["spec"]["containers"][0]["image"] = (
            "foreign:model"
        )
    elif fault == "pod-owner":
        state["pods"][0]["metadata"]["ownerReferences"][0]["uid"] = "foreign-replica-set"
    elif fault == "pod-template":
        state["pods"][0]["spec"]["containers"][0]["image"] = "foreign:model"
    elif fault == "duplicate-pod":
        state["pods"].append(copy.deepcopy(state["pods"][0]))
    elif fault == "continuation":
        state["continuation"] = "more-owned-pods"
    elif fault == "resource-version":
        c.stored["deployment"]["metadata"].pop("resourceVersion")
    elif fault == "native-change":
        original = c.k8s.apps.read_namespaced_deployment
        calls = []

        def changed(**kwargs):
            value = original(**kwargs)
            calls.append(True)
            if len(calls) == 1:
                c.stored["deployment"]["metadata"]["resourceVersion"] = "2"
            return value

        c.k8s.apps.read_namespaced_deployment = changed
    with pytest.raises(ValueError):
        c.k8s.observe_model_deployment(
            c.run.k8s_job_name,
            custody=c.target,
            control=dict(generation=0, command_id=None, replicas=2),
            deadline=time.monotonic() + 30,
        )
    assert not c.mutations


@pytest.mark.parametrize(
    "fault,expected",
    [
        ("none", "ready"),
        ("no-pods", "progressing"),
        ("unobserved-generation", "progressing"),
        ("not-ready", "progressing"),
        ("stale-availability", "crash_loop"),
        ("missing", "missing"),
    ],
)
def test_native_readiness_requires_original_ready_workload(fault, expected):
    import time

    from test_deployment_lifecycle_identity import identity_client

    c = identity_client(ingress=True)
    state = native_workload(c.k8s, c.stored)
    if fault == "no-pods":
        state["pods"].clear()
    elif fault == "unobserved-generation":
        c.stored["deployment"]["status"]["observedGeneration"] = 0
    elif fault == "not-ready":
        state["pods"][0]["status"]["conditions"][0]["status"] = "False"
    elif fault == "stale-availability":
        pod = state["pods"][0]
        pod["status"]["conditions"][0]["status"] = "False"
        container = pod["spec"]["containers"][0]
        pod["status"]["containerStatuses"] = [
            dict(
                name=container["name"],
                image=container["image"],
                imageID="sha256:owned",
                ready=False,
                restartCount=2,
                state=dict(waiting=dict(reason="CrashLoopBackOff")),
            )
        ]
    elif fault == "missing":
        c.stored.pop("deployment")
    runtime, urls = c.k8s.observe_model_deployment(
        c.run.k8s_job_name,
        custody=c.target,
        control=dict(generation=0, command_id=None, replicas=2),
        deadline=time.monotonic() + 30,
    )
    assert runtime == expected and not c.mutations
    if expected != "ready":
        assert not urls


def test_original_deployment_observation_publishes_without_sql_during_native_reads(observed):
    from automl_api.models.enums import RunStatus

    c = observed
    row = read_deployments(c)
    assert row.runtime_state == "ready" and row.status == RunStatus.SUCCEEDED
    assert row.platform_endpoint and row.internal_endpoint is None and row.base_url is None
    assert c.native_reads and all(count == 0 for _, count in c.native_reads)
    assert not c.control_attempts and c.saved().tags["deployment_observation"]["version"] == 1


def test_new_stop_cannot_be_overwritten_by_an_older_ready_observation(observed):
    from automl_api.models.enums import RunStatus

    c = observed
    captured = inspect_observation(c, select_observation(c))
    assert captured.reads[0].runtime_state == "ready"
    control(c, "stop")
    before = list(c.native_reads)
    row = publish_observation(c, captured)
    assert row.status == RunStatus.CANCELLED and row.runtime_state == "stopping"
    assert row.platform_endpoint is None and not c.saved().tags.get("deployment_observation")
    assert before == c.native_reads


@pytest.mark.parametrize(
    "fault", ["run-source", "original-receipt", "creation-pending", "project-deletion"]
)
def test_changed_sql_authority_cannot_publish_an_old_observation(observed, fault):
    import copy

    from automl_api.models.enums import OutboxStatus, RunStatus
    from automl_api.models.projects import Project
    from automl_api.models.runs import ModelRun, RunArtifact
    from automl_api.models.workflows import OutboxEntry

    c = observed
    captured = inspect_observation(c, select_observation(c))
    with c.factory() as db, db.begin():
        if fault == "run-source":
            run = db.get(ModelRun, c.deployment_id)
            run.params = {**run.params, "unexpected-source-change": True}
        elif fault == "original-receipt":
            artifact = db.get(RunArtifact, c.dockerfile_id)
            metadata = copy.deepcopy(artifact.artifact_metadata)
            metadata["deployment_kubernetes"]["resources"]["deployment"]["receipt"]["uid"] = (
                "foreign-original-uid"
            )
            artifact.artifact_metadata = metadata
        elif fault == "creation-pending":
            db.get(OutboxEntry, c.entry_id).status = OutboxStatus.PENDING
        else:
            project = db.get(Project, c.project_id)
            project.settings = {**project.settings, "deletion_in_progress": True}
    row = publish_observation(c, captured)
    assert row.status == RunStatus.RUNNING and row.runtime_state == "unavailable"
    assert not c.saved().tags.get("deployment_observation") and row.platform_endpoint is None


@pytest.mark.parametrize("fault,code", [("inactive", 401), ("token", 401), ("membership", 403)])
def test_observation_publication_rechecks_current_reader_authority(observed, fault, code):
    from datetime import UTC, datetime, timedelta

    from automl_api.models.iam import User
    from automl_api.models.projects import ProjectMembership
    from fastapi import HTTPException
    from sqlalchemy import select

    c = observed
    captured = inspect_observation(c, select_observation(c))
    with c.factory() as db, db.begin():
        actor = db.get(User, c.user_id)
        if fault == "inactive":
            actor.is_active = False
        elif fault == "token":
            actor.token_version += 1
        else:
            db.scalars(
                select(ProjectMembership).where(
                    ProjectMembership.project_id == c.project_id,
                    ProjectMembership.user_id == c.user_id,
                )
            ).one().expires_at = datetime.now(UTC) - timedelta(seconds=1)
    with pytest.raises(HTTPException) as failure:
        publish_observation(c, captured)
    assert failure.value.status_code == code and not c.saved().tags.get("deployment_observation")


@pytest.mark.parametrize("newer", ["progressing", "unavailable"])
def test_newer_observation_revision_fences_an_older_ready_read(observed, newer):
    from automl_api.models.enums import RunStatus
    from kubernetes.client import ApiException

    c = observed
    first, second = select_observation(c), select_observation(c)
    first = inspect_observation(c, first)
    assert first.reads[0].runtime_state == "ready"
    original = c.kubernetes.apps.read_namespaced_deployment
    if newer == "unavailable":

        def unavailable(**kwargs):
            raise ApiException(status=503)

        c.kubernetes.apps.read_namespaced_deployment = unavailable
    else:
        c.native_resources["deployment"]["status"].update(
            availableReplicas=0, unavailableReplicas=1
        )
        c.native_resources["deployment"]["metadata"]["resourceVersion"] = "2"
    second = inspect_observation(c, second)
    c.kubernetes.apps.read_namespaced_deployment = original
    assert second.reads[0].runtime_state == newer
    assert publish_observation(c, second).runtime_state == newer
    row = publish_observation(c, first)
    assert row.runtime_state == "unavailable" and row.status == RunStatus.RUNNING
    assert c.saved().tags["deployment_observation"]["version"] == 1


def test_completed_restart_retains_original_control_custody_for_observation(observed):
    c = observed
    c.observation["pods"].clear()
    c.observation["replica_sets"][0]["spec"]["replicas"] = 0
    control(c, "stop")
    assert c.dispatch() == 1 and c.saved().tags["desired_state"] == "deployment_stopped"
    control(c, "start")
    assert c.dispatch() == 1 and c.saved().tags["desired_state"] == "deployment_active"
    c.observation = native_workload(c.kubernetes, c.native_resources)
    assert read_deployments(c).runtime_state == "ready"


def test_actual_deployment_route_commits_before_native_reads_and_preserves_pagination(
    observed, monkeypatch
):
    from automl_api.api.routes import operations as routes
    from automl_api.models.iam import User
    from automl_api.services import operations

    c = observed
    monkeypatch.setattr(operations, "KubernetesTrainingClient", lambda: c.kubernetes)
    with c.factory() as db:
        rows = routes.deployments(c.project_id, db, db.get(User, c.user_id), offset=0, limit=1)
        assert len(rows) == 1 and rows[0].runtime_state == "ready"
        count = len(c.native_reads)
        assert (
            routes.deployments(c.project_id, db, db.get(User, c.user_id), offset=1, limit=1) == []
        )
    assert len(c.native_reads) == count and all(held == 0 for _, held in c.native_reads)


def test_expired_native_observation_budget_performs_no_native_read(monkeypatch):
    import time

    from test_deployment_lifecycle_identity import identity_client

    c = identity_client()
    monkeypatch.setattr(
        c.k8s,
        "_lifecycle_plans",
        lambda *_: pytest.fail("Expired observer performed a native read"),
    )
    with pytest.raises(TimeoutError):
        c.k8s.observe_model_deployment(
            c.run.k8s_job_name,
            custody=c.target,
            control=dict(generation=0, command_id=None, replicas=2),
            deadline=time.monotonic() - 1,
        )
    assert not c.reads and not c.mutations
