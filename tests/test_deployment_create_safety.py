"""Inference creation cannot adopt or erase resources merely by matching names."""

import copy
import uuid
from types import SimpleNamespace

import pytest
from automl_api.core.config import Settings
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from kubernetes.client import ApiException


@pytest.fixture
def creation():
    k8s = object.__new__(KubernetesTrainingClient)
    k8s.settings = Settings(
        inference_ingress_enabled=True, inference_ingress_host_template="{name}.invalid"
    )
    manifests = k8s.build_model_deployment_manifest(
        identity_ca_digest="c" * 64,
        identity_control_base_url="https://control.invalid/api/v1/internal/deployments",
        deployment_id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        project_name="Owned",
        environment="test",
        model_name="Ridge",
        model_uri="s3://owned/model.joblib",
        model_digest="a" * 64,
        image="registry.invalid/never-pull:fixture",
        replicas=0,
        cpu_request="1000m",
        memory_request="1024Mi",
    )
    manifests = {kind: copy.deepcopy(resource) for kind, resource in manifests.items()}
    c = SimpleNamespace(
        k8s=k8s, manifests=manifests, stored={}, creates=[], reads=[], deleted=[], fail=None
    )

    def created(kind, **kwargs):
        c.creates.append(kind)
        if kind in c.stored:
            raise ApiException(status=409)
        if c.fail == kind:
            raise ApiException(status=500)
        resource = copy.deepcopy(kwargs["body"])
        resource["metadata"]["uid"] = "owned-" + kind
        c.stored[kind] = resource
        return resource

    def read(kind, **kwargs):
        c.reads.append(kind)
        if kind not in c.stored:
            raise ApiException(status=404)
        return copy.deepcopy(c.stored[kind])

    def deleted(kind, **kwargs):
        c.deleted.append(kind)
        c.stored.pop(kind, None)

    k8s.apps = SimpleNamespace(
        create_namespaced_deployment=lambda **kw: created("deployment", **kw),
        read_namespaced_deployment=lambda **kw: read("deployment", **kw),
        delete_namespaced_deployment=lambda **kw: deleted("deployment", **kw),
    )
    k8s.core = SimpleNamespace(
        create_namespaced_service=lambda **kw: created("service", **kw),
        read_namespaced_service=lambda **kw: read("service", **kw),
        delete_namespaced_service=lambda **kw: deleted("service", **kw),
    )
    k8s.networking = SimpleNamespace(
        create_namespaced_ingress=lambda **kw: created("ingress", **kw),
        read_namespaced_ingress=lambda **kw: read("ingress", **kw),
        delete_namespaced_ingress=lambda **kw: deleted("ingress", **kw),
    )

    def existing(kind):
        value = copy.deepcopy(manifests[kind])
        value["metadata"]["uid"] = "owned-" + kind
        c.stored[kind] = value
        return value

    c.existing = existing
    return c


def test_foreign_conflicted_deployment_is_not_adopted(creation):
    c = creation
    foreign = c.existing("deployment")
    foreign["metadata"]["labels"]["automl.platform/project-id"] = str(uuid.uuid4())
    with pytest.raises(ValueError, match="ownership|spec"):
        c.k8s.create_model_deployment(c.manifests)
    assert c.creates == ["deployment"] and not c.deleted
    assert c.stored["deployment"] == foreign


def test_later_failure_never_deletes_existing_resources_by_name(creation):
    c = creation
    existing = copy.deepcopy(c.existing("deployment"))
    c.fail = "service"
    with pytest.raises(ApiException) as failure:
        c.k8s.create_model_deployment(c.manifests)
    assert failure.value.status == 500
    assert not c.deleted
    assert c.stored["deployment"] == existing


def test_matching_existing_resources_and_api_defaults_are_reusable(creation):
    c = creation
    original = copy.deepcopy(c.manifests)
    for kind in c.manifests:
        resource = c.existing(kind)
        resource["metadata"]["resourceVersion"] = "server-version"
    pod = c.stored["deployment"]["spec"]["template"]["spec"]
    pod["containers"][0]["resources"]["requests"] = {"cpu": "1", "memory": "1Gi"}
    pod["containers"][0]["resources"]["limits"] = {"cpu": "1", "memory": "1Gi"}
    c.stored["service"]["spec"]["clusterIP"] = "10.0.0.9"
    c.k8s.create_model_deployment(c.manifests)
    assert c.reads == ["deployment", "service", "ingress"]
    assert not c.deleted and c.manifests == original


@pytest.mark.parametrize("kind", ["deployment", "service", "ingress"])
@pytest.mark.parametrize("fault", ["owner", "name", "namespace", "uid", "deleting", "spec", "list"])
def test_conflict_requires_original_ownership_target_and_requested_spec(creation, kind, fault):
    c = creation
    existing = c.existing(kind)
    if fault == "owner":
        existing["metadata"]["labels"]["automl.platform/deployment-id"] = str(uuid.uuid4())
    elif fault == "name":
        existing["metadata"]["name"] = "foreign-name"
    elif fault == "namespace":
        existing["metadata"]["namespace"] = "foreign-namespace"
    elif fault == "uid":
        existing["metadata"].pop("uid")
    elif fault == "deleting":
        existing["metadata"]["deletionTimestamp"] = "2026-10-25T00:00:00Z"
    elif fault == "spec":
        if kind == "deployment":
            existing["spec"]["replicas"] = 1
        elif kind == "service":
            existing["spec"]["ports"][0]["targetPort"] = "foreign"
        else:
            existing["spec"]["rules"][0]["host"] = "foreign.invalid"
    else:
        if kind == "deployment":
            existing["spec"]["template"]["spec"]["containers"].append({"name": "foreign"})
        elif kind == "service":
            existing["spec"]["ports"].append({"name": "foreign", "port": 8081})
        else:
            existing["spec"]["rules"].append({"host": "foreign.invalid"})
    before = copy.deepcopy(existing)
    with pytest.raises(ValueError, match="ownership|spec"):
        c.k8s.create_model_deployment(c.manifests)
    assert not c.deleted and c.stored[kind] == before


@pytest.mark.parametrize(
    "fault",
    [
        "namespace",
        "owner",
        "malformed_owner",
        "kind",
        "name",
        "spec",
        "template",
        "selector",
        "backend",
    ],
)
def test_all_target_preflight_finishes_before_any_post(creation, fault):
    c = creation
    if fault == "namespace":
        c.manifests["service"]["metadata"]["namespace"] = "foreign"
    elif fault == "owner":
        c.manifests["ingress"]["metadata"]["labels"]["automl.platform/project-id"] = str(
            uuid.uuid4()
        )
    elif fault == "malformed_owner":
        c.manifests["service"]["metadata"]["labels"]["automl.platform/project-id"] = "invalid"
    elif fault == "kind":
        c.manifests["service"]["kind"] = "Secret"
    elif fault == "name":
        c.manifests["service"]["metadata"]["name"] = ""
    elif fault == "spec":
        c.manifests["service"]["spec"] = {}
    elif fault == "template":
        c.manifests["deployment"]["spec"]["template"]["metadata"]["labels"][
            "automl.platform/project-id"
        ] = str(uuid.uuid4())
    elif fault == "selector":
        c.manifests["service"]["spec"]["selector"]["automl.platform/deployment-id"] = str(
            uuid.uuid4()
        )
    else:
        c.manifests["ingress"]["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"][
            "name"
        ] = "foreign"
    with pytest.raises(ValueError):
        c.k8s.create_model_deployment(c.manifests)
    assert not c.creates and not c.reads and not c.deleted


@pytest.mark.parametrize("kind", ["service", "ingress"])
def test_partial_failure_preserves_resources_for_same_manifest_retry(creation, kind):
    c = creation
    c.fail = kind
    with pytest.raises(ApiException):
        c.k8s.create_model_deployment(c.manifests)
    original = {key: value["metadata"]["uid"] for key, value in c.stored.items()}
    assert not c.deleted
    c.fail = None
    c.k8s.create_model_deployment(c.manifests)
    assert set(c.stored) == set(c.manifests)
    assert all(c.stored[key]["metadata"]["uid"] == value for key, value in original.items())
    assert not c.deleted


def test_typed_kubernetes_conflict_response_uses_public_serialization(creation):
    from kubernetes.client import V1Deployment, V1ObjectMeta

    c = creation
    resource = c.existing("deployment")
    c.k8s.apps.read_namespaced_deployment = lambda **_: V1Deployment(
        api_version=resource["apiVersion"],
        kind=resource["kind"],
        metadata=V1ObjectMeta(**resource["metadata"]),
        spec=resource["spec"],
    )
    c.k8s.create_model_deployment(c.manifests)
    assert not c.deleted and len(c.stored) == 3


@pytest.mark.parametrize("status", [403, 404, 500])
def test_conflict_read_errors_propagate_without_cleanup(creation, status):
    c = creation
    c.existing("deployment")

    def failed(**_):
        raise ApiException(status=status)

    c.k8s.apps.read_namespaced_deployment = failed
    with pytest.raises(ApiException) as error:
        c.k8s.create_model_deployment(c.manifests)
    assert error.value.status == status and not c.deleted
