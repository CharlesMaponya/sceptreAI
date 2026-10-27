"""UID and spec guarded Ray activation using the installed native SDK interface."""

import copy
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.services import reconciler
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from kubernetes.client import ApiException
from test_phase1_reconciler import _attempt, _run


@pytest.fixture
def activation():
    run = _run()
    attempt = _attempt(run)
    manifest = reconciler.build_ray_job_manifest(run, attempt, name="original-activation")
    manifest["spec"]["suspend"] = True
    resource = copy.deepcopy(manifest)
    resource["metadata"].update(uid="original-uid", resourceVersion="1")
    client = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    client._configured = True
    client.settings = SimpleNamespace(training_namespace=manifest["metadata"]["namespace"])
    client.custom = MagicMock()
    client.custom.get_namespaced_custom_object.return_value = resource
    active = copy.deepcopy(resource)
    active["spec"]["suspend"] = False
    active["metadata"]["resourceVersion"] = "2"
    client.custom.patch_namespaced_custom_object.return_value = active
    return client, manifest, resource, active


def test_native_activation_tests_uid_resource_version_and_entire_original_spec(activation):
    client, manifest, resource, active = activation
    assert client.activate_ray_job(manifest, "original-uid") == active
    args = client.custom.patch_namespaced_custom_object.call_args.kwargs
    assert args["_content_type"] == "application/json-patch+json"
    assert args["body"] == [
        dict(op="test", path="/metadata/uid", value="original-uid"),
        dict(op="test", path="/metadata/resourceVersion", value="1"),
        dict(op="test", path="/spec", value=resource["spec"]),
        dict(op="replace", path="/spec/suspend", value=False),
    ]
    client.custom.get_namespaced_custom_object.return_value = active
    assert client.activate_ray_job(manifest, "original-uid") == active
    assert client.custom.patch_namespaced_custom_object.call_count == 1


@pytest.mark.parametrize(
    "field", ["uid", "namespace", "name", "version", "labels", "spec", "suspend"]
)
def test_native_activation_refuses_replacement_or_changed_spec_before_patch(activation, field):
    client, manifest, resource, _ = activation
    if field in {"uid", "namespace", "name"}:
        resource["metadata"][field] = "foreign"
    elif field == "version":
        resource["metadata"]["resourceVersion"] = None
    elif field == "labels":
        resource["metadata"]["labels"]["automl.platform/attempt-id"] = "foreign"
    elif field == "spec":
        resource["spec"]["entrypoint"] = "foreign command"
    else:
        resource["spec"]["suspend"] = "false"
    with pytest.raises(ValueError, match="activation target"):
        client.activate_ray_job(manifest, "original-uid")
    client.custom.patch_namespaced_custom_object.assert_not_called()


def test_native_activation_does_not_retry_a_failed_precondition(activation):
    client, manifest, _, _ = activation
    client.custom.patch_namespaced_custom_object.side_effect = ApiException(status=422)
    with pytest.raises(ApiException) as exc:
        client.activate_ray_job(manifest, "original-uid")
    assert exc.value.status == 422
    assert client.custom.get_namespaced_custom_object.call_count == 1
    assert client.custom.patch_namespaced_custom_object.call_count == 1


@pytest.mark.parametrize("bad", ["uid", "suspended"])
def test_native_activation_cannot_accept_a_changed_reply(activation, bad):
    client, manifest, _, active = activation
    if bad == "uid":
        active["metadata"]["uid"] = "replacement"
    else:
        active["spec"]["suspend"] = True
    with pytest.raises(ValueError):
        client.activate_ray_job(manifest, "original-uid")
    assert client.custom.patch_namespaced_custom_object.call_count == 1
