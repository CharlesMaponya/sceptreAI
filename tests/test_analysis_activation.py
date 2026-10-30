"""Original server-normalized Batch Job specification guards activation and recovery."""

import copy
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.services.analysis_submission import _spec_matches
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from kubernetes import client
from kubernetes.client import ApiException


@pytest.fixture
def activation():
    original = dict(
        apiVersion="batch/v1",
        kind="Job",
        metadata=dict(
            name="original",
            namespace="owned",
            uid="original-uid",
            labels={"automl.platform/attempt-id": "original"},
        ),
        spec=dict(
            suspend=True,
            template=dict(
                spec=dict(
                    restartPolicy="Never", containers=[dict(name="owned", image="owned:original")]
                )
            ),
        ),
    )
    current = copy.deepcopy(original)
    current["metadata"]["resourceVersion"] = "90"
    active = copy.deepcopy(current)
    active["spec"]["suspend"] = False
    active["metadata"]["resourceVersion"] = "91"
    k8s = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    k8s.settings = SimpleNamespace(training_namespace="neighbor")
    k8s.batch = MagicMock()
    k8s.batch.api_client = client.ApiClient()
    k8s.batch.read_namespaced_job.return_value = k8s.batch.api_client._ApiClient__deserialize(
        current, "V1Job"
    )
    k8s.batch.patch_namespaced_job.return_value = k8s.batch.api_client._ApiClient__deserialize(
        active, "V1Job"
    )
    return k8s, original, current, active


def test_analysis_activation_tests_original_uid_fresh_version_and_full_frozen_spec(activation):
    k8s, original, current, active = activation
    assert k8s.activate_job(original) == active
    call = k8s.batch.patch_namespaced_job.call_args.kwargs
    assert call["namespace"] == "owned" and call["name"] == "original"
    assert call["_request_timeout"] == 5 and call["_content_type"] == "application/json-patch+json"
    assert call["body"] == [
        dict(op="test", path="/metadata/uid", value="original-uid"),
        dict(op="test", path="/metadata/resourceVersion", value="90"),
        dict(op="test", path="/spec", value=current["spec"]),
        dict(op="replace", path="/spec/suspend", value=False),
    ]
    k8s.batch.read_namespaced_job.return_value = k8s.batch.api_client._ApiClient__deserialize(
        active, "V1Job"
    )
    assert k8s.activate_job(original) == active
    assert k8s.batch.patch_namespaced_job.call_count == 1
    assert original["spec"]["suspend"] is True


@pytest.mark.parametrize(
    "field", ["uid", "namespace", "name", "labels", "version", "spec", "suspend"]
)
def test_analysis_activation_rejects_replacement_or_changed_frozen_spec(activation, field):
    k8s, original, current, _ = activation
    if field in {"uid", "namespace", "name"}:
        current["metadata"][field] = "replacement"
    elif field == "labels":
        current["metadata"]["labels"]["automl.platform/attempt-id"] = "neighbor"
    elif field == "version":
        current["metadata"].pop("resourceVersion")
    elif field == "spec":
        current["spec"]["template"]["spec"]["containers"][0]["image"] = "changed"
    else:
        current["spec"]["suspend"] = None
    k8s.batch.read_namespaced_job.return_value = k8s.batch.api_client._ApiClient__deserialize(
        current, "V1Job"
    )
    with pytest.raises(ValueError, match="activation target"):
        k8s.activate_job(original)
    k8s.batch.patch_namespaced_job.assert_not_called()


def test_analysis_activation_does_not_retry_failed_preconditions(activation):
    k8s, original, _, _ = activation
    k8s.batch.patch_namespaced_job.side_effect = ApiException(status=422)
    with pytest.raises(ApiException):
        k8s.activate_job(original)
    assert (
        k8s.batch.read_namespaced_job.call_count == k8s.batch.patch_namespaced_job.call_count == 1
    )


@pytest.mark.parametrize("bad", ["uid", "suspended"])
def test_analysis_activation_rejects_invalid_native_ack(activation, bad):
    k8s, original, _, active = activation
    if bad == "uid":
        active["metadata"]["uid"] = "replacement"
    else:
        active["spec"]["suspend"] = True
    k8s.batch.patch_namespaced_job.return_value = k8s.batch.api_client._ApiClient__deserialize(
        active, "V1Job"
    )
    with pytest.raises(ValueError):
        k8s.activate_job(original)


def test_api_default_equivalence_does_not_admit_injected_credentials_or_containers():
    wanted = dict(
        suspend=True,
        template=dict(
            metadata=dict(labels={"automl.platform/attempt-id": "attempt"}),
            spec=dict(
                containers=[
                    dict(
                        name="owned",
                        image="owned:v1",
                        env=[dict(name="EMPTY", value="")],
                        resources=dict(requests=dict(cpu="1.0"), limits=dict(memory="1024Mi")),
                    )
                ],
                restartPolicy="Never",
                automountServiceAccountToken=False,
            ),
        ),
    )
    actual = copy.deepcopy(wanted)
    container = actual["template"]["spec"]["containers"][0]
    container["env"][0].pop("value")
    container["resources"]["requests"]["cpu"] = "1"
    container["resources"]["limits"]["memory"] = "1Gi"
    container.update(terminationMessagePath="/dev/termination-log", terminationMessagePolicy="File")
    actual.update(
        parallelism=1,
        completions=1,
        completionMode="NonIndexed",
        manualSelector=False,
        podReplacementPolicy="TerminatingOrFailed",
        selector=dict(matchLabels={"batch.kubernetes.io/controller-uid": "original"}),
    )
    actual["template"]["metadata"]["labels"].update(
        {
            "controller-uid": "original",
            "batch.kubernetes.io/controller-uid": "original",
            "job-name": "owned",
            "batch.kubernetes.io/job-name": "owned",
        }
    )
    actual["template"]["spec"].update(
        dnsPolicy="ClusterFirst",
        schedulerName="default-scheduler",
        securityContext={},
        terminationGracePeriodSeconds=30,
    )
    assert _spec_matches(wanted, actual, "original", "owned")
    for kind in ("credential", "sidecar", "security", "extra_resource", "selector"):
        bad = copy.deepcopy(actual)
        pod = bad["template"]["spec"]
        if kind == "credential":
            pod["containers"][0]["env"][0]["valueFrom"] = dict(
                secretKeyRef=dict(name="neighbor", key="credential")
            )
        elif kind == "sidecar":
            pod["containers"].append(dict(name="injected", image="neighbor"))
        elif kind == "security":
            pod["securityContext"] = {"runAsUser": 0}
        elif kind == "extra_resource":
            pod["containers"][0]["resources"]["limits"]["nvidia.com/gpu"] = "1"
        else:
            bad["selector"]["matchLabels"]["batch.kubernetes.io/controller-uid"] = "replacement"
        assert not _spec_matches(wanted, bad, "original", "owned"), kind
