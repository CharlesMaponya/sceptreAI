"""Native analysis creates cannot reconstruct identity by reading a conflicting name."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.workflow_state import StaleFence
from kubernetes import client
from kubernetes.client import ApiException


@pytest.fixture(params=["validation", "explainability", "drift"])
def creation(request):
    k8s = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    k8s.settings = SimpleNamespace(training_namespace="neighbor")
    k8s.batch = MagicMock()
    labels = {
        "automl.platform/project-id": "project",
        "automl.platform/run-id": "run",
        "automl.platform/attempt-id": "attempt",
        "automl.platform/generation": "1",
        "automl.platform/workflow-stage": "training_run",
        "automl.platform/analysis-kind": request.param,
    }
    manifest = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "original", "namespace": "owned-analysis", "labels": labels},
        "spec": {"suspend": True},
    }
    reply = client.V1Job(
        metadata=client.V1ObjectMeta(
            name="original", namespace="owned-analysis", uid="original-uid", labels=deepcopy(labels)
        )
    )
    k8s.batch.create_namespaced_job.return_value = reply
    return k8s, manifest, reply


def test_original_create_reply_preserves_native_uid_without_lookup(creation):
    k8s, manifest, reply = creation
    before = deepcopy(manifest)
    assert k8s.create_job(manifest) is reply
    assert manifest == before
    k8s.batch.create_namespaced_job.assert_called_once_with(
        namespace="owned-analysis", body=manifest, _request_timeout=5
    )
    k8s.batch.read_namespaced_job.assert_not_called()


@pytest.mark.parametrize("matching", [True, False])
def test_create_conflict_never_adopts_matching_or_replacement_job(creation, matching):
    k8s, manifest, reply = creation
    conflict = ApiException(status=409)
    k8s.batch.create_namespaced_job.side_effect = conflict
    reply.metadata.uid = "replacement-uid"
    if not matching:
        reply.metadata.labels["automl.platform/attempt-id"] = "neighbor"
    k8s.batch.read_namespaced_job.return_value = reply
    with pytest.raises(ApiException) as caught:
        k8s.create_job(manifest)
    assert caught.value is conflict
    assert k8s.batch.create_namespaced_job.call_count == 1
    k8s.batch.read_namespaced_job.assert_not_called()


@pytest.mark.parametrize(
    "bad", ["uid", "blank_uid", "name", "namespace", "labels", "missing_label", "scope"]
)
def test_malformed_original_create_reply_cannot_publish_identity(creation, bad):
    k8s, manifest, reply = creation
    if bad == "uid":
        reply.metadata.uid = None
    elif bad == "blank_uid":
        reply.metadata.uid = " \t"
    elif bad in {"name", "namespace"}:
        setattr(reply.metadata, bad, "neighbor")
    elif bad == "labels":
        reply.metadata.labels = None
    elif bad == "missing_label":
        del reply.metadata.labels["automl.platform/generation"]
    else:
        reply.metadata.labels["automl.platform/attempt-id"] = "neighbor"
    with pytest.raises(StaleFence, match="original.*identity"):
        k8s.create_job(manifest)
    assert k8s.batch.create_namespaced_job.call_count == 1
    k8s.batch.read_namespaced_job.assert_not_called()


@pytest.mark.parametrize("field", ["project-id", "run-id", "generation", "workflow-stage"])
def test_incomplete_bound_request_fails_before_create(creation, field):
    k8s, manifest, _ = creation
    del manifest["metadata"]["labels"]["automl.platform/" + field]
    with pytest.raises(StaleFence, match="bound.*scope"):
        k8s.create_job(manifest)
    k8s.batch.create_namespaced_job.assert_not_called()


@pytest.mark.parametrize(
    "error", [ApiException(status=429), ApiException(status=503), OSError("lost reply")]
)
def test_unknown_create_outcomes_propagate_without_retry_or_lookup(creation, error):
    k8s, manifest, _ = creation
    k8s.batch.create_namespaced_job.side_effect = error
    with pytest.raises(type(error)) as caught:
        k8s.create_job(manifest)
    assert caught.value is error
    assert k8s.batch.create_namespaced_job.call_count == 1
    k8s.batch.read_namespaced_job.assert_not_called()


def test_unbound_legacy_conflict_is_also_held_without_lookup():
    k8s = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    k8s.settings = SimpleNamespace(training_namespace="owned")
    k8s.batch = MagicMock()
    k8s.batch.create_namespaced_job.side_effect = ApiException(status=409)
    with pytest.raises(ApiException):
        k8s.create_job({"metadata": {"name": "legacy"}})
    k8s.batch.read_namespaced_job.assert_not_called()
