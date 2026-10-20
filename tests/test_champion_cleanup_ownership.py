"""Cleanup must never delete a same-name resource belonging to another owner."""

import uuid
from copy import deepcopy
from unittest.mock import Mock

import pytest
from automl_api.services import refit_jobs
from automl_api.services.champion_workloads import delete_owned_resource
from kubernetes.client import ApiException
from test_champion_refit_jobs import FakeKubernetes


def cleanup_case(role):
    k8s = FakeKubernetes()
    attempt_id = uuid.uuid4()
    label = f"automl.platform/{role}-attempt"
    labels = {label: str(attempt_id)}
    if role == "evaluation":
        labels.update(
            {
                "automl.platform/evaluation-scope": str(uuid.uuid4()),
                "automl.platform/project-id": str(uuid.uuid4()),
            }
        )
    name = f"{role}-{attempt_id.hex}"
    for kind in ("job", "secret", "network_policy"):
        k8s.create(
            kind,
            "qa-refit",
            {
                "metadata": {
                    "name": name,
                    "namespace": "qa-refit",
                    "uid": str(uuid.uuid4()),
                    "labels": deepcopy(labels),
                }
            },
        )
    desired = {"job": {"metadata": {"name": name, "namespace": "qa-refit", "labels": labels}}}
    return k8s, name, labels, desired


@pytest.mark.parametrize("role", ["refit", "evaluation"])
@pytest.mark.parametrize("kind", ["job", "secret", "network_policy"])
def test_cleanup_preserves_same_name_foreign_resource(role, kind):
    k8s, name, labels, desired = cleanup_case(role)
    original = deepcopy(k8s.objects)
    assert refit_jobs._cleanup(k8s, desired, labels=labels) is False
    assert refit_jobs._cleanup(k8s, desired, labels=labels) is True
    assert not any(k8s.objects.values())
    k8s.objects = original
    if kind != "job":
        k8s.objects["job"].clear()
    k8s.objects[kind][name]["metadata"]["labels"] = {key: "another-owner" for key in labels}
    foreign = deepcopy(k8s.objects[kind][name])
    complete = refit_jobs._cleanup(k8s, desired, labels=labels)
    assert k8s.objects[kind].get(name) == foreign
    assert complete is False
    if kind == "secret":
        assert name in k8s.objects["network_policy"]


@pytest.mark.parametrize("kind", ["job", "secret", "network_policy"])
def test_deletion_is_bound_to_the_observed_uid_and_confirmed_absence(kind):
    k8s, name, labels, _ = cleanup_case("refit")
    api = {"job": k8s.batch, "secret": k8s.core, "network_policy": k8s.networking}[kind]
    read = getattr(api, f"read_namespaced_{kind}")
    delete = Mock(wraps=getattr(api, f"delete_namespaced_{kind}"))
    uid = k8s.objects[kind][name]["metadata"]["uid"]
    metadata = {"name": name, "namespace": "qa-refit", "labels": labels}
    assert delete_owned_resource(read, delete, metadata, foreground=kind == "job") is (
        kind != "job"
    )
    kwargs = delete.call_args.kwargs
    assert kwargs["body"]["preconditions"] == {"uid": uid}
    assert kwargs["name"] == name and kwargs["namespace"] == "qa-refit"
    assert kwargs["_request_timeout"] == 5
    assert (kwargs["body"].get("propagationPolicy") == "Foreground") == (kind == "job")
    assert delete_owned_resource(read, delete, metadata, foreground=kind == "job") is True
    delete.assert_called_once()


@pytest.mark.parametrize(
    "fault",
    [
        "name",
        "namespace",
        "missing_uid",
        "empty_uid",
        "typed_uid",
        "labels",
        "missing_labels",
        "listed_uid",
    ],
)
def test_ambiguous_resource_identity_never_reaches_delete(fault):
    k8s, name, labels, _ = cleanup_case("refit")
    metadata = {"name": name, "namespace": "qa-refit", "labels": labels}
    value = k8s.core.read_namespaced_secret(name, "qa-refit")
    if fault in {"name", "namespace"}:
        setattr(value.metadata, fault, "other")
    elif fault == "missing_uid":
        del value.metadata.uid
    elif fault == "empty_uid":
        value.metadata.uid = ""
    elif fault == "typed_uid":
        value.metadata.uid = True
    elif fault == "labels":
        value.metadata.labels = {next(iter(labels)): "foreign"}
    elif fault == "missing_labels":
        value.metadata.labels = None
    else:
        metadata["uid"] = "previous-identity"
    delete = Mock()
    assert delete_owned_resource(Mock(return_value=value), delete, metadata) is False
    delete.assert_not_called()
    assert name in k8s.objects["secret"]


@pytest.mark.parametrize("labels", [{}, {"owner": ""}, {"owner": None}])
def test_missing_expected_ownership_does_not_guess_at_a_resource(labels):
    read, delete = Mock(), Mock()
    assert (
        delete_owned_resource(
            read, delete, {"name": "unknown", "namespace": "qa-refit", "labels": labels}
        )
        is False
    )
    read.assert_not_called()
    delete.assert_not_called()


@pytest.mark.parametrize("kind", ["job", "secret", "network_policy"])
def test_uid_precondition_preserves_replacement_between_read_and_delete(kind):
    k8s, name, labels, _ = cleanup_case("evaluation")
    api = {"job": k8s.batch, "secret": k8s.core, "network_policy": k8s.networking}[kind]
    read = getattr(api, f"read_namespaced_{kind}")
    remove = getattr(api, f"delete_namespaced_{kind}")
    old_uid = k8s.objects[kind][name]["metadata"]["uid"]
    replacement = deepcopy(k8s.objects[kind][name])
    replacement["metadata"].update(uid=str(uuid.uuid4()), labels={"owner": "foreign"})

    def delete(**kwargs):
        assert kwargs["body"]["preconditions"] == {"uid": old_uid}
        k8s.objects[kind][name] = deepcopy(replacement)
        return remove(**kwargs)

    assert (
        delete_owned_resource(
            read,
            delete,
            {"name": name, "namespace": "qa-refit", "labels": labels},
            foreground=kind == "job",
        )
        is False
    )
    assert k8s.objects[kind][name] == replacement


@pytest.mark.parametrize("status", [403, 500])
@pytest.mark.parametrize("operation", ["read", "delete", "confirmation"])
def test_cleanup_errors_do_not_report_absence(status, operation):
    k8s, name, labels, _ = cleanup_case("refit")
    value = k8s.core.read_namespaced_secret(name, "qa-refit")
    read = Mock(return_value=value)
    delete = Mock()
    error = ApiException(status=status)
    if operation == "read":
        read.side_effect = error
    elif operation == "delete":
        delete.side_effect = error
    else:
        read.side_effect = [value, error]
    with pytest.raises(ApiException) as caught:
        delete_owned_resource(
            read, delete, {"name": name, "namespace": "qa-refit", "labels": labels}
        )
    assert caught.value.status == status
    if operation == "read":
        delete.assert_not_called()
    assert name in k8s.objects["secret"]


@pytest.mark.parametrize("outcome", ["finalizer", "gone", "replacement"])
def test_delete_acknowledgement_is_not_an_absence_proof(outcome):
    k8s, name, labels, _ = cleanup_case("refit")
    read = k8s.core.read_namespaced_secret

    def delete(**kwargs):
        assert kwargs["body"]["preconditions"]["uid"]
        if outcome != "finalizer":
            del k8s.objects["secret"][name]
            if outcome == "replacement":
                k8s.objects["secret"][name] = {
                    "metadata": {
                        "name": name,
                        "namespace": "qa-refit",
                        "uid": str(uuid.uuid4()),
                        "labels": {"owner": "foreign"},
                    }
                }
            raise ApiException(status=404)

    assert delete_owned_resource(
        read, delete, {"name": name, "namespace": "qa-refit", "labels": labels}
    ) is (outcome == "gone")
    assert (name in k8s.objects["secret"]) == (outcome != "gone")


@pytest.mark.parametrize("role", ["refit", "evaluation"])
def test_foreground_finalizer_keeps_credentials_and_policy(role, monkeypatch):
    k8s, name, labels, desired = cleanup_case(role)
    held = Mock()
    monkeypatch.setattr(k8s.batch, "delete_namespaced_job", held)
    assert refit_jobs._cleanup(k8s, desired, labels=labels) is False
    assert name in k8s.objects["secret"] and name in k8s.objects["network_policy"]
    del k8s.objects["job"][name]
    assert refit_jobs._cleanup(k8s, desired, labels=labels) is True
    assert not any(k8s.objects.values())


@pytest.mark.parametrize(
    "fault", ["missing_label", "invalid_label", "wrong_name", "manifest_owner"]
)
def test_cleanup_rejects_an_unbound_manifest_before_kubernetes(fault):
    k8s, _name, labels, desired = cleanup_case("refit")
    if fault == "missing_label":
        labels.clear()
    elif fault == "invalid_label":
        labels[next(iter(labels))] = "not-a-workflow-id"
    elif fault == "manifest_owner":
        desired["job"]["metadata"]["labels"] = {refit_jobs.LABEL: str(uuid.uuid4())}
    else:
        desired["job"]["metadata"]["name"] = "foreign-name"
    read = Mock()
    k8s.batch.read_namespaced_job = read
    assert refit_jobs._cleanup(k8s, desired, labels=labels) is False
    read.assert_not_called()
