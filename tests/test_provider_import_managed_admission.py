"""Actual managed verifier admission validates native ownership and effective pod spec."""

import copy
import json
import uuid
from types import SimpleNamespace

import pytest
from automl_api.services.attempt_identity import admit_attempt
from automl_api.services.workflow_state import StaleFence
from kubernetes import client
from test_attempt_object_grants import issue, rows
from test_provider_import_attempts import bind_identity
from test_provider_import_attempts import pending_case as pending_case  # noqa: F401
from test_provider_import_jobs import job_case as job_case  # noqa: F401
from test_provider_import_jobs import tick
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.fixture
def managed_case(job_case, monkeypatch, request):
    c = job_case
    if hasattr(request, "param"):
        monkeypatch.setenv("PROVIDER_IMPORT_NODE_SELECTOR", json.dumps(request.param))
    tick(c)
    k8s = c.k8s
    bind_identity(c, monkeypatch)
    bound = c.k8s.core.read_namespaced_pod("verifier", "unused")
    job = next(iter(k8s.objects["job"].values()))
    data = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "verifier",
            "namespace": k8s.settings.training_namespace,
            "uid": bound.metadata.uid,
            "labels": job["metadata"]["labels"],
            "ownerReferences": [
                {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "name": job["metadata"]["name"],
                    "uid": job["metadata"]["uid"],
                    "controller": True,
                }
            ],
        },
        "spec": job["spec"]["template"]["spec"],
    }
    c.pod_data = copy.deepcopy(data)
    k8s.core.read_namespaced_pod = lambda *_args, **_kw: client.ApiClient().deserialize(
        SimpleNamespace(data=json.dumps(c.pod_data)), "V1Pod"
    )
    c.k8s = k8s
    return c


def admit(c):
    return admit_attempt(c.factory, c.attempt_id, c.token, c.k8s)


def test_managed_pod_admission_requires_the_observed_job_and_clean_actual_spec(managed_case):
    c = managed_case
    proof = admit(c)
    assert proof["pod_uid"] == c.pod_data["metadata"]["uid"]
    assert proof["project_id"] == str(c.project_id)


@pytest.mark.parametrize(
    "fault",
    [
        "extra_env",
        "default_token",
        "other_sa",
        "host_network",
        "sidecar",
        "restart",
        "hook",
        "job_uid",
        "job_parallelism",
        "sa_uid",
        "sa_annotation",
        "policy_uid",
        "policy_rule",
        "other_policy",
        "pod_selector",
        "job_selector",
        "job_node",
        "pod_scheduler",
        "job_scheduler",
    ],
)
def test_managed_admission_rejects_bypasses_even_with_unchanged_digest_annotation(
    managed_case, fault
):
    c = managed_case
    before = copy.deepcopy(c.k8s.objects), copy.deepcopy(c.pod_data)
    job = next(iter(c.k8s.objects["job"].values()))
    sa = next(iter(c.k8s.objects["service_account"].values()))
    network = next(iter(c.k8s.objects["network"].values()))
    pod = c.pod_data["spec"]
    if fault == "extra_env":
        pod["containers"][0]["env"].append({"name": "DATABASE_URL", "value": "forbidden"})
    elif fault == "default_token":
        pod["automountServiceAccountToken"] = True
    elif fault == "other_sa":
        pod["serviceAccountName"] = "privileged"
    elif fault == "host_network":
        pod["hostNetwork"] = True
    elif fault == "sidecar":
        pod["containers"].append(copy.deepcopy(pod["containers"][0]))
    elif fault == "restart":
        pod["restartPolicy"] = "Always"
    elif fault == "hook":
        pod["containers"][0]["lifecycle"] = {"postStart": {"exec": {"command": ["forbidden"]}}}
    elif fault == "job_uid":
        job["metadata"]["uid"] = str(uuid.uuid4())
        c.pod_data["metadata"]["ownerReferences"][0]["uid"] = job["metadata"]["uid"]
    elif fault == "job_parallelism":
        job["spec"]["parallelism"] = 2
    elif fault == "sa_uid":
        sa["metadata"]["uid"] = str(uuid.uuid4())
    elif fault == "sa_annotation":
        sa["metadata"]["annotations"]["cloud-role"] = "privileged"
    elif fault == "policy_uid":
        network["metadata"]["uid"] = str(uuid.uuid4())
    elif fault == "policy_rule":
        network["spec"]["egress"].append({})
    elif fault == "pod_selector":
        pod["nodeSelector"] = {"kubernetes.io/hostname": "unapproved"}
    elif fault == "job_selector":
        job["spec"]["template"]["spec"]["nodeSelector"] = {"pool": "unapproved"}
    elif fault == "job_node":
        job["spec"]["template"]["spec"]["nodeName"] = "bypass-scheduler"
    elif fault == "pod_scheduler":
        pod["schedulerName"] = "unapproved-scheduler"
    elif fault == "job_scheduler":
        job["spec"]["template"]["spec"]["schedulerName"] = "unapproved-scheduler"
    else:
        policy = copy.deepcopy(network)
        policy["metadata"]["name"] = "broadening"
        policy["spec"]["podSelector"] = {}
        policy["spec"]["egress"] = [{}]
        c.k8s.objects["network"]["broadening"] = policy
    with pytest.raises(StaleFence):
        admit(c)
    c.k8s.objects, c.pod_data = before
    assert admit(c)["pod_uid"] == c.pod_data["metadata"]["uid"]


def test_owned_source_namespace_is_rejected_before_the_native_signer(managed_case, monkeypatch):
    from automl_api.services.attempt_object_grants import ObjectGrantUnavailable, issue_input_grant

    c = managed_case

    def request():
        return issue_input_grant(
            c.factory,
            c.attempt_id,
            c.token,
            c.k8s,
            {"request_id": uuid.uuid4(), "role": "train", "object_index": 0},
            store_factory=lambda: c.capability_store,
        )

    with monkeypatch.context() as patch:
        patch.setattr(c.capability_store, "uri_for_key", lambda _key: "s3c://safe/roles")
        with pytest.raises(ObjectGrantUnavailable):
            request()
    c.signer.assert_not_called()
    assert request()["url"].startswith("https://objects.invalid/")
    c.signer.assert_called_once()


@pytest.mark.parametrize("access", ["admission", "grant"])
def test_extra_pod_label_cannot_hide_a_broadening_policy(managed_case, access):
    from automl_api.services.attempt_object_grants import issue_input_grant

    c = managed_case
    c.pod_data["metadata"]["labels"]["injected-access"] = "public"
    assert admit(c)["pod_uid"] == c.pod_data["metadata"]["uid"]
    policy = copy.deepcopy(next(iter(c.k8s.objects["network"].values())))
    policy["metadata"]["name"] = "broadening-extra-label"
    policy["spec"]["podSelector"] = {"matchLabels": {"injected-access": "public"}}
    policy["spec"]["egress"] = [{}]
    c.k8s.objects["network"][policy["metadata"]["name"]] = policy

    def request():
        if access == "admission":
            return admit(c)
        return issue_input_grant(
            c.factory,
            c.attempt_id,
            c.token,
            c.k8s,
            {"request_id": uuid.uuid4(), "role": "train", "object_index": 0},
            store_factory=lambda: c.capability_store,
        )

    with pytest.raises(StaleFence, match="Another network policy"):
        request()
    c.signer.assert_not_called()
    del c.k8s.objects["network"][policy["metadata"]["name"]]
    assert request()
    assert c.signer.call_count == (access == "grant")


@pytest.mark.parametrize(
    "label", ["automl.platform/provider-verification", "automl.platform/workflow-stage"]
)
@pytest.mark.parametrize("fault", ["missing", "changed"])
def test_approved_policy_must_select_the_live_verifier(managed_case, label, fault):
    c = managed_case
    assert admit(c)
    labels = c.pod_data["metadata"]["labels"]
    original = labels.pop(label)
    if fault == "changed":
        labels[label] = "unselected"
    with pytest.raises(StaleFence):
        admit(c)
    c.signer.assert_not_called()
    labels[label] = original
    assert admit(c)


@pytest.mark.parametrize("direction", ["ingress", "egress"])
@pytest.mark.parametrize(
    "key,operator,values,matches",
    [
        ("injected-access", "In", ["public"], True),
        ("injected-access", "In", ["private"], False),
        ("absent", "In", ["public"], False),
        ("injected-access", "NotIn", ["public"], False),
        ("injected-access", "NotIn", ["private"], True),
        ("absent", "NotIn", ["public"], True),
        ("injected-access", "Exists", [], True),
        ("absent", "Exists", [], False),
        ("injected-access", "DoesNotExist", [], False),
        ("absent", "DoesNotExist", [], True),
    ],
)
def test_effective_set_selectors_gate_credentials(
    managed_case, direction, key, operator, values, matches
):
    c = managed_case
    c.pod_data["metadata"]["labels"]["injected-access"] = "public"
    assert admit(c)
    policy = copy.deepcopy(next(iter(c.k8s.objects["network"].values())))
    policy["metadata"]["name"] = "another-policy"
    policy["spec"]["podSelector"] = {
        "matchExpressions": [{"key": key, "operator": operator, "values": values}]
    }
    policy["spec"].update(ingress=[], egress=[])
    policy["spec"][direction] = [{}]
    c.k8s.objects["network"]["another-policy"] = policy
    request = {"request_id": uuid.uuid4(), "role": "train", "object_index": 0}
    if matches:
        with pytest.raises(StaleFence, match="Another network policy"):
            issue(c, request)
        c.signer.assert_not_called()
        assert rows(c) == []
        del c.k8s.objects["network"]["another-policy"]
    assert issue(c, request)["url"].startswith("https://objects.invalid/")
    c.signer.assert_called_once()


@pytest.mark.parametrize("matches", [False, True])
def test_effective_network_check_reads_every_policy_page(managed_case, monkeypatch, matches):
    c = managed_case
    assert admit(c)
    owned = next(iter(c.k8s.objects["network"].values()))
    policy = copy.deepcopy(owned)
    policy["metadata"]["name"] = "second-page-policy"
    policy["spec"]["podSelector"] = {} if matches else {"matchLabels": {"absent": "label"}}
    policy["spec"]["egress"] = [{}]
    seen = []

    def list_policies(**kwargs):
        assert c.engine.pool.checkedout() == 0
        assert kwargs["limit"] == 100
        page = kwargs["_continue"]
        seen.append(page)
        if page is None:
            return SimpleNamespace(items=[owned], metadata=SimpleNamespace(_continue="next-page"))
        assert page == "next-page"
        return SimpleNamespace(items=[policy], metadata=SimpleNamespace(_continue=None))

    monkeypatch.setattr(c.k8s.networking, "list_namespaced_network_policy", list_policies)
    if matches:
        with pytest.raises(StaleFence, match="Another network policy"):
            admit(c)
    else:
        assert admit(c)
    assert seen == [None, "next-page"]
    c.signer.assert_not_called()


def test_unsupported_policy_selector_fails_closed(managed_case):
    c = managed_case
    assert admit(c)
    policy = copy.deepcopy(next(iter(c.k8s.objects["network"].values())))
    policy["metadata"]["name"] = "unsupported-selector"
    policy["spec"]["podSelector"] = {
        "matchExpressions": [{"key": "unknown", "operator": "unsupported"}]
    }
    c.k8s.objects["network"]["unsupported-selector"] = policy
    with pytest.raises(StaleFence, match="selector is unsupported"):
        admit(c)
    c.signer.assert_not_called()
    del c.k8s.objects["network"]["unsupported-selector"]
    assert admit(c)


@pytest.mark.parametrize("matches", [False, True])
def test_policy_selector_requirements_are_combined_with_and(managed_case, matches):
    c = managed_case
    c.pod_data["metadata"]["labels"]["injected-access"] = "public"
    assert admit(c)
    policy = copy.deepcopy(next(iter(c.k8s.objects["network"].values())))
    policy["metadata"]["name"] = "compound-selector"
    policy["spec"]["podSelector"] = {
        "matchLabels": {"injected-access": "public"},
        "matchExpressions": [
            {"key": "absent", "operator": "DoesNotExist" if matches else "Exists"}
        ],
    }
    policy["spec"]["egress"] = [{}]
    c.k8s.objects["network"]["compound-selector"] = policy
    if matches:
        with pytest.raises(StaleFence, match="Another network policy"):
            admit(c)
    else:
        assert admit(c)
    c.signer.assert_not_called()


def test_additional_default_deny_does_not_broaden_access(managed_case):
    c = managed_case
    assert admit(c)
    policy = copy.deepcopy(next(iter(c.k8s.objects["network"].values())))
    policy["metadata"]["name"] = "default-deny"
    policy["spec"].update(podSelector={}, ingress=[], egress=[])
    c.k8s.objects["network"]["default-deny"] = policy
    assert admit(c)
    c.signer.assert_not_called()
