"""Placement is approved before remote creation and remains immutable during admission."""

import copy
import json
import subprocess

import pytest
from automl_api.models.datasets import DatasetProviderImport
from automl_api.models.workflows import WorkflowCommand, WorkflowEvent
from automl_api.services.provider_import_attempts import verification_attempt
from automl_api.services.provider_import_workload import DIGEST, SUBMISSION, _node_selector
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from sqlalchemy import select
from test_provider_import_attempts import pending_case as pending_case  # noqa: F401
from test_provider_import_jobs import job_case as job_case  # noqa: F401
from test_provider_import_jobs import tick
from test_provider_import_lifecycle import _provider_chart_command
from test_provider_import_managed_admission import admit
from test_provider_import_managed_admission import managed_case as managed_case  # noqa: F401
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]

INVALID = [
    "{",
    "null",
    "[]",
    '"pool"',
    '{"pool": true}',
    '{"pool": null}',
    '{"pool": ["worker"]}',
    '{"": "worker"}',
    '{"/pool": "worker"}',
    '{"bad/prefix/pool": "worker"}',
    '{"UPPER.example/pool": "worker"}',
    '{"pool": "_invalid"}',
    '{"pool": "has space"}',
    json.dumps({"a" * 64: "worker"}),
    json.dumps({"pool": "a" * 64}),
    json.dumps({"a" * 254 + "/pool": "worker"}),
    json.dumps({f"pool{i}": "worker" for i in range(33)}),
    json.dumps({"pool": "a" * 4096}),
]


@pytest.mark.parametrize(
    "raw",
    INVALID,
    ids=[
        "malformed",
        "null",
        "array",
        "scalar",
        "boolean-value",
        "null-value",
        "array-value",
        "empty-key",
        "empty-prefix",
        "multiple-slashes",
        "uppercase-prefix",
        "bad-value",
        "whitespace-value",
        "long-name",
        "long-value",
        "long-prefix",
        "too-many-labels",
        "byte-limit",
    ],
)
def test_invalid_placement_never_commits_a_claim_or_contacts_kubernetes(job_case, monkeypatch, raw):
    c = job_case
    monkeypatch.setenv("PROVIDER_IMPORT_NODE_SELECTOR", raw)
    tick(c)
    with c.factory() as db:
        assert verification_attempt(db, db.get(DatasetProviderImport, c.import_id)) is None
        assert db.get(WorkflowCommand, c.command_id).retry_count == 0
    assert not c.k8s.creates and not c.k8s.deletes


@pytest.mark.parametrize(
    "selector",
    [
        {},
        {"kubernetes.io/hostname": "approved-worker"},
        {"a" * 253 + "/" + "N" * 63: "v" * 63, "empty": ""},
    ],
)
def test_valid_kubernetes_label_boundaries_remain_supported(monkeypatch, selector):
    monkeypatch.setenv("PROVIDER_IMPORT_NODE_SELECTOR", json.dumps(selector))
    assert _node_selector() == selector


def test_committed_placement_cannot_change_with_live_operator_configuration(job_case, monkeypatch):
    c = job_case
    selector = {"kubernetes.io/hostname": "approved-worker"}
    monkeypatch.setenv("PROVIDER_IMPORT_NODE_SELECTOR", json.dumps(selector))
    tick(c)
    original = copy.deepcopy(next(iter(c.k8s.objects["job"].values())))
    assert original["spec"]["template"]["spec"]["nodeSelector"] == selector
    wanted = copy.deepcopy(original)
    wanted["metadata"].pop("uid")
    digest = wanted["metadata"].pop("annotations")[DIGEST]
    assert digest == canonical_request_hash(wanted)
    with c.factory() as db:
        attempt = verification_attempt(db, db.get(DatasetProviderImport, c.import_id))
        event = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == attempt.id, WorkflowEvent.event_key == SUBMISSION
            )
        )
        assert (
            event.payload["resources"]["job"]["spec"]["template"]["spec"]["nodeSelector"]
            == selector
        )
    monkeypatch.setenv("PROVIDER_IMPORT_NODE_SELECTOR", '{"pool":"different"}')
    tick(c)
    assert next(iter(c.k8s.objects["job"].values())) == original
    assert c.k8s.creates == ["service_account", "network", "job"]
    assert "PROVIDER_IMPORT_NODE_SELECTOR" not in {
        e["name"] for e in original["spec"]["template"]["spec"]["containers"][0]["env"]
    }


@pytest.mark.parametrize(
    "managed_case", [{"kubernetes.io/hostname": "approved-worker"}], indirect=True
)
@pytest.mark.parametrize("target", ["pod", "job"])
@pytest.mark.parametrize("selector", [None, {"pool": "unapproved"}])
def test_configured_placement_cannot_be_removed_or_relabelled_with_an_old_digest(
    managed_case, target, selector
):
    c = managed_case
    before = copy.deepcopy(c.k8s.objects), copy.deepcopy(c.pod_data)
    c.pod_data["spec"]["nodeName"] = "approved-worker"
    c.pod_data["spec"]["schedulerName"] = "default-scheduler"
    assert admit(c)["pod_uid"] == c.pod_data["metadata"]["uid"]
    spec = (
        c.pod_data["spec"]
        if target == "pod"
        else next(iter(c.k8s.objects["job"].values()))["spec"]["template"]["spec"]
    )
    if selector is None:
        spec.pop("nodeSelector")
    else:
        spec["nodeSelector"] = selector
    with pytest.raises(StaleFence):
        admit(c)
    c.k8s.objects, c.pod_data = before
    assert admit(c)["pod_uid"] == c.pod_data["metadata"]["uid"]


@pytest.mark.parametrize(
    "selector",
    [
        [],
        {"pool": True},
        {"bad/key/slash": "worker"},
        {"pool": "_invalid"},
        {"a" * 254 + "/pool": "worker"},
        {f"pool{i}": "worker" for i in range(33)},
        {("a" * 220) + f"/pool{i}": "v" * 63 for i in range(20)},
    ],
)
def test_chart_rejects_malformed_or_oversized_placement(selector):
    result = subprocess.run(
        [
            *_provider_chart_command(),
            "--set-json",
            "providerImports.nodeSelector=" + json.dumps(selector),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "nodeSelector" in result.stderr or "invalid propertyName" in result.stderr
