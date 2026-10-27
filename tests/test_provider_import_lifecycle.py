"""Operator cancellation, deletion ownership and public configuration wiring."""

import copy
from unittest.mock import MagicMock

import pytest
import yaml
from automl_api.models.datasets import DatasetProviderImport
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import (
    DeletionStage,
    DeletionTombstone,
    OutboxEntry,
    WorkflowCommand,
)
from automl_api.services import deletion
from automl_api.services.provider_import import assert_provider_cleanup_safe, cancel_provider_import
from automl_api.services.workflow_state import StaleFence
from fastapi import HTTPException
from sqlalchemy import delete, select
from test_project_management_integration import reconcile_deletion_stage
from test_provider_import_consumers import source_case as source_case  # noqa: F401
from test_provider_import_registration import begin, run
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


def test_owned_namespace_is_rejected_before_any_copy_read(registration_case):
    c = registration_case
    from automl_api.services.provider_distribution import ProviderDistribution
    from test_provider_copy_cli import signed

    value = c.value.model_dump(mode="json")
    for item in value["safe_data"]["objects"]:
        old = item["uri"]
        item["uri"] = f"s3c://safe/projects/{c.project_id}/" + old.split("://safe/")[1]
        c.store.objects[(item["uri"], item["version"])] = c.store.objects[(old, item["version"])]
    c.value = ProviderDistribution.model_validate(value)
    c.receipt = signed(c.value, c.key)
    version_id, _, _ = begin(c)
    c.store.uri_for_key = lambda key: f"s3://safe/{key}"
    with pytest.raises(ValueError, match="outside application-owned"):
        run(c, version_id)
    assert not c.store.calls and not c.store.writes


def test_later_storage_configuration_change_cannot_delete_protected_source(source_case):
    c = source_case
    before = copy.deepcopy(c.store.objects)
    c.store.uri_for_key = lambda key: f"s3://safe/{key}"
    with c.factory() as db:
        assert_provider_cleanup_safe(db, c.store, "unrelated")
        with pytest.raises(ValueError, match="Protected provider copies"):
            assert_provider_cleanup_safe(db, c.store, "roles")
    assert c.store.objects == before


def test_pending_import_blocks_deletion_until_operator_cancellation(registration_case):
    c = registration_case
    version_id, _, _ = begin(c)
    with c.factory() as db, db.begin():
        with pytest.raises(HTTPException, match="provider import verification"):
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
        cancel_provider_import(db, c.actor_id, version_id)
    with c.factory() as db, db.begin():
        result = deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
        assert result["status"] == "pending"
    # This test owns its synthetic deletion records; retain/inspect before cleanup.
    with c.factory() as db, db.begin():
        db.execute(delete(DeletionStage).where(DeletionStage.project_id == c.project_id))
        db.execute(delete(DeletionTombstone).where(DeletionTombstone.project_id == c.project_id))


def test_running_import_cannot_be_cancelled_by_ignoring_its_lease(registration_case):
    from automl_api.models.enums import CommandStatus

    c = registration_case
    version_id, _, command_id = begin(c)
    with c.factory() as db, db.begin():
        command = db.get(WorkflowCommand, command_id)
        command.status = CommandStatus.RUNNING
        with pytest.raises(StaleFence, match="importer to stop"):
            cancel_provider_import(db, c.actor_id, version_id)


def test_project_deletion_removes_only_owned_descriptor_and_metadata(source_case, monkeypatch):
    c = source_case
    before = copy.deepcopy(c.store.objects)
    from automl_api.services.provider_import import descriptor_bytes

    legacy_uri = c.store.uri_for_key(f"projects/{c.project_id}/legacy-provider-descriptor.json")
    c.store.descriptors[legacy_uri] = descriptor_bytes(c.value)
    assert c.store.descriptors
    prefixes = []

    def remove(prefix):
        prefixes.append(prefix)
        uri = c.store.uri_for_key(prefix).rstrip("/") + "/"
        for name in list(c.store.descriptors):
            if name.startswith(uri):
                del c.store.descriptors[name]

    c.store.delete_prefix = remove
    c.store.prefix_absent = lambda prefix: (
        not any(
            uri.startswith(c.store.uri_for_key(prefix).rstrip("/") + "/")
            for uri in [*c.store.descriptors, *[key[0] for key in c.store.objects]]
        )
    )
    monkeypatch.setattr(deletion, "get_object_store", lambda *_: c.store)
    try:
        with c.factory() as db, db.begin():
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
        done = False
        for _ in range(4):
            with c.factory() as db:
                entry = db.scalar(
                    select(OutboxEntry).where(
                        OutboxEntry.project_id == c.project_id,
                        OutboxEntry.topic == "project.delete",
                    )
                )
                done = reconcile_deletion_stage(db, entry, MagicMock())
        assert done
        assert prefixes == [f"projects/{c.project_id}", f"automl/projects/{c.project_id}"]
        assert not c.store.descriptors and c.store.objects == before
        with c.factory() as db:
            assert db.get(Project, c.project_id) is None
            assert db.get(DatasetProviderImport, c.import_id) is None
            assert (
                db.scalar(
                    select(DeletionTombstone).where(DeletionTombstone.project_id == c.project_id)
                ).status
                == "complete"
            )
    finally:
        with c.factory() as db, db.begin():
            db.execute(delete(DeletionStage).where(DeletionStage.project_id == c.project_id))
            db.execute(
                delete(DeletionTombstone).where(DeletionTombstone.project_id == c.project_id)
            )


def _provider_chart_command():
    return [
        "helm",
        "template",
        "provider-source",
        "infra/helm/sceptre",
        "--set",
        "providerImports.policiesConfigMap=approved-imports",
        "--set",
        "championEvaluation.authorityPublicKeySecret=authority-public",
        "--set",
        "providerImports.image=example.invalid/verifier@sha256:" + "a" * 64,
        "--set",
        "attemptIdentity.controlBaseUrl=https://control.invalid/api/v1/internal/attempts",
        "--set-json",
        'providerImports.egressRules=[{"to":[{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":"trusted"}},"podSelector":{"matchLabels":{"app":"control"}}}],"ports":[{"port":8443,"protocol":"TCP"}]}]',
    ]


@pytest.mark.parametrize("selector", [{}, {"kubernetes.io/hostname": "approved-worker"}])
def test_noncanonical_chart_mounts_public_policy_without_allocator_credentials(selector):
    import json
    import subprocess

    command = [
        *_provider_chart_command(),
        "--set-json",
        "providerImports.nodeSelector=" + json.dumps(selector),
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    docs = list(yaml.safe_load_all(result.stdout))
    deployments = [
        d
        for d in docs
        if d
        and d["kind"] == "Deployment"
        and d["spec"]["template"]["metadata"]["labels"].get("app.kubernetes.io/component")
        in ("api", "reconciler")
    ]
    assert len(deployments) == 2
    for resource in deployments:
        spec = resource["spec"]["template"]["spec"]
        assert any(
            v.get("configMap", {}).get("name") == "approved-imports" for v in spec["volumes"]
        )
        assert any(
            v.get("secret", {}).get("secretName") == "authority-public" for v in spec["volumes"]
        )
        assert all(v["name"] != "evaluation-allocator" for v in spec["volumes"])
    config = next(
        d["data"]
        for d in docs
        if d and d["kind"] == "ConfigMap" and "PROVIDER_IMPORT_IMAGE" in d.get("data", {})
    )
    assert config["PROVIDER_IMPORT_IMAGE"].endswith("@sha256:" + "a" * 64)
    assert config["PROVIDER_IMPORT_MEMORY_MIB"] == "2048"
    assert json.loads(config["PROVIDER_IMPORT_NODE_SELECTOR"]) == selector
    bad = subprocess.run(
        [*command, "--set", "championEvaluation.authorityPublicKeySecret="],
        capture_output=True,
        text=True,
    )
    assert bad.returncode != 0 and "providerImports requires" in bad.stderr


@pytest.mark.parametrize("workload", ["job", "ray"])
@pytest.mark.parametrize("public_key", [True, False])
def test_all_workload_pods_require_and_mount_only_public_source_configuration(
    monkeypatch, workload, public_key
):
    from automl_api.services import kubernetes_training as kube
    from automl_api.services import reconciler
    from test_kubernetes_training_edge_contracts import _capacity, _client
    from test_phase1_reconciler import _attempt, _run

    monkeypatch.setenv("PROVIDER_IMPORT_POLICIES_CONFIG_MAP", "approved-imports")
    if public_key:
        monkeypatch.setenv("EVALUATION_AUTHORITY_PUBLIC_KEY_SECRET", "authority-public")
    else:
        monkeypatch.delenv("EVALUATION_AUTHORITY_PUBLIC_KEY_SECRET", raising=False)

    def build():
        run = _run()
        if workload == "ray":
            return reconciler.build_ray_job_manifest(run, _attempt(run), name="source-test")
        estimate = kube.TrainingEstimateRead(
            capacity=_capacity(),
            estimated_working_set_mb=512,
            cpu_request_cores=1,
            cpu_limit_cores=2,
            memory_request_mb=512,
            memory_limit_mb=1024,
            gpu_requested=False,
            expected_minutes=1,
            active_deadline_seconds=120,
            estimated_core_hours=0.1,
            max_concurrent_jobs=1,
            can_launch=True,
        )
        return _client().build_job_manifest(
            run_id=run.id, project_id=run.project_id, estimate=estimate
        )

    if not public_key:
        with pytest.raises(ValueError, match="authority public key"):
            build()
        return
    spec = build()["spec"]
    pods = (
        [spec["template"]["spec"]]
        if workload == "job"
        else [
            spec["submitterPodTemplate"]["spec"],
            spec["rayClusterSpec"]["headGroupSpec"]["template"]["spec"],
            spec["rayClusterSpec"]["workerGroupSpecs"][0]["template"]["spec"],
        ]
    )
    for pod in pods:
        container = pod["containers"][0]
        env = {item["name"]: item for item in container["env"]}
        assert env["PROVIDER_IMPORT_POLICIES_FILE"]["value"] == "/provider-import/policies.json"
        assert env["EVALUATION_AUTHORITY_PUBLIC_KEY_FILE"]["value"] == (
            "/evaluation-authority/public-key.pem"
        )
        assert (
            not {"EVALUATION_AUTHORITY_TOKEN", "EVALUATION_AUTHORITY_PRIVATE_KEY_FILE"} & env.keys()
        )
        volumes = {item["name"]: item for item in pod["volumes"]}
        assert volumes["provider-import"]["configMap"] == {
            "name": "approved-imports",
            "items": [{"key": "policies.json", "path": "policies.json"}],
        }
        assert volumes["evaluation-authority"]["secret"] == {
            "secretName": "authority-public",
            "items": [{"key": "public-key.pem", "path": "public-key.pem"}],
        }
        mounts = {item["name"]: item for item in container["volumeMounts"]}
        assert all(mounts[name]["readOnly"] for name in ("provider-import", "evaluation-authority"))
