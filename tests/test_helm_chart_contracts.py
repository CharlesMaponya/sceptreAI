"""Helm chart contract tests (task-index P5-W01/W02/W03).

These tests validate the values schema closure and the Gateway API ingress
contract without requiring a Helm binary, so they run in every CI job.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

CHART_DIR = Path(__file__).resolve().parents[1] / "infra" / "helm" / "sceptre"
SCHEMA_PATH = CHART_DIR / "values.schema.json"


@pytest.mark.parametrize("override, message", [
    ("postgresql.enabled=true", "external PostgreSQL"),
    ("auth.existingSecret=", "existing auth"),
    ("auth.simpleAuthEnabled=true", "OIDC"),
    ("gateway.tls.enabled=false", "TLS Gateway"),
    ("inference.ingress.enabled=true", "direct inference"),
    ("api.image.digest=", "pinned by sha256"),
    ("kuberay-operator.enabled=true", "platform-managed"),
])
def test_production_rejects_unsafe_defaults(override, message):
    import subprocess

    result = subprocess.run([
        "helm", "template", "test", str(CHART_DIR), "-f",
        str(CHART_DIR.parents[2] / "tests/fixtures/production-values.yaml"),
        "--set", override,
    ], capture_output=True, text=True)
    assert result.returncode != 0
    assert message in result.stderr


def test_production_manifest_uses_external_services_digests_and_disruption_budgets():
    import subprocess
    import yaml

    result = subprocess.run([
        "helm", "template", "test", str(CHART_DIR), "-f",
        str(CHART_DIR.parents[2] / "tests/fixtures/production-values.yaml"),
    ], capture_output=True, text=True, check=True)
    docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    assert not any(doc["kind"] in {"StatefulSet", "Secret"} for doc in docs)
    budgets = [doc for doc in docs if doc["kind"] == "PodDisruptionBudget"]
    assert len(budgets) == 3
    for doc in docs:
        if doc["kind"] == "Deployment":
            pod = doc["spec"]["template"]["spec"]
            assert pod["topologySpreadConstraints"]
            assert all("@sha256:" in item["image"] for item in pod["containers"])


@pytest.fixture(scope="module")
def schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def _iter_objects(node: object):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _iter_objects(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_objects(item)


def test_every_object_schema_is_closed(schema: dict) -> None:
    """P5-W01: additionalProperties must be false on every declared object."""
    objects = [
        node
        for node in _iter_objects(schema)
        if node.get("type") == "object" and "properties" in node
    ]
    assert objects, "schema declares no objects"
    open_objects = [node for node in objects if node.get("additionalProperties") is not False]
    # Passthrough nodes (subchart forwarding) may omit 'properties'; any node
    # that declares properties must close them.
    offenders = [node for node in open_objects if node.get("properties")]
    assert not offenders, f"{len(offenders)} object schemas are not closed"


def test_gateway_section_pins_api_version_and_controller(schema: dict) -> None:
    """P5-W04: Gateway API version and controller are pinned in the schema."""
    gateway = schema["properties"]["gateway"]
    api_version = gateway["properties"]["apiVersion"]
    assert api_version.get("enum") == [
        "gateway.networking.k8s.io/v1",
        "gateway.networking.k8s.io/v1beta1",
    ]
    controller = gateway["properties"]["controllerName"]
    assert controller.get("minLength") == 1


def test_nginx_gateway_class_is_forbidden(schema: dict) -> None:
    """P5-W03: the retired ingress-nginx controller is rejected by schema."""
    class_name = schema["properties"]["gateway"]["properties"]["className"]
    assert class_name.get("not") == {"enum": ["nginx"]}


def test_default_values_disable_gateway_and_use_no_ingress_class() -> None:
    """Default profile ships with gateway disabled and no nginx class set."""
    import yaml

    values = yaml.safe_load((CHART_DIR / "values.yaml").read_text(encoding="utf-8"))
    assert values["gateway"]["enabled"] is False
    assert values["ingress"]["className"] == ""
    assert "nginx" not in json.dumps(values).lower() or all(
        "nginx" not in str(key) for key in values
    )


def test_gateway_template_renders_the_three_core_resources() -> None:
    """The gateway template references GatewayClass, Gateway, and HTTPRoute."""
    template = (CHART_DIR / "templates" / "gateway.yaml").read_text(encoding="utf-8")
    for kind in ("kind: GatewayClass", "kind: Gateway\n", "kind: HTTPRoute"):
        assert kind in template
    assert "gateway.networking.k8s.io" in template


def test_rbac_defines_stage_scoped_service_accounts() -> None:
    """P5-W05: splitter/preparation/refit/evaluation identities exist."""
    rbac = (CHART_DIR / "templates" / "rbac.yaml").read_text(encoding="utf-8")
    for identity in (
        "sceptre-dataset-splitter",
        "sceptre-dataset-preparation",
        "sceptre-champion-refit",
        "sceptre-champion-evaluation",
    ):
        assert f"name: {identity}" in rbac


def test_rendered_analysis_workers_have_a_provisioned_secret_and_observer_permissions():
    import shutil
    import subprocess

    import yaml

    if not shutil.which("helm"):
        pytest.skip("Helm is required to verify rendered credential wiring")
    rendered = subprocess.run(
        ["helm", "template", "sceptre", str(CHART_DIR)], capture_output=True, text=True, check=True
    )
    documents = list(yaml.safe_load_all(rendered.stdout))
    config = next(
        doc
        for doc in documents
        if doc
        and doc["kind"] == "ConfigMap"
        and "WORKER_DATABASE_SECRET_NAME" in doc.get("data", {})
    )["data"]
    secret = next(
        doc
        for doc in documents
        if doc
        and doc["kind"] == "Secret"
        and doc["metadata"]["name"] == config["WORKER_DATABASE_SECRET_NAME"]
    )
    assert config["WORKER_DATABASE_SECRET_KEY"] in secret["data"]
    migration = next(
        doc
        for doc in documents
        if doc and doc["kind"] == "Job" and "-migrate-" in doc["metadata"]["name"]
    )
    container = migration["spec"]["template"]["spec"]["containers"][0]
    assert "provision_worker_database.py" in container["args"][0]
    assert any(env["name"] == "WORKER_DATABASE_URL" for env in container["env"])
    role = next(
        doc
        for doc in documents
        if doc and doc["kind"] == "Role" and doc["metadata"]["name"] == "sceptre-reconciler"
    )
    assert any(
        "jobs/status" in rule["resources"] and "get" in rule["verbs"] for rule in role["rules"]
    )
    assert any("pods" in rule["resources"] and "list" in rule["verbs"] for rule in role["rules"])


def test_manual_serving_controls_have_narrow_scale_and_delete_permissions() -> None:
    rbac = (CHART_DIR / "templates" / "rbac.yaml").read_text(encoding="utf-8")
    workloads = rbac.split('name: {{ include "sceptre.fullname" . }}-workloads', 1)[1].split("---", 1)[0]
    assert 'resources: ["deployments/scale"]\n    verbs: ["get", "patch"]' in workloads
    for resource in ("services", "deployments"):
        assert f'resources: ["{resource}"]\n    verbs: ["get", "list", "watch", "delete"]' in workloads
