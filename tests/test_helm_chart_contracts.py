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


@pytest.mark.parametrize(
    "override, message",
    [
        ("postgresql.enabled=true", "external PostgreSQL"),
        ("auth.existingSecret=", "existing auth"),
        ("auth.simpleAuthEnabled=true", "OIDC"),
        ("auth.oidc.issuer=", "Production OIDC"),
        ("auth.oidc.issuer=http://identity.example.test", "Production OIDC"),
        ("auth.oidc.clientId=", "Production OIDC"),
        ("auth.oidc.requireMfa=false", "Production OIDC"),
        ("gateway.tls.enabled=false", "TLS Gateway"),
        ("inference.ingress.enabled=true", "direct inference"),
        ("api.image.digest=", "pinned by sha256"),
        ("kuberay-operator.enabled=true", "platform-managed"),
    ],
)
def test_production_rejects_unsafe_defaults(override, message):
    import subprocess

    result = subprocess.run(
        [
            "helm",
            "template",
            "test",
            str(CHART_DIR),
            "-f",
            str(CHART_DIR.parents[2] / "tests/fixtures/production-values.yaml"),
            "--set",
            override,
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert message in result.stderr


def test_production_manifest_uses_external_services_digests_and_disruption_budgets():
    import subprocess

    import yaml

    result = subprocess.run(
        [
            "helm",
            "template",
            "test",
            str(CHART_DIR),
            "-f",
            str(CHART_DIR.parents[2] / "tests/fixtures/production-values.yaml"),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    assert not any(doc["kind"] in {"StatefulSet", "Secret"} for doc in docs)
    budgets = [doc for doc in docs if doc["kind"] == "PodDisruptionBudget"]
    assert len(budgets) == 3
    gateway = next(doc for doc in docs if doc["kind"] == "Gateway")
    assert [item["protocol"] for item in gateway["spec"]["listeners"]] == ["HTTPS"]
    route = next(doc for doc in docs if doc["kind"] == "HTTPRoute")
    assert route["spec"]["parentRefs"][0]["sectionName"] == "https"
    config = next(doc for doc in docs if doc["kind"] == "ConfigMap")["data"]
    from automl_api.core.config import Settings
    from automl_api.security.authentication_policy import validate_authentication_configuration

    validate_authentication_configuration(
        Settings(
            environment=config["ENVIRONMENT"],
            simple_auth_enabled=False,
            oidc_issuer=config["OIDC_ISSUER"],
            oidc_client_id=config["OIDC_CLIENT_ID"],
            oidc_require_mfa=config["OIDC_REQUIRE_MFA"] == "true",
            public_app_url=config["PUBLIC_APP_URL"],
        )
    )
    for doc in docs:
        if doc["kind"] == "Deployment":
            pod = doc["spec"]["template"]["spec"]
            assert pod["topologySpreadConstraints"]
            assert all("@sha256:" in item["image"] for item in pod["containers"])


def test_local_http_gateway_and_confidential_oidc_secret_reference():
    import subprocess

    import yaml

    result = subprocess.run(
        [
            "helm",
            "template",
            "test",
            str(CHART_DIR),
            "--set",
            "gateway.enabled=true",
            "--set",
            "auth.oidc.existingSecret=organization-identity",
            "--set",
            "auth.oidc.clientSecretKey=client-secret",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    gateway = next(doc for doc in docs if doc["kind"] == "Gateway")
    assert [item["protocol"] for item in gateway["spec"]["listeners"]] == ["HTTP"]
    route = next(doc for doc in docs if doc["kind"] == "HTTPRoute")
    assert route["spec"]["parentRefs"][0]["sectionName"] == "http"
    api = next(
        doc
        for doc in docs
        if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "test-sceptre-api"
    )
    container = api["spec"]["template"]["spec"]["containers"][0]
    secret_env = next(item for item in container["env"] if item["name"] == "OIDC_CLIENT_SECRET")
    assert secret_env == {
        "name": "OIDC_CLIENT_SECRET",
        "valueFrom": {
            "secretKeyRef": {
                "name": "organization-identity",
                "key": "client-secret",
            }
        },
    }


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
    workloads = rbac.split('name: {{ include "sceptre.fullname" . }}-workloads', 1)[1]
    workloads = workloads.split("---", 1)[0]
    assert 'resources: ["deployments/scale"]\n    verbs: ["get", "patch"]' in workloads
    for resource in ("services", "deployments"):
        rule = f'resources: ["{resource}"]\n    verbs: ["get", "list", "watch", "delete"]'
        assert rule in workloads


@pytest.mark.parametrize(
    "override",
    [
        "championEvaluation.enabled=false",
        "championEvaluation.controlBaseUrl=http://api.example.test",
        "championEvaluation.authorityUrl=https://user:pass@authority.example.test",
        "championEvaluation.allocatorSecret=",
        "championEvaluation.authorityPublicKeySecret=",
        "championEvaluation.image=example/evaluator:latest",
        "championEvaluation.egressRules[0].ports[0].port=80",
    ],
)
def test_evaluator_configuration_fails_closed(override):
    import subprocess

    result = subprocess.run(
        [
            "helm",
            "template",
            "test",
            str(CHART_DIR),
            "-f",
            str(CHART_DIR.parents[2] / "tests/fixtures/production-values.yaml"),
            "--set",
            override,
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "championEvaluation" in result.stderr


def test_evaluator_allocator_credentials_are_controller_only():
    import subprocess

    import yaml

    result = subprocess.run(
        [
            "helm",
            "template",
            "test",
            str(CHART_DIR),
            "-f",
            str(CHART_DIR.parents[2] / "tests/fixtures/production-values.yaml"),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    config = next(doc for doc in docs if doc["kind"] == "ConfigMap")["data"]
    assert config["EVALUATION_IMAGE"].endswith("@sha256:" + "0" * 64)
    assert json.loads(config["EVALUATION_EGRESS_RULES"])[0]["ports"][0]["port"] == 443
    assert "EVALUATION_ALLOCATOR_TOKENS_FILE" not in config
    jwt_refs = []
    for doc in docs:
        if doc["kind"] != "Deployment":
            continue
        pod = doc["spec"]["template"]["spec"]
        component = doc["spec"]["template"]["metadata"]["labels"].get("app.kubernetes.io/component")
        volumes = {v["name"]: v for v in pod.get("volumes", [])}
        containers = pod.get("containers", []) + pod.get("initContainers", [])
        for container in containers:
            env = {e["name"]: e for e in container.get("env", [])}
            mounts = {m["name"]: m for m in container.get("volumeMounts", [])}
            if container["name"] == "reconciler":
                assert (
                    env["EVALUATION_ALLOCATOR_TOKENS_FILE"]["value"]
                    == "/evaluation-allocator/tokens.json"
                )
                assert mounts["evaluation-allocator"]["readOnly"] is True
                assert (
                    volumes["evaluation-allocator"]["secret"]["secretName"]
                    == "evaluation-allocators"
                )
                assert env["EVALUATION_CA_FILE"]["value"] == "/evaluation-ca/ca.crt"
                assert volumes["evaluation-ca"]["secret"]["items"] == [
                    {"key": "ca.crt", "path": "ca.crt"}
                ]
            else:
                assert "EVALUATION_ALLOCATOR_TOKENS_FILE" not in env
                assert "evaluation-allocator" not in mounts
            if container["name"] in {"api", "reconciler"}:
                assert (
                    env["EVALUATION_AUTHORITY_PUBLIC_KEY_FILE"]["value"]
                    == "/evaluation-authority/public-key.pem"
                )
                assert mounts["evaluation-authority"]["readOnly"] is True
                jwt_refs.append(env["JWT_SECRET_KEY"]["valueFrom"]["secretKeyRef"])
        if component != "reconciler":
            assert not any(
                v.get("secret", {}).get("secretName") == "evaluation-allocators"
                for v in volumes.values()
            )
    assert len(jwt_refs) == 2 and jwt_refs[0] == jwt_refs[1]


def test_disabled_evaluator_renders_no_allocator_mounts():
    import subprocess

    import yaml

    result = subprocess.run(
        ["helm", "template", "test", str(CHART_DIR)], capture_output=True, text=True, check=True
    )
    docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    for doc in docs:
        if doc["kind"] == "Deployment":
            volumes = doc["spec"]["template"]["spec"].get("volumes", [])
            assert not any(v["name"].startswith("evaluation-") for v in volumes)
        if doc["kind"] == "ConfigMap":
            assert not any(key.startswith("EVALUATION_") for key in doc["data"])
