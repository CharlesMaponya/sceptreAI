import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api.models.enums import AttemptStatus
from automl_api.models.workflows import WorkflowAttempt
from automl_api.services import evaluation_access, evaluation_jobs
from automl_api.services.evaluation_keys import prepare_evaluator_credentials
from automl_api.services.evaluation_publication import _event
from automl_api.services.workflow_state import InvalidTransition
from kubernetes.client import ApiException
from test_champion_evaluation_authority import compatible_uri_fixture as compatible_uri_fixture
from test_champion_evaluation_authority import handoff_case as handoff_case
from test_champion_refit_jobs import FakeKubernetes

pytest_plugins = ["test_champion_evaluation_planning", "test_qualification_control"]


class EvaluatorKubernetes(FakeKubernetes):
    def ensure_service_account(self, name):
        assert name == "sceptre-champion-evaluation"

    def read(self, kind, name, namespace):
        result = super().read(kind, name, namespace)
        result.metadata.namespace = namespace
        result.immutable = self.objects[kind][name].get("immutable")
        return result


@pytest.fixture
def evaluator_job_case(handoff_case, monkeypatch):
    db, scope, factory, authority, public_key, _ = handoff_case
    k8s = EvaluatorKubernetes()
    monkeypatch.setattr(
        evaluation_access,
        "get_settings",
        lambda: SimpleNamespace(jwt_secret_key="evaluation-job-tests-key-01234567890123456789"),
    )
    for key, value in {
        "EVALUATION_CONTROL_BASE_URL": "https://api.test/api/v1/internal/evaluations",
        "EVALUATION_AUTHORITY_URL": "https://authority.test",
        "EVALUATION_AUTHORITY_PUBLIC_KEY_SECRET": "authority-public-key",
        "EVALUATION_CA_SECRET": "local-ca",
        "EVALUATION_EGRESS_RULES": json.dumps(
            [
                {
                    "to": [{"podSelector": {"matchLabels": {"app": "evaluation-api"}}}],
                    "ports": [{"port": 8443}],
                }
            ]
        ),
    }.items():
        monkeypatch.setenv(key, value)
    plan, token, key_name = prepare_evaluator_credentials(
        factory, scope.id, k8s, authority, authority_public_key=public_key
    )
    attempt = db.get(WorkflowAttempt, plan.attempt_id)

    def before_create():
        assert not db.get_bind().in_nested_transaction()
        with factory() as session:
            assert session.get(WorkflowAttempt, attempt.id).status == AttemptStatus.SUBMITTED
            assert _event(session, attempt.id, "evaluation_submission") is not None

    k8s.before_create = before_create
    return SimpleNamespace(
        db=db,
        scope=scope,
        factory=factory,
        authority=authority,
        k8s=k8s,
        plan=plan,
        token=token,
        key=key_name,
        attempt=attempt,
    )


def submit(case):
    return evaluation_jobs.submit_evaluator(
        case.factory, case.k8s, case.plan, case.token, case.key, fence=case.attempt.fencing_token
    )


def test_submission_is_durable_isolated_and_replays_native_creation(evaluator_job_case):
    case = evaluator_job_case
    case.k8s.lose_job_reply = True
    with pytest.raises(ApiException):
        submit(case)
    desired = submit(case)
    assert submit(case) == desired
    name = desired["job"]["metadata"]["name"]
    assert len(case.k8s.objects["job"]) == 1
    assert len(case.k8s.objects["secret"]) == 2  # Signing key and two-token delivery Secret.
    pod = desired["job"]["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert desired["job"]["spec"]["backoffLimit"] == 0
    assert 0 < desired["job"]["spec"]["activeDeadlineSeconds"] <= 7200
    assert pod["automountServiceAccountToken"] is False and pod["restartPolicy"] == "Never"
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["securityContext"]["allowPrivilegeEscalation"] is False
    assert container["command"][-1] == "automl_api.training.evaluation_worker"
    assert container["resources"]["requests"] == container["resources"]["limits"]
    env = {e["name"]: e for e in container["env"]}
    assert "DATABASE_URL" not in env and "AWS_SECRET_ACCESS_KEY" not in env
    assert env["EVALUATION_AUTHORITY_TOKEN"]["valueFrom"]["secretKeyRef"]["name"] == name
    assert case.token not in json.dumps(desired)
    assert desired["network"]["spec"]["ingress"] == []
    assert desired["network"]["spec"]["podSelector"]["matchLabels"][evaluation_jobs.LABEL] == str(
        case.plan.attempt_id
    )
    state = case.authority.get(f"allocations/{case.plan.allocation_id}").json()
    assert state["status"] == "allocated" and state["cas_version"] == 0


@pytest.mark.parametrize(
    "field,value",
    [
        ("EVALUATION_CONTROL_BASE_URL", "http://api.test"),
        ("EVALUATION_AUTHORITY_URL", "https://name:secret@authority.test"),
        ("EVALUATION_IMAGE", "image:latest"),
        ("EVALUATION_EGRESS_RULES", "[]"),
        ("EVALUATION_AUTHORITY_PUBLIC_KEY_SECRET", ""),
        ("EVALUATION_MEMORY_MIB", "0"),
    ],
)
def test_invalid_config_creates_no_workload(evaluator_job_case, monkeypatch, field, value):
    case = evaluator_job_case
    monkeypatch.setenv(field, value)
    with pytest.raises(ValueError):
        submit(case)
    assert not case.k8s.objects["job"] and not case.k8s.objects["network_policy"]
    case.db.refresh(case.attempt)
    assert case.attempt.status == AttemptStatus.PENDING


@pytest.mark.parametrize("change", ["token", "manifest", "expired", "started"])
def test_replay_rejects_changed_or_started_submission(evaluator_job_case, change):
    case = evaluator_job_case
    desired = submit(case)
    name = desired["job"]["metadata"]["name"]
    if change == "token":
        case.k8s.objects["secret"][name]["stringData"]["authority"] = "other-token"
    elif change == "manifest":
        case.k8s.objects["job"][name]["metadata"]["annotations"] = {}
    else:
        case.db.refresh(case.attempt)
        if change == "expired":
            case.attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        else:
            case.attempt.status = AttemptStatus.RUNNING
        case.db.flush()
    with pytest.raises(ValueError):
        submit(case)
    assert len(case.k8s.objects["job"]) == 1


def test_wrong_registered_token_or_key_is_rejected(evaluator_job_case):
    case = evaluator_job_case
    with pytest.raises(InvalidTransition):
        evaluation_jobs.submit_evaluator(
            case.factory, case.k8s, case.plan, "wrong", case.key, fence=case.attempt.fencing_token
        )
    with pytest.raises(InvalidTransition):
        evaluation_jobs.submit_evaluator(
            case.factory, case.k8s, case.plan, case.token, "wrong", fence=case.attempt.fencing_token
        )
    assert not case.k8s.objects["job"]
