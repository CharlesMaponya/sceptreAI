import copy
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import AttemptStatus, WorkflowStage
from automl_api.services import reconciler
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.workflow_state import StaleFence
from digest_fixtures import verified_raw_version
from kubernetes.client import ApiException
from ray_submission_helpers import submit_training_ray_job
from test_phase1_reconciler import _attempt, _entry, _run


@pytest.fixture
def submission():
    run = _run()
    attempt = _attempt(run)
    entry = _entry(attempt)
    db = MagicMock()
    values = iter([attempt, run])
    db.scalar.side_effect = lambda statement: (
        None
        if statement.column_descriptions[0]["entity"].__name__
        in {"DatasetProviderImport", "PromotionalScopeMember", "WorkflowEvent"}
        else next(values)
    )
    db.get.return_value = verified_raw_version(id=run.dataset_version_id, project_id=run.project_id)
    k8s = MagicMock()
    k8s.create_ray_job.side_effect = lambda body: {
        **copy.deepcopy(body),
        "metadata": {**body["metadata"], "uid": str(uuid.uuid4())},
    }
    return db, run, attempt, entry, k8s


@pytest.mark.parametrize("terminal", [False, True])
@pytest.mark.parametrize(
    "invalid", ["project", "run", "aggregate_type", "stage", "logical_key", "topic"]
)
def test_training_submission_rejects_mismatched_outbox_before_native_work(
    submission, invalid, terminal
):
    db, _, attempt, entry, k8s = submission
    if terminal:
        attempt.status = AttemptStatus.SUCCEEDED
    if invalid == "project":
        entry.project_id = uuid.uuid4()
    elif invalid == "run":
        entry.aggregate_id = uuid.uuid4()
    elif invalid == "aggregate_type":
        entry.aggregate_type = "profiling_job"
    elif invalid == "stage":
        attempt.stage = WorkflowStage.TRAINING_TRIAL
    elif invalid == "logical_key":
        attempt.logical_key = f"training-run:{uuid.uuid4()}"
    else:
        entry.topic = "ray.dataset.split.submit"
    with pytest.raises(StaleFence, match="scope"):
        submit_training_ray_job(db, entry, k8s)
    k8s.ensure_service_account.assert_not_called()
    k8s.create_ray_job.assert_not_called()


def test_valid_training_scope_preserves_existing_job_name_and_terminal_replay(submission):
    db, run, attempt, entry, k8s = submission
    submit_training_ray_job(db, entry, k8s)
    expected = f"sceptre-run-{str(run.id)[:8]}-g{attempt.generation}"
    assert attempt.ray_job_name == expected
    assert k8s.create_ray_job.call_args.args[0]["metadata"]["name"] == expected
    db.scalar.side_effect = None
    db.scalar.return_value = attempt
    assert submit_training_ray_job(db, entry, k8s) is attempt
    assert k8s.create_ray_job.call_count == 1


@pytest.fixture
def native_submission():
    run = _run()
    attempt = _attempt(run)
    manifest = reconciler.build_ray_job_manifest(run, attempt, name="existing-short-job-name")
    k8s = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    k8s._configured = True
    k8s.settings = SimpleNamespace(training_namespace="wrong-default")
    k8s.custom = MagicMock()
    created = copy.deepcopy(manifest)
    created["metadata"]["uid"] = str(uuid.uuid4())
    k8s.custom.create_namespaced_custom_object.return_value = created
    k8s.custom.get_namespaced_custom_object.return_value = created
    return k8s, manifest, created


@pytest.mark.parametrize(
    "invalid",
    [
        "project",
        "attempt",
        "generation",
        "run",
        "name",
        "namespace",
        "uid",
        "job_id",
        "entrypoint",
        "image",
        "environment",
        "resources",
        "worker_groups",
        "spec",
        "response",
        "runtime_environment",
        "container_command",
    ],
)
def test_native_ray_response_must_match_complete_submission(native_submission, invalid):
    k8s, manifest, created = native_submission
    if invalid in {"project", "attempt", "generation", "run"}:
        key = {
            "project": "project-id",
            "attempt": "attempt-id",
            "generation": "generation",
            "run": "run-id",
        }[invalid]
        created["metadata"]["labels"]["automl.platform/" + key] = "foreign"
    elif invalid in {"name", "namespace", "uid"}:
        created["metadata"][invalid] = "" if invalid == "uid" else "foreign"
    elif invalid in {"job_id", "entrypoint"}:
        created["spec"]["jobId" if invalid == "job_id" else "entrypoint"] = "foreign"
    elif invalid == "runtime_environment":
        created["spec"]["runtimeEnvYAML"] = "env_vars: {UNREQUESTED: true}"
    elif invalid == "container_command":
        created["spec"]["rayClusterSpec"]["headGroupSpec"]["template"]["spec"]["containers"][0][
            "command"
        ] = ["foreign-command"]
    elif invalid == "spec":
        created["spec"] = None
    elif invalid == "response":
        k8s.custom.create_namespaced_custom_object.return_value = None
        k8s.custom.get_namespaced_custom_object.return_value = None
    elif invalid == "worker_groups":
        created["spec"]["rayClusterSpec"]["workerGroupSpecs"] = []
    else:
        container = created["spec"]["rayClusterSpec"]["headGroupSpec"]["template"]["spec"][
            "containers"
        ][0]
        if invalid == "image":
            container["image"] = "foreign/image"
        elif invalid == "environment":
            container["env"][0]["value"] = "foreign"
        else:
            container["resources"]["limits"]["cpu"] = "99"
    with pytest.raises(ValueError, match="RayJob"):
        k8s.create_ray_job(manifest)


def test_native_ray_creation_accepts_submitted_fields_plus_server_defaults(native_submission):
    k8s, manifest, created = native_submission
    created["metadata"].update(resourceVersion="23", creationTimestamp="2026-10-22T00:00:00Z")
    created["metadata"]["labels"]["operator-added"] = "true"
    created["status"] = {"jobDeploymentStatus": "Suspended"}
    assert k8s.create_ray_job(manifest) is created
    k8s.custom.create_namespaced_custom_object.assert_called_once()
    k8s.custom.get_namespaced_custom_object.assert_not_called()


@pytest.mark.parametrize("status", [403, 500])
def test_native_create_errors_do_not_trigger_conflict_adoption(native_submission, status):
    k8s, manifest, _ = native_submission
    k8s.custom.create_namespaced_custom_object.side_effect = ApiException(status=status)
    with pytest.raises(ApiException) as caught:
        k8s.create_ray_job(manifest)
    assert caught.value.status == status
    k8s.custom.get_namespaced_custom_object.assert_not_called()


def test_lost_creation_reply_cannot_adopt_an_unproven_existing_job(native_submission):
    k8s, manifest, _ = native_submission
    k8s.custom.create_namespaced_custom_object.side_effect = [
        ConnectionError("lost ACK"),
        ApiException(status=409),
    ]
    with pytest.raises(ConnectionError):
        k8s.create_ray_job(manifest)
    with pytest.raises(ApiException) as held:
        k8s.create_ray_job(manifest)
    assert held.value.status == 409
    calls = k8s.custom.create_namespaced_custom_object.call_args_list
    assert calls[0] == calls[1]
    k8s.custom.get_namespaced_custom_object.assert_not_called()


@pytest.mark.parametrize("stage", ["training_run", "splitter", "preparation"])
def test_manifest_pins_existing_native_worker_host_default(stage):
    from test_phase3_preparation import _dataset_attempt, _profile_and_version

    if stage == "training_run":
        run = _run()
        manifest = reconciler.build_ray_job_manifest(run, _attempt(run), name="existing-name")
    else:
        attempt = _dataset_attempt(WorkflowStage(stage))
        profile, version = _profile_and_version(attempt)
        manifest = reconciler.build_dataset_ray_job_manifest(
            profile, version, attempt, name="existing-name"
        )
    groups = manifest["spec"]["rayClusterSpec"]["workerGroupSpecs"]
    assert len(groups) == 1 and groups[0]["numOfHosts"] == 1


def test_unknown_conflict_cannot_adopt_a_replacement_uid(native_submission):
    k8s, manifest, created = native_submission
    created["metadata"]["uid"] = "replacement-with-identical-command-fields"
    k8s.custom.create_namespaced_custom_object.side_effect = ApiException(status=409)
    with pytest.raises(ApiException) as caught:
        k8s.create_ray_job(manifest)
    assert caught.value.status == 409
    k8s.custom.get_namespaced_custom_object.assert_not_called()
