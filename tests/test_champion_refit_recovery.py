"""Refit recovery cannot mistake lost custody for confirmed cleanup."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.enums import AttemptStatus, ScopeStatus
from automl_api.models.workflows import PromotionalScope, WorkflowEvent
from automl_api.services import refit_jobs
from sqlalchemy import func, select

pytest_plugins = ["test_champion_refit_jobs"]


def test_missing_submission_does_not_acknowledge_cleanup_with_live_resources(jobs_case):
    db, _, _, attempt, k8s, factory = jobs_case
    refit_jobs._reconcile(factory, k8s, attempt.id)
    event = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempt.id,
            WorkflowEvent.event_key == "refit_submission",
        )
    )
    assert event is not None and all(k8s.objects.values())
    resources = deepcopy(k8s.objects)
    db.refresh(attempt)
    attempt.status = AttemptStatus.FAILED
    db.delete(event)
    db.flush()
    refit_jobs._reconcile(factory, k8s, attempt.id)
    db.refresh(attempt)
    assert attempt.cleanup_state != "complete"
    assert k8s.objects == resources
    db.add(
        WorkflowEvent(
            project_id=event.project_id,
            attempt_id=event.attempt_id,
            sequence=1
            + db.scalar(
                select(func.max(WorkflowEvent.sequence)).where(
                    WorkflowEvent.attempt_id == attempt.id
                )
            ),
            event_key=event.event_key,
            event_type=event.event_type,
            payload=event.payload,
            result_digest=event.result_digest,
        )
    )
    db.flush()
    for _ in range(2):
        refit_jobs._reconcile(factory, k8s, attempt.id)
    db.refresh(attempt)
    assert attempt.cleanup_state == "complete" and not any(k8s.objects.values())


def test_scope_deadline_loss_during_observation_terminates_attempt(jobs_case):
    db, _, scope, attempt, k8s, factory = jobs_case
    refit_jobs._reconcile(factory, k8s, attempt.id)
    resources = deepcopy(k8s.objects)

    def deadline_removed(_name):
        with factory() as session, session.begin():
            session.get(PromotionalScope, scope.id).scope_deadline_at = None
        return "missing"

    k8s.job_state = deadline_removed
    refit_jobs._reconcile(factory, k8s, attempt.id)
    db.refresh(attempt)
    assert attempt.status == AttemptStatus.FAILED
    assert attempt.cleanup_state != "complete"
    assert k8s.objects == resources


@pytest.mark.parametrize(
    "change", ["expired_scope", "cancelled_scope", "expired_lease", "missing_lease", "unchanged"]
)
def test_observation_rechecks_scope_and_lease_before_side_effects(jobs_case, change):
    db, _, scope, attempt, k8s, factory = jobs_case
    refit_jobs._reconcile(factory, k8s, attempt.id)
    resources = deepcopy(k8s.objects)

    def changed_during_observation(_name):
        from automl_api.models.workflows import WorkflowAttempt

        with factory() as session, session.begin():
            current_scope = session.get(PromotionalScope, scope.id)
            current = session.get(WorkflowAttempt, attempt.id)
            if change == "expired_scope":
                current_scope.scope_deadline_at = datetime.now(UTC) - timedelta(seconds=1)
            elif change == "cancelled_scope":
                current_scope.status = ScopeStatus.CANCELLED
            elif change == "expired_lease":
                current.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif change == "missing_lease":
                current.lease_expires_at = None
        return "queued" if change == "unchanged" else "missing"

    k8s.job_state = changed_during_observation
    refit_jobs._reconcile(factory, k8s, attempt.id)
    db.refresh(attempt)
    db.refresh(scope)
    assert attempt.status == (
        AttemptStatus.SUBMITTED if change == "unchanged" else AttemptStatus.FAILED
    )
    assert scope.status == {
        "expired_scope": ScopeStatus.FAILED,
        "cancelled_scope": ScopeStatus.CANCELLED,
    }.get(change, ScopeStatus.RUNNING)
    assert k8s.objects == resources


@pytest.mark.parametrize(
    "field,value",
    [
        ("REFIT_CONTROL_BASE_URL", "http://control.test"),
        ("REFIT_CONTROL_BASE_URL", "https://name:secret@control.test"),
        ("REFIT_IMAGE", "image:latest"),
        ("REFIT_EGRESS_RULES", "[]"),
        ("REFIT_EGRESS_RULES", '[{"to":[{"podSelector":{}}],"ports":[{"port":80}]}]'),
        ("REFIT_MEMORY_MIB", "0"),
        ("REFIT_CPU_CORES", "0"),
    ],
)
def test_invalid_submission_configuration_has_no_external_effects(
    jobs_case, monkeypatch, field, value
):
    db, _, scope, attempt, k8s, factory = jobs_case
    monkeypatch.setenv(field, value)
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    db.refresh(attempt)
    db.refresh(scope)
    assert attempt.status == AttemptStatus.CANCELLED
    assert scope.status == ScopeStatus.FAILED
    assert not any(k8s.objects.values())
    assert (
        db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == attempt.id,
                WorkflowEvent.event_key == "refit_submission",
            )
        )
        is None
    )


def test_refit_submission_mounts_explicit_ca_without_ambient_credentials(jobs_case, monkeypatch):
    _, _, _, attempt, k8s, factory = jobs_case
    monkeypatch.setenv("REFIT_CA_SECRET", "reviewed-refit-ca")
    refit_jobs._reconcile(factory, k8s, attempt.id)
    pod = k8s.objects["job"][f"refit-{attempt.id.hex}"]["spec"]["template"]["spec"]
    assert (
        next(v for v in pod["volumes"] if v["name"] == "ca")["secret"]["secretName"]
        == "reviewed-refit-ca"
    )
    container = pod["containers"][0]
    assert {e["name"]: e.get("value") for e in container["env"]}[
        "REFIT_CA_FILE"
    ] == "/refit-ca/ca.crt"
    assert next(m for m in container["volumeMounts"] if m["name"] == "ca")["readOnly"] is True


@pytest.mark.parametrize("kind", ["secret", "network_policy", "job"])
def test_transient_native_creation_failure_preserves_durable_retry(jobs_case, monkeypatch, kind):
    from kubernetes.client import ApiException

    db, _, _, attempt, k8s, factory = jobs_case
    api = {"secret": k8s.core, "network_policy": k8s.networking, "job": k8s.batch}[kind]
    method = f"create_namespaced_{kind}"
    create = getattr(api, method)

    def denied(*_args, **_kwargs):
        raise ApiException(status=503)

    monkeypatch.setattr(api, method, denied)
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    db.refresh(attempt)
    assert attempt.status == AttemptStatus.SUBMITTED
    assert attempt.cleanup_state != "complete"
    event = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempt.id, WorkflowEvent.event_key == "refit_submission"
        )
    )
    assert event is not None
    digest = event.result_digest
    monkeypatch.setattr(api, method, create)
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    db.refresh(attempt)
    db.refresh(event)
    assert attempt.status == AttemptStatus.SUBMITTED and attempt.generation == 1
    assert event.result_digest == digest
    assert all(len(resources) == 1 for resources in k8s.objects.values())


@pytest.mark.parametrize("kind", ["network_policy", "job"])
def test_conflicting_native_manifest_is_not_adopted(jobs_case, kind):
    db, _, _, attempt, k8s, factory = jobs_case
    refit_jobs._reconcile(factory, k8s, attempt.id)
    name = f"refit-{attempt.id.hex}"
    k8s.objects[kind][name]["metadata"]["annotations"][refit_jobs.DIGEST] = "changed"
    resources = deepcopy(k8s.objects)
    k8s.states[name] = "missing"
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    db.refresh(attempt)
    assert attempt.status == AttemptStatus.FAILED
    assert attempt.cleanup_state != "complete" and k8s.objects == resources


@pytest.mark.parametrize("change", ["missing", "changed"])
def test_active_attempt_rejects_missing_or_changed_submission(jobs_case, change):
    db, _, _, attempt, k8s, factory = jobs_case
    refit_jobs._reconcile(factory, k8s, attempt.id)
    resources = deepcopy(k8s.objects)
    event = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempt.id, WorkflowEvent.event_key == "refit_submission"
        )
    )
    if change == "missing":
        db.delete(event)
    else:
        event.result_digest = "changed"
    db.flush()
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    db.refresh(attempt)
    assert attempt.status == AttemptStatus.FAILED
    assert attempt.cleanup_state != "complete" and k8s.objects == resources


@pytest.mark.parametrize("change", ["label", "project", "fence"])
def test_existing_token_secret_cannot_cross_ownership(jobs_case, change):
    import uuid
    from types import SimpleNamespace

    from automl_api.services.refit_access import refit_token

    db, _, scope, attempt, k8s, factory = jobs_case
    refit_jobs._reconcile(factory, k8s, attempt.id)
    secret = k8s.objects["secret"][f"refit-{attempt.id.hex}"]
    if change == "label":
        secret["metadata"]["labels"][refit_jobs.LABEL] = str(uuid.uuid4())
    else:
        secret["stringData"]["token"] = refit_token(
            SimpleNamespace(
                id=attempt.id,
                project_id=uuid.uuid4() if change == "project" else attempt.project_id,
                fencing_token="other-fence" if change == "fence" else attempt.fencing_token,
            ),
            scope.scope_deadline_at,
        )
    resources = deepcopy(k8s.objects)
    k8s.states[f"refit-{attempt.id.hex}"] = "missing"
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    db.refresh(attempt)
    assert attempt.status == AttemptStatus.FAILED
    assert attempt.cleanup_state != "complete" and k8s.objects == resources


@pytest.mark.parametrize("different_stage", [False, True])
def test_sweep_preserves_active_or_differently_typed_attempt(jobs_case, different_stage):
    from automl_api.models.enums import WorkflowStage

    db, _, _, attempt, k8s, factory = jobs_case
    refit_jobs._reconcile(factory, k8s, attempt.id)
    resources = deepcopy(k8s.objects)
    if different_stage:
        db.refresh(attempt)
        attempt.stage = WorkflowStage.CHAMPION_EVALUATION
        attempt.status = AttemptStatus.FAILED
        db.flush()
    refit_jobs.sweep_refit_resources(factory, k8s)
    assert k8s.objects == resources


def test_sweep_follows_pages_and_preserves_noncanonical_names(jobs_case, monkeypatch):
    from types import SimpleNamespace

    db, _, _, attempt, k8s, factory = jobs_case
    refit_jobs._reconcile(factory, k8s, attempt.id)
    name = f"refit-{attempt.id.hex}"
    for objects in k8s.objects.values():
        objects["foreign-name"] = deepcopy(objects[name])
        objects["foreign-name"]["metadata"]["name"] = "foreign-name"
    db.refresh(attempt)
    attempt.status = AttemptStatus.FAILED
    db.flush()
    pages = []
    for api, kind in ((k8s.core, "secret"), (k8s.networking, "network_policy"), (k8s.batch, "job")):
        method = f"list_namespaced_{kind}"
        original = getattr(api, method)

        def paginated(*, _continue=None, original=original, **kwargs):
            pages.append(_continue)
            if _continue is None:
                return SimpleNamespace(
                    items=[
                        SimpleNamespace(
                            metadata=SimpleNamespace(
                                labels={refit_jobs.LABEL: "invalid"}, name=name
                            )
                        )
                    ],
                    metadata=SimpleNamespace(_continue="next-page"),
                )
            assert _continue == "next-page"
            return original(**kwargs)

        monkeypatch.setattr(api, method, paginated)
    for _ in range(2):
        refit_jobs.sweep_refit_resources(factory, k8s)
    assert pages.count(None) == pages.count("next-page") == 6
    assert all(set(objects) == {"foreign-name"} for objects in k8s.objects.values())


def test_deleted_attempt_observation_has_no_side_effects(jobs_case):
    import uuid

    _, _, _, _, k8s, factory = jobs_case
    refit_jobs._reconcile(factory, k8s, uuid.uuid4())
    assert not any(k8s.objects.values())
