"""Durable admission and generation-fenced deployment controls."""

import pytest
from test_deployment_lifecycle_identity import (
    bound_run as bound_run,
)
from test_deployment_lifecycle_identity import (
    capacity_client as capacity_client,
)
from test_deployment_lifecycle_identity import (
    committed_tenant as committed_tenant,
)
from test_deployment_lifecycle_identity import (
    context_api as context_api,
)
from test_deployment_lifecycle_identity import (
    control as control,
)
from test_deployment_lifecycle_identity import (
    deployment_object as deployment_object,
)
from test_deployment_lifecycle_identity import (
    model_api as model_api,
)
from test_deployment_lifecycle_identity import (
    native_lifecycle as native_lifecycle,
)
from test_deployment_lifecycle_identity import (
    phase1_engine as phase1_engine,
)
from test_deployment_lifecycle_identity import (
    qualified_model as qualified_model,
)
from test_deployment_lifecycle_identity import (
    selected_scope as selected_scope,
)
from test_deployment_lifecycle_identity import (
    serving_model as serving_model,
)
from urllib3.exceptions import ReadTimeoutError


@pytest.mark.parametrize("action", ["stop", "start", "cleanup"])
def test_control_admission_never_performs_native_mutation(native_lifecycle, action):
    c = native_lifecycle
    if action != "stop":
        control(c, "stop")
        c.native_mutations.clear()
    control(c, action)
    assert not c.native_mutations


@pytest.fixture
def controls(native_lifecycle, monkeypatch):
    import copy
    from types import SimpleNamespace

    from automl_api.services import deployment_controls
    from automl_api.workflow_reconciler import run_once
    from kubernetes.client import ApiException

    c = native_lifecycle
    c.control_state = {}
    c.control_attempts = []
    c.control_effects = []
    for value in c.native_resources.values():
        value["metadata"]["generation"] = 1
        value["status"] = {"observedGeneration": 1}

    def patch(**kwargs):
        body = copy.deepcopy(kwargs["body"])
        c.control_attempts.append((body, c.engine.pool.checkedout()))
        if callback := c.control_state.pop("before_patch", None):
            callback(body)
        value = c.native_resources["deployment"]
        if (
            body[0]["value"] != value["metadata"]["uid"]
            or body[1]["value"] != value["metadata"]["resourceVersion"]
        ):
            raise ApiException(status=409)
        if status := c.control_state.pop("patch_status", None):
            raise ApiException(status=status)
        old = value["spec"]["replicas"]
        for operation in body[2:]:
            if operation["path"] == "/spec/replicas":
                value["spec"]["replicas"] = operation["value"]
            else:
                key = (
                    operation["path"]
                    .removeprefix("/metadata/annotations/")
                    .replace("~1", "/")
                    .replace("~0", "~")
                )
                value["metadata"]["annotations"][key] = operation["value"]
        value["metadata"]["resourceVersion"] = str(int(value["metadata"]["resourceVersion"]) + 1)
        if value["spec"]["replicas"] != old:
            value["metadata"]["generation"] += 1
        value["status"]["observedGeneration"] = value["metadata"]["generation"]
        c.control_effects.append(copy.deepcopy(value))
        lost = c.control_state.pop("lose_ack", False)
        if lost:
            raise (
                lost
                if isinstance(lost, Exception)
                else OSError("Owned control reply withheld after acceptance")
            )
        return copy.deepcopy(value)

    c.kubernetes.apps.patch_namespaced_deployment = patch
    for kind, api in (("deployment", c.kubernetes.apps), ("service", c.kubernetes.core)):
        method = "delete_namespaced_" + kind
        original = getattr(api, method)

        def wrapped(*, body, _kind=kind, _original=original, **kwargs):
            c.control_attempts.append(("delete-" + _kind, c.engine.pool.checkedout()))
            current = c.native_resources.get(_kind)
            if (
                current is not None
                and body.preconditions.resource_version != current["metadata"]["resourceVersion"]
            ):
                raise ApiException(status=409)
            if _kind == "deployment" and c.control_state.get("hold_deletion"):
                current["metadata"]["deletionTimestamp"] = "2026-10-25T00:00:00Z"
                current["metadata"]["resourceVersion"] = str(
                    int(current["metadata"]["resourceVersion"]) + 1
                )
                return None
            return _original(body=body, **kwargs)

        setattr(api, method, wrapped)
    monkeypatch.setattr(deployment_controls, "KubernetesTrainingClient", lambda: c.kubernetes)
    c.dispatch = lambda: run_once(c.factory, worker_id="deployment-control-owner")

    def retry():
        from datetime import UTC, datetime, timedelta

        from automl_api.models.workflows import OutboxEntry
        from sqlalchemy import update

        with c.factory() as db, db.begin():
            db.execute(
                update(OutboxEntry)
                .where(
                    OutboxEntry.project_id == c.project_id,
                    OutboxEntry.topic == deployment_controls.TOPIC,
                )
                .values(available_at=datetime.now(UTC) - timedelta(seconds=1))
            )
        return c.dispatch()

    c.retry_control = retry

    def saved():
        from automl_api.models.runs import ModelRun

        with c.factory() as db:
            run = db.get(ModelRun, c.deployment_id)
            return SimpleNamespace(status=run.status, tags=copy.deepcopy(run.tags))

    c.saved = saved
    c.native_reads.clear()
    return c


def test_durable_stop_start_cleanup_cycle_is_database_free_during_native_calls(controls):
    from automl_api.models.enums import RunStatus
    from automl_api.services.deployment_objects import has_unretired_deployment_writes

    c = controls
    control(c, "stop")
    assert not c.control_attempts and not c.native_reads
    assert c.saved().status == RunStatus.CANCELLED
    assert c.dispatch() == 1 and c.saved().tags["desired_state"] == "deployment_stopped"
    control(c, "start")
    assert c.saved().status == RunStatus.QUEUED
    assert c.dispatch() == 1 and c.saved().status == RunStatus.RUNNING
    control(c, "stop")
    assert c.dispatch() == 1
    control(c, "cleanup")
    assert c.dispatch() == 1
    assert c.saved().tags["runtime_cleaned_at"] and not c.native_resources
    assert all(count == 0 for _, count in c.control_attempts) and set(
        count for _, count in c.native_reads
    ) == {0}
    with c.factory() as db:
        assert not has_unretired_deployment_writes(db, c.project_id, [c.deployment_id])
    before = list(c.control_attempts)
    assert c.dispatch() == 0 and c.dispatch() == 0 and before == c.control_attempts


@pytest.mark.parametrize(
    "reply_error",
    [OSError("Owned reply lost"), ReadTimeoutError(None, "owned", "Owned SDK read timeout")],
    ids=["os-error", "sdk-timeout"],
)
def test_lost_native_reply_recovers_generation_without_another_patch(controls, reply_error):
    from automl_api.models.enums import OutboxStatus
    from automl_api.models.workflows import OutboxEntry, WorkflowCommand
    from automl_api.services.deployment_controls import TOPIC
    from sqlalchemy import select

    c = controls
    control(c, "stop")
    with c.factory() as db, db.begin():
        entry = db.scalars(
            select(OutboxEntry).where(
                OutboxEntry.project_id == c.project_id, OutboxEntry.topic == TOPIC
            )
        ).one()
        db.get(WorkflowCommand, entry.command_id).max_retries = 1
    c.control_state["lose_ack"] = reply_error
    assert c.dispatch() == 1 and len(c.control_effects) == 1
    with c.factory() as db:
        entry = db.scalars(
            select(OutboxEntry).where(
                OutboxEntry.project_id == c.project_id, OutboxEntry.topic == TOPIC
            )
        ).one()
        assert entry.status == OutboxStatus.PENDING and entry.delivery_attempts == 0
        assert db.get(WorkflowCommand, entry.command_id).retry_count == 0
    assert c.retry_control() == 1 and len(c.control_attempts) == 1
    assert c.saved().tags["desired_state"] == "deployment_stopped"


def test_newer_admission_prevents_old_native_result_from_publishing(controls):
    from automl_api.models.enums import RunStatus

    c = controls
    control(c, "stop")
    c.control_state["before_patch"] = lambda _: control(c, "start")
    assert c.dispatch() == 1
    assert (
        c.saved().status == RunStatus.QUEUED
        and c.saved().tags["deployment_control"]["generation"] == 2
    )
    assert c.dispatch() == 1 and c.saved().status == RunStatus.RUNNING
    assert (
        c.native_resources["deployment"]["metadata"]["annotations"][
            "automl.platform/control-generation"
        ]
        == "2"
    )


def test_delayed_old_patch_cannot_override_newer_noop_scale_generation(controls):
    c = controls
    control(c, "stop")

    def newer(_):
        control(c, "start")
        assert c.dispatch() == 1

    c.control_state["before_patch"] = newer
    assert c.dispatch() == 1
    assert len(c.control_effects) == 1
    actual = c.native_resources["deployment"]
    assert (
        actual["spec"]["replicas"] == 1
        and actual["metadata"]["annotations"]["automl.platform/control-generation"] == "2"
    )
    assert c.retry_control() == 1
    assert c.saved().tags["desired_state"] == "deployment_active"


def test_cleanup_admission_irreversibly_rejects_restart(controls):
    from fastapi import HTTPException

    c = controls
    control(c, "stop")
    assert c.dispatch() == 1
    control(c, "cleanup")
    with pytest.raises(HTTPException) as failure:
        control(c, "start")
    assert failure.value.status_code == 409
    assert c.dispatch() == 1 and c.saved().tags["runtime_cleaned_at"]


def test_pending_pods_replay_only_observation_without_another_scale(controls):
    c = controls
    control(c, "stop")
    c.native_pods.append(object())
    assert c.dispatch() == 1 and len(c.control_effects) == 1
    assert c.saved().tags["desired_state"] == "deployment_stop_pending"
    c.native_pods.clear()
    assert c.retry_control() == 1 and len(c.control_attempts) == 1
    assert c.saved().tags["desired_state"] == "deployment_stopped"


def test_duplicate_stop_admission_creates_one_control_command(controls):
    from concurrent.futures import ThreadPoolExecutor

    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services.deployment_controls import OPERATION
    from sqlalchemy import select

    c = controls
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(control, c, "stop") for _ in range(2)]
        for future in futures:
            future.result(timeout=20)
    with c.factory() as db:
        rows = list(
            db.scalars(
                select(WorkflowCommand).where(
                    WorkflowCommand.project_id == c.project_id,
                    WorkflowCommand.operation == OPERATION,
                )
            )
        )
        assert len(rows) == 1 and rows[0].request_payload["generation"] == 1
    assert not c.control_attempts
    assert c.dispatch() == 1


def test_sql_backend_death_after_native_acceptance_recovers_original_generation(controls):
    from sqlalchemy import event, text

    c = controls
    control(c, "stop")
    killed = []

    def terminate(connection, _cursor, statement, parameters, _context, _many):
        if killed or not statement.lstrip().lower().startswith("update outbox_entries "):
            return
        values = parameters.values() if isinstance(parameters, dict) else parameters
        if not any(
            isinstance(getattr(value, "obj", None), dict) and "observations" in value.obj
            for value in values
        ):
            return
        pid = connection.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
        with c.engine.begin() as inspector:
            assert inspector.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
        killed.append(pid)

    event.listen(c.engine, "before_cursor_execute", terminate)
    try:
        assert c.dispatch() == 1 and len(killed) == 1 and len(c.control_effects) == 1
    finally:
        event.remove(c.engine, "before_cursor_execute", terminate)
    assert c.retry_control() == 1 and len(c.control_attempts) == 1
    assert c.saved().tags["desired_state"] == "deployment_stopped"


@pytest.mark.parametrize("same_owner", [False, True])
def test_native_fact_survives_takeover_but_old_delivery_cannot_acknowledge(controls, same_owner):
    from datetime import UTC, datetime, timedelta

    from automl_api.models.enums import OutboxStatus
    from automl_api.models.workflows import OutboxEntry
    from automl_api.services.deployment_controls import TOPIC, reconcile_control
    from sqlalchemy import select

    c = controls
    control(c, "stop")
    chosen = []
    owner = "deployment-control-owner" if same_owner else "successor-control-owner"

    def takeover(_):
        with c.factory() as db, db.begin():
            entry = db.scalars(
                select(OutboxEntry).where(
                    OutboxEntry.project_id == c.project_id, OutboxEntry.topic == TOPIC
                )
            ).one()
            entry.lease_owner = owner
            entry.lease_expires_at = datetime.now(UTC) + timedelta(minutes=2)
            entry.delivery_attempts += 1
            chosen.append(entry.id)

    c.control_state["before_patch"] = takeover
    assert c.dispatch() == 1 and len(c.control_effects) == 1
    with c.factory() as db:
        entry = db.get(OutboxEntry, chosen[0])
        assert entry.status == OutboxStatus.CLAIMED and entry.lease_owner == owner
        assert entry.payload["observations"]
    reconcile_control(c.factory, chosen[0], owner)
    assert c.saved().tags["desired_state"] == "deployment_stopped" and len(c.control_attempts) == 1


def test_failed_cas_replans_only_after_original_resource_version_is_fenced(controls):
    c = controls
    control(c, "stop")

    def status_update(_):
        value = c.native_resources["deployment"]
        value["metadata"]["resourceVersion"] = "2"

    c.control_state["before_patch"] = status_update
    assert c.dispatch() == 1 and not c.control_effects
    assert c.retry_control() == 1 and len(c.control_effects) == 1
    assert [body[1]["value"] for body, _ in c.control_attempts] == ["1", "2"]


@pytest.mark.parametrize("action", ["stop", "start"])
def test_failed_control_can_be_admitted_as_a_new_generation(controls, action):
    from automl_api.models.enums import CommandStatus, OutboxStatus
    from automl_api.models.workflows import OutboxEntry, WorkflowCommand
    from automl_api.services.deployment_controls import TOPIC
    from sqlalchemy import select

    c = controls
    if action == "start":
        control(c, "stop")
        assert c.dispatch() == 1
    control(c, action)
    with c.factory() as db, db.begin():
        entry = db.scalars(
            select(OutboxEntry).where(
                OutboxEntry.project_id == c.project_id,
                OutboxEntry.topic == TOPIC,
                OutboxEntry.status == OutboxStatus.PENDING,
            )
        ).one()
        db.get(WorkflowCommand, entry.command_id).max_retries = 1
        failed_command_id = entry.command_id
    c.control_state["patch_status"] = 503
    assert c.dispatch() == 1 and c.saved().tags["deployment_control"]["state"] == "failed"
    failed_generation = c.saved().tags["deployment_control"]["generation"]
    control(c, action)
    assert c.saved().tags["deployment_control"]["generation"] == failed_generation + 1
    assert c.dispatch() == 1
    assert c.saved().tags["deployment_control"]["state"] == "complete"
    with c.factory() as db:
        assert db.get(WorkflowCommand, failed_command_id).status == CommandStatus.FAILED


def test_cancelled_command_cannot_start_a_native_mutation(controls):
    from automl_api.models.enums import CommandStatus
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services.deployment_controls import OPERATION
    from sqlalchemy import select

    c = controls
    control(c, "stop")
    with c.factory() as db, db.begin():
        command = db.scalars(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == c.project_id, WorkflowCommand.operation == OPERATION
            )
        ).one()
        command.status = CommandStatus.CANCELLED
    assert c.dispatch() == 1 and not c.control_attempts


def test_revoked_restart_actor_cannot_issue_native_mutation(controls):
    from automl_api.models.iam import User

    c = controls
    control(c, "stop")
    assert c.dispatch() == 1
    control(c, "start")
    before = list(c.control_attempts)
    with c.factory() as db, db.begin():
        db.get(User, c.user_id).is_active = False
    assert c.dispatch() == 1 and c.control_attempts == before


@pytest.mark.parametrize("action,expected", [("start", "starting"), ("cleanup", "cleaning")])
def test_pending_controls_are_not_promoted_by_listing(controls, action, expected):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from automl_api.models.iam import User
    from automl_api.services import operations

    c = controls
    control(c, "stop")
    assert c.dispatch() == 1
    control(c, action)
    view = SimpleNamespace(
        settings=c.kubernetes.settings,
        model_deployment_state=Mock(return_value="ready"),
        model_deployment_urls=Mock(return_value={}),
    )
    with c.factory() as db, db.begin():
        rows = operations.list_model_deployments(db, db.get(User, c.user_id), c.project_id, view)
        assert [r.runtime_state for r in rows if r.run.id == c.deployment_id] == [expected]
    view.model_deployment_state.assert_not_called()


def test_cleanup_reconciles_a_terminating_deployment_without_rebinding(controls):
    c = controls
    control(c, "stop")
    assert c.dispatch() == 1
    control(c, "cleanup")
    c.control_state["hold_deletion"] = True
    assert c.dispatch() == 1
    assert c.native_resources["deployment"]["metadata"]["deletionTimestamp"]
    assert not c.saved().tags.get("runtime_cleaned_at")
    c.control_state["hold_deletion"] = False
    assert c.retry_control() == 1
    assert c.saved().tags["runtime_cleaned_at"] and not c.native_resources
