"""Declared remote lifecycle workers must never use their legacy SQL connection."""

import sys
import uuid
from dataclasses import replace
from unittest.mock import MagicMock

import pytest
from automl_api.models.runs import ModelRun
from automl_api.services.attempt_identity import attempt_audience
from automl_api.training import pipeline, worker
from automl_api.training.attempt_rpc import AttemptRPC
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_ray_pod_credentials import env, pods
from test_ray_pod_credentials import manifests as manifests
from test_ray_pod_credentials import stage as stage
from test_training_publication_postgres import publication as publication


@pytest.fixture
def remote_worker(publication, monkeypatch, tmp_path):
    engine, run_id, attempt_id, fence, _ = publication
    with Session(engine) as db:
        project_id = db.get(ModelRun, run_id).project_id
    token = tmp_path / "token"
    token.write_text("owned-fixture-token")
    monkeypatch.setenv("AUTOML_PARENT_CONTROL_PROTOCOL", "parent-control-v1")
    monkeypatch.setenv("ATTEMPT_CONTROL_BASE_URL", "https://control.example.test/internal/attempts")
    monkeypatch.setenv("AUTOML_IDENTITY_TOKEN_FILE", str(token))
    monkeypatch.setenv("AUTOML_PROJECT_ID", str(project_id))
    sql = MagicMock(side_effect=AssertionError("remote lifecycle opened worker SQL"))
    monkeypatch.setattr(worker, "get_session_factory", sql)
    calls = []
    reply = {
        "protocol": "parent-control-v1",
        "attempt_id": str(attempt_id),
        "run_id": str(run_id),
        "project_id": str(project_id),
        "generation": 1,
        "run_kind": "training",
        "audience": attempt_audience(attempt_id, fence),
        "has_result": False,
        "completed": False,
        "changed": True,
    }

    def request(control, method, path, **kwargs):
        calls.append((method, path, kwargs))
        assert control.attempt_id == attempt_id
        return dict(reply)

    monkeypatch.setattr(AttemptRPC, "request", request)
    return run_id, (attempt_id, fence), uuid.uuid4().hex, reply, calls, sql


def test_remote_parent_start_uses_control_instead_of_sql(remote_worker):
    run_id, context, owner, _, calls, sql = remote_worker
    assert worker._begin_fenced_attempt(run_id, worker_id=owner) == context
    assert calls[-1][1] == "parent/start"
    sql.assert_not_called()


def test_remote_parent_heartbeat_uses_control_instead_of_sql(remote_worker):
    run_id, context, owner, _, calls, sql = remote_worker
    worker._heartbeat_fenced_attempt(run_id, context, owner)
    assert calls[-1][1] == "parent/heartbeat"
    sql.assert_not_called()


def test_remote_parent_completion_uses_control_instead_of_sql(remote_worker):
    run_id, context, owner, reply, calls, sql = remote_worker
    reply.update(has_result=True, completed=True)
    worker._complete_fenced_attempt(run_id, context, worker_id=owner)
    assert calls[-1][1] == "parent/complete"
    sql.assert_not_called()


def test_remote_parent_receipt_uses_control_instead_of_sql(remote_worker):
    run_id, context, _, reply, calls, sql = remote_worker
    reply.update(has_result=True, completed=True)
    assert worker._resume_committed_result(run_id, context)
    assert calls[-1][1] == "parent/receipt"
    sql.assert_not_called()


def test_remote_parent_failure_uses_control_instead_of_sql(remote_worker):
    run_id, context, owner, _, calls, sql = remote_worker
    assert worker._fail_fenced_attempt(run_id, context, RuntimeError("fit failed"), worker_id=owner)
    assert calls[-1][1] == "parent/failure"
    sql.assert_not_called()


def test_remote_parent_main_fetches_run_kind_without_worker_sql(remote_worker, monkeypatch):
    run_id, _, _, reply, calls, sql = remote_worker
    reply.update(has_result=True, completed=True)
    monkeypatch.setattr(
        sys,
        "argv",
        ["worker", "--run-id", str(run_id), "--attempt-protocol", "training-parameters-v1"],
    )
    monkeypatch.setattr(worker, "_enable_rapids_accelerator", lambda: False)
    compute = MagicMock(side_effect=AssertionError("committed result was recomputed"))
    monkeypatch.setattr(pipeline, "execute_training_run", compute)
    worker.main()
    assert calls[0][1] == "parent/context"
    assert calls[-1][1] == "parent/receipt"
    sql.assert_not_called()
    compute.assert_not_called()


@pytest.mark.parametrize(
    "invalid",
    ["attempt", "run", "project", "audience", "protocol", "generation", "completion", "extra"],
)
def test_remote_parent_rejects_changed_response_without_sql(remote_worker, invalid):
    run_id, _, owner, reply, _, sql = remote_worker
    if invalid in {"attempt", "run", "project"}:
        reply[invalid + "_id"] = str(uuid.uuid4())
    elif invalid == "audience":
        reply["audience"] = "foreign"
    elif invalid == "protocol":
        reply["protocol"] = "unknown"
    elif invalid == "generation":
        reply["generation"] = 0
    elif invalid == "completion":
        reply["completed"] = True
    else:
        reply["object_uri"] = "s3://forbidden"
    with pytest.raises(worker.StaleFence):
        worker._begin_fenced_attempt(run_id, worker_id=owner)
    sql.assert_not_called()


@pytest.mark.parametrize("protocol", ["", "unknown"])
def test_unknown_parent_transport_declaration_never_falls_back_to_sql(
    remote_worker, monkeypatch, protocol
):
    run_id, _, owner, _, calls, sql = remote_worker
    monkeypatch.setenv("AUTOML_PARENT_CONTROL_PROTOCOL", protocol)
    with pytest.raises(worker.StaleFence):
        worker._begin_fenced_attempt(run_id, worker_id=owner)
    assert calls == []
    sql.assert_not_called()


def test_unavailable_parent_transport_never_falls_back_or_discloses_error(
    remote_worker, monkeypatch
):
    run_id, _, owner, _, _, sql = remote_worker

    def unavailable(*args, **kwargs):
        raise RuntimeError("https://private.example/?token=do-not-disclose")

    monkeypatch.setattr(AttemptRPC, "request", unavailable)
    with pytest.raises(worker.StaleFence) as caught:
        worker._begin_fenced_attempt(run_id, worker_id=owner)
    assert "do-not-disclose" not in str(caught.value)
    sql.assert_not_called()


@pytest.mark.parametrize("configured", [False, True])
def test_only_new_configured_training_head_selects_parent_rpc(manifests, configured, monkeypatch):
    build, _, stage, settings = manifests
    from automl_api.services import reconciler

    settings = replace(
        settings,
        attempt_control_base_url="https://control.example.test/internal/attempts"
        if configured
        else "",
    )
    monkeypatch.setattr(reconciler, "get_settings", lambda: settings)
    submitter, head, compute = pods(build(credential_protocol="ray-parent-control-v2"))
    assert "AUTOML_PARENT_CONTROL_PROTOCOL" not in env(submitter)
    assert "AUTOML_PARENT_CONTROL_PROTOCOL" not in env(compute)
    assert env(head).get("AUTOML_PARENT_CONTROL_PROTOCOL") == (
        {"name": "AUTOML_PARENT_CONTROL_PROTOCOL", "value": "parent-control-v1"}
        if configured and stage == "training_run"
        else None
    )
    assert all(
        "AUTOML_PARENT_CONTROL_PROTOCOL" not in env(pod)
        for pod in pods(build(credential_protocol="ray-pod-env-v1"))
    )
