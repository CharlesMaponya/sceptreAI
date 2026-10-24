"""Declared model persistence never opens ambient provider storage."""

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.services.workflow_state import StaleFence
from automl_api.training import pipeline
from automl_api.training.attempt_rpc import AttemptControlUnavailable, AttemptRPC


@pytest.fixture
def remote_model(monkeypatch, tmp_path):
    run = SimpleNamespace(id=uuid.uuid4(), project_id=uuid.uuid4())
    attempt, candidate = uuid.uuid4(), uuid.uuid4()
    token = tmp_path / "identity"
    token.write_text("fixture-token")
    for key, value in {
        "AUTOML_MODEL_OUTPUT_PROTOCOL": "training-model-output-v1",
        "AUTOML_ATTEMPT_ID": str(attempt),
        "AUTOML_FENCING_TOKEN": "f" * 32,
        "AUTOML_PROJECT_ID": str(run.project_id),
        "AUTOML_IDENTITY_TOKEN_FILE": str(token),
        "ATTEMPT_CONTROL_BASE_URL": "https://control.example.test/internal/attempts",
    }.items():
        monkeypatch.setenv(key, value)
    storage = MagicMock(side_effect=AssertionError("declared model writer opened worker storage"))
    monkeypatch.setattr(pipeline, "get_object_store", storage)
    state = {"calls": [], "receipt": None, "lose_ack": False}

    def request(control, method, path, **kwargs):
        assert control.attempt_id == attempt
        state["calls"].append((method, path))
        if method == "PUT":
            payload = kwargs["content"]
            digest = hashlib.sha256(payload).hexdigest()
            state["receipt"] = {
                "grant_id": str(uuid.uuid4()),
                "grant_digest": "a" * 64,
                "expires_at": (datetime.now(UTC) + timedelta(seconds=45)).isoformat(),
                "uri": f"s3://owned/projects/{run.project_id}/runs/{run.id}/attempts/{attempt}/models/{candidate}/{digest}.joblib",
                "sha256": digest,
                "byte_size": len(payload),
                "object_version": "native-v1",
                "project_id": str(run.project_id),
                "run_id": str(run.id),
                "attempt_id": str(attempt),
                "candidate_id": str(candidate),
                "candidate_name": "Ridge",
            }
            if state["lose_ack"]:
                raise AttemptControlUnavailable("owned response loss")
        return state["receipt"]

    monkeypatch.setattr(AttemptRPC, "request", request)
    return run, storage, state


def test_declared_model_serialization_uses_control_without_storage(remote_model):
    run, storage, state = remote_model
    uri, digest = pipeline._persist_candidate_model(run, "Ridge", {"model": "fixture"})
    assert (uri, digest) == (state["receipt"]["uri"], state["receipt"]["sha256"])
    assert [method for method, _ in state["calls"]] == ["PUT"]
    storage.assert_not_called()


def test_lost_model_upload_ack_uses_only_receipt_lookup(remote_model):
    run, storage, state = remote_model
    state["lose_ack"] = True
    uri, digest = pipeline._persist_candidate_model(run, "Ridge", {"model": "fixture"})
    assert (uri, digest) == (state["receipt"]["uri"], state["receipt"]["sha256"])
    assert [method for method, _ in state["calls"]] == ["PUT", "GET"]
    storage.assert_not_called()


def test_invalid_declared_protocol_never_falls_back(remote_model, monkeypatch):
    monkeypatch.setenv("AUTOML_MODEL_OUTPUT_PROTOCOL", "unknown")
    with pytest.raises(StaleFence):
        pipeline._persist_candidate_model(remote_model[0], "Ridge", {"model": "fixture"})
    remote_model[1].assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("run_id", str(uuid.UUID(int=1))),
        ("attempt_id", str(uuid.UUID(int=2))),
        ("project_id", str(uuid.UUID(int=3))),
        ("candidate_name", "Other"),
        ("sha256", "0" * 64),
        ("byte_size", 1),
        ("object_version", ""),
        ("uri", "s3://unowned/other"),
        ("extra", "unapproved"),
        ("expires_at", "2000-01-01T00:00:00Z"),
    ],
)
def test_model_receipt_must_match_its_closed_declaration(remote_model, monkeypatch, field, value):
    original = AttemptRPC.request

    def request(*args, **kwargs):
        reply = original(*args, **kwargs)
        return {**reply, field: value}

    monkeypatch.setattr(AttemptRPC, "request", request)
    with pytest.raises(StaleFence):
        pipeline._persist_candidate_model(remote_model[0], "Ridge", {"model": "fixture"})
    assert [method for method, _ in remote_model[2]["calls"]] == ["PUT"]
    remote_model[1].assert_not_called()


def test_saved_v4_manifest_has_context_but_no_model_protocol():
    from unittest.mock import patch

    from automl_api.core.config import Settings
    from automl_api.services import reconciler
    from test_phase1_reconciler import _attempt, _run
    from test_ray_pod_credentials import env, pods

    run = _run()
    with patch.object(
        reconciler,
        "get_settings",
        return_value=Settings(
            attempt_control_base_url="https://control.example.test/internal/attempts",
        ),
    ):
        old = reconciler.build_ray_job_manifest(
            run, _attempt(run), name="saved-v4", credential_protocol="ray-training-context-v4"
        )
        new = reconciler.build_ray_job_manifest(
            run, _attempt(run), name="saved-v5", credential_protocol="ray-model-output-v5"
        )
    assert all("AUTOML_MODEL_OUTPUT_PROTOCOL" not in env(pod) for pod in pods(old))
    submitter, head, worker = pods(new)
    assert "AUTOML_MODEL_OUTPUT_PROTOCOL" not in env(submitter)
    for pod in (head, worker):
        assert env(pod)["AUTOML_MODEL_OUTPUT_PROTOCOL"]["value"] == "training-model-output-v1"
    assert env(pods(old)[1])["AUTOML_TRAINING_CONTEXT_PROTOCOL"]["value"] == "training-context-v1"
