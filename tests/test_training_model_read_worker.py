"""A declared model reader must not open worker storage or deserialize unverified bytes."""

import pytest
from automl_api.services.workflow_state import StaleFence
from automl_api.training import pipeline
from test_candidate_recovery import checkpoint
from test_ray_pod_credentials import manifests as manifests
from test_ray_pod_credentials import stage as stage


def test_declared_reader_never_opens_worker_storage(monkeypatch):
    run, _, entry = checkpoint(monkeypatch)
    monkeypatch.setenv("AUTOML_TRAINING_MODEL_READ_PROTOCOL", "unknown")

    def forbidden():
        raise AssertionError("declared model reader opened worker storage")

    monkeypatch.setattr(pipeline, "get_object_store", forbidden)
    with pytest.raises(StaleFence):
        pipeline._restore_candidate(run, entry)


@pytest.fixture
def read_case(monkeypatch, tmp_path):
    import json
    import uuid
    from datetime import UTC, datetime, timedelta
    from unittest.mock import MagicMock
    from urllib.parse import urlencode

    import httpx
    from automl_api.services.attempt_identity import attempt_audience
    from automl_api.services.workflow_state import canonical_request_hash
    from automl_api.training import object_capabilities

    run, store, entry = checkpoint(monkeypatch)
    payload = store.read_bytes.return_value
    attempt, fence = uuid.uuid4(), "f" * 32
    for key, value in {
        "AUTOML_TRAINING_MODEL_READ_PROTOCOL": "training-model-read-v1",
        "AUTOML_ATTEMPT_ID": str(attempt),
        "AUTOML_FENCING_TOKEN": fence,
        "AUTOML_PROJECT_ID": str(run.project_id),
        "ATTEMPT_CONTROL_BASE_URL": "https://control.invalid/internal/attempts",
        "AUTOML_IDENTITY_TOKEN_FILE": str(tmp_path / "token"),
    }.items():
        monkeypatch.setenv(key, value)
    (tmp_path / "token").write_text("owned-token")
    native = MagicMock(side_effect=AssertionError("declared model reader opened worker storage"))
    monkeypatch.setattr(pipeline, "get_object_store", native)
    actual_client = httpx.Client
    state = {"fault": None, "rpc": [], "reads": []}

    class Bytes(httpx.SyncByteStream):
        def __iter__(self):
            body = state.get("body", payload)
            yield body

    def handler(request):
        if request.url.host == "control.invalid":
            assert request.headers["authorization"] == "Bearer owned-token"
            state["rpc"].append((request.method, request.url.path))
            assert request.method == "POST" and request.url.path.endswith("/training/models/read")
            if state["fault"] == "lost_ack":
                raise httpx.ReadError("owned lost URL acknowledgement")
            body = json.loads(request.content)
            assert set(body) == {"request_id", "run_id", "candidate_name"}
            reply = {
                "protocol": "training-model-read-v1",
                "grant_id": str(uuid.uuid4()),
                "grant_digest": "a" * 64,
                "expires_at": (datetime.now(UTC) + timedelta(seconds=50)).isoformat(),
                "byte_limit": len(payload),
                "sha256": entry["model_artifact_sha256"],
                "url": "https://objects.invalid/model?"
                + urlencode(
                    {
                        "X-Amz-Date": datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
                        "X-Amz-Expires": "30",
                        "versionId": "original-version",
                    }
                ),
                "object_version": "original-version",
                "project_id": str(run.project_id),
                "attempt_id": str(attempt),
                "run_id": str(run.id),
                "candidate_name": entry["model"],
                "request_id": body["request_id"],
                "request_digest": canonical_request_hash(body),
                "audience": attempt_audience(attempt, fence),
                "uri": entry["model_artifact_uri"],
            }
            reply.update(state.get("reply_change", {}))
            return httpx.Response(200, json=reply)
        assert request.url.host == "objects.invalid" and request.method == "GET"
        assert "authorization" not in request.headers and "owned-token" not in str(request.url)
        assert request.headers["accept-encoding"] == "identity"
        state["reads"].append(request)
        headers = {"x-amz-version-id": "original-version"}
        headers.update(state.get("headers", {}))
        return httpx.Response(state.get("status", 200), headers=headers, stream=Bytes())

    def client(**kwargs):
        assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
        assert kwargs["verify"].verify_mode != 0
        return actual_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(object_capabilities.httpx, "Client", client)
    return run, entry, payload, native, state


def test_declared_model_read_verifies_then_restores_without_storage(read_case):
    run, entry, _, native, state = read_case
    result = pipeline._restore_candidate(run, entry)
    assert result["_model"] == {"saved": True} and result["resumed"] is True
    native.assert_not_called()
    assert len(state["rpc"]) == len(state["reads"]) == 1


@pytest.mark.parametrize(
    "field",
    [
        "protocol",
        "project_id",
        "attempt_id",
        "run_id",
        "candidate_name",
        "request_id",
        "request_digest",
        "audience",
        "uri",
        "sha256",
        "object_version",
        "url",
        "byte_limit",
    ],
)
def test_changed_read_ack_fails_before_deserialization(read_case, monkeypatch, field):
    import uuid
    from unittest.mock import MagicMock

    run, entry, _, native, state = read_case
    values = {
        "protocol": "unknown",
        "project_id": str(uuid.uuid4()),
        "attempt_id": str(uuid.uuid4()),
        "run_id": str(uuid.uuid4()),
        "candidate_name": "foreign",
        "request_id": str(uuid.uuid4()),
        "request_digest": "b" * 64,
        "audience": "foreign",
        "uri": "s3://foreign/model",
        "sha256": "b" * 64,
        "object_version": None,
        "url": None,
        "byte_limit": 5_000_000_001,
    }
    state["reply_change"] = {field: values[field]}
    load = MagicMock()
    monkeypatch.setattr(pipeline.joblib, "load", load)
    with pytest.raises(StaleFence):
        pipeline._restore_candidate(run, entry)
    load.assert_not_called()
    native.assert_not_called()
    assert len(state["rpc"]) == 1


@pytest.mark.parametrize(
    "fault", ["hash", "short", "long", "version", "range", "encoding", "length"]
)
def test_invalid_native_bytes_never_reach_deserializer(read_case, monkeypatch, fault):
    from unittest.mock import MagicMock

    run, entry, payload, native, state = read_case
    if fault in {"hash", "short", "long"}:
        state["body"] = {
            "hash": b"x" * len(payload),
            "short": payload[:-1],
            "long": payload + b"x",
        }[fault]
    else:
        state["headers"] = {
            "version": {"x-amz-version-id": "another-version"},
            "range": {"content-range": f"bytes 0-2/{len(payload)}"},
            "encoding": {"content-encoding": "gzip"},
            "length": {"content-length": str(len(payload) + 1)},
        }[fault]
    load = MagicMock()
    monkeypatch.setattr(pipeline.joblib, "load", load)
    with pytest.raises(StaleFence):
        pipeline._restore_candidate(run, entry)
    load.assert_not_called()
    native.assert_not_called()


def test_lost_model_read_ack_is_not_retried(read_case):
    run, entry, _, native, state = read_case
    state["fault"] = "lost_ack"
    with pytest.raises(StaleFence):
        pipeline._restore_candidate(run, entry)
    assert len(state["rpc"]) == 1 and state["reads"] == []
    native.assert_not_called()


def test_saved_worker_keeps_native_model_read(monkeypatch):
    monkeypatch.delenv("AUTOML_TRAINING_MODEL_READ_PROTOCOL", raising=False)
    run, store, entry = checkpoint(monkeypatch)
    assert pipeline._restore_candidate(run, entry)["_model"] == {"saved": True}
    store.read_bytes.assert_called_once_with(entry["model_artifact_uri"])


@pytest.mark.parametrize("value", ["", "unknown"])
def test_invalid_declared_read_protocol_fails_without_http(read_case, monkeypatch, value):
    run, entry, _, native, state = read_case
    monkeypatch.setenv("AUTOML_TRAINING_MODEL_READ_PROTOCOL", value)
    with pytest.raises(StaleFence):
        pipeline._restore_candidate(run, entry)
    assert state["rpc"] == state["reads"] == []
    native.assert_not_called()


@pytest.mark.parametrize("fault", ["project", "missing", "partial"])
def test_invalid_model_read_context_fails_before_http(read_case, monkeypatch, fault):
    import uuid

    run, entry, _, native, state = read_case
    if fault == "project":
        monkeypatch.setenv("AUTOML_PROJECT_ID", str(uuid.uuid4()))
    else:
        monkeypatch.delenv("AUTOML_ATTEMPT_ID")
        if fault == "missing":
            monkeypatch.delenv("AUTOML_FENCING_TOKEN")
    with pytest.raises(StaleFence):
        pipeline._restore_candidate(run, entry)
    assert state["rpc"] == state["reads"] == []
    native.assert_not_called()


def test_new_model_read_marker_is_head_only_and_keeps_other_protocols(manifests, monkeypatch):
    from dataclasses import replace

    from automl_api.services import reconciler
    from test_ray_pod_credentials import env, pods

    build, _, stage, settings = manifests
    monkeypatch.setattr(
        reconciler,
        "get_settings",
        lambda: replace(
            settings, attempt_control_base_url="https://control.example.test/internal/attempts"
        ),
    )
    submitter, head, worker = pods(build(credential_protocol="ray-training-model-read-v10"))
    assert "AUTOML_TRAINING_MODEL_READ_PROTOCOL" not in env(submitter)
    assert "AUTOML_TRAINING_MODEL_READ_PROTOCOL" not in env(worker)
    if stage == "training_run":
        assert env(head)["AUTOML_TRAINING_MODEL_READ_PROTOCOL"]["value"] == "training-model-read-v1"
        for pod in (head, worker):
            assert env(pod)["AUTOML_TRAINING_METADATA_PROTOCOL"]["value"] == "training-metadata-v1"
            assert env(pod)["AUTOML_TRAINING_TRACKING_PROTOCOL"]["value"] == "training-tracking-v1"
    else:
        assert "AUTOML_TRAINING_MODEL_READ_PROTOCOL" not in env(head)
