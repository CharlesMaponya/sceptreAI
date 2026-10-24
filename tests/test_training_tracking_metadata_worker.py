"""Declared metadata never uses the worker's native metadata client."""

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training import pipeline
from automl_api.training.attempt_rpc import AttemptControlUnavailable, AttemptRPC
from test_ray_pod_credentials import manifests as manifests
from test_ray_pod_credentials import stage as stage
from test_training_tracking_worker import remote_tracking as remote_tracking


@pytest.fixture
def remote_metadata(remote_tracking, monkeypatch):
    run, sql, native, _, start, state = remote_tracking
    monkeypatch.setenv("AUTOML_TRAINING_METADATA_PROTOCOL", "training-metadata-v1")
    active = MagicMock()
    active.__enter__.return_value = SimpleNamespace(info=SimpleNamespace(run_id="a" * 32))
    start.return_value = active
    original = AttemptRPC.request
    state["metadata"] = []

    def request(control, method, path, **kwargs):
        if method == "GET" and path.startswith("training/metadata/"):
            state.setdefault("metadata_wire", []).append((method, path))
            return state["metadata_reply"]
        if path != "training/metadata":
            return original(control, method, path, **kwargs)
        payload = kwargs["payload"]
        state["metadata"].append(payload)
        state.setdefault("metadata_wire", []).append((method, path))
        reply = dict(
            protocol="training-metadata-v1",
            attempt_id=str(control.attempt_id),
            project_id=str(run.project_id),
            run_id=str(run.id),
            role=payload["role"],
            audience=attempt_audience(control.attempt_id, "f" * 32),
            request_id=payload["request_id"],
            request_digest=canonical_request_hash(payload),
            tracking_run_id="a" * 32,
            command_id=str(uuid.uuid4()),
            committed=True,
        )

        reply.update(state.get("corrupt_metadata_reply", {}))
        state["metadata_reply"] = reply
        if state.get("lose_metadata_ack"):
            raise AttemptControlUnavailable("owned committed metadata response loss")
        return reply

    monkeypatch.setattr(AttemptRPC, "request", request)
    metadata_sdk = MagicMock(
        side_effect=AssertionError("declared worker opened native metric writer")
    )
    monkeypatch.setattr(pipeline.mlflow, "log_metric", metadata_sdk)
    return run, state, metadata_sdk


@pytest.mark.parametrize("role", ["parent", "candidate:Ridge", "failure:Ridge"])
def test_declared_metrics_do_not_use_native_worker_metadata(remote_metadata, role):
    run, state, native = remote_metadata
    parent = None if role == "parent" else "owned-parent"
    with pipeline._tracked_mlflow_run(run, role=role, run_name="owned", parent_run_id=parent):
        pipeline._log_metric_synchronously("rmse", 0.2)
    native.assert_not_called()
    assert len(state["metadata"]) == 1
    assert state["metadata"][0]["role"] == role


@pytest.mark.parametrize("kind", ["tags", "params"])
def test_declared_tags_and_params_use_the_active_role(remote_metadata, monkeypatch, kind):
    from automl_api.training import training_metadata

    run, state, _ = remote_metadata
    native = MagicMock(side_effect=AssertionError("native metadata writer reached"))
    monkeypatch.setattr(pipeline.mlflow, "set_tags" if kind == "tags" else "log_params", native)
    with pipeline._tracked_mlflow_run(run, role="parent", run_name="owned"):
        getattr(training_metadata, "log_" + kind)({"owned": "value"})
    native.assert_not_called()
    assert state["metadata"][0]["kind"] == kind


def test_lost_metadata_ack_uses_one_get_without_post_retry(remote_metadata):
    run, state, native = remote_metadata
    state["lose_metadata_ack"] = True
    with pipeline._tracked_mlflow_run(run, role="parent", run_name="owned"):
        pipeline._log_metric_synchronously("rmse", 0.2)
    assert [method for method, _ in state["metadata_wire"]] == ["POST", "GET"]
    native.assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("attempt_id", str(uuid.uuid4())),
        ("project_id", str(uuid.uuid4())),
        ("run_id", str(uuid.uuid4())),
        ("role", "failure:Ridge"),
        ("audience", "foreign"),
        ("request_id", str(uuid.uuid4())),
        ("request_digest", "f" * 64),
        ("tracking_run_id", "foreign"),
        ("committed", False),
        ("protocol", "unknown"),
    ],
)
def test_invalid_metadata_ack_never_falls_back_to_native(remote_metadata, field, value):
    run, state, native = remote_metadata
    state["corrupt_metadata_reply"] = {field: value}
    with pytest.raises(StaleFence):
        with pipeline._tracked_mlflow_run(run, role="parent", run_name="owned"):
            pipeline._log_metric_synchronously("rmse", 0.2)
    native.assert_not_called()


def test_declared_metadata_cannot_escape_its_owned_role(remote_metadata):
    _, state, native = remote_metadata
    with pytest.raises(StaleFence):
        pipeline._log_metric_synchronously("rmse", 0.2)
    assert state["metadata"] == []
    native.assert_not_called()


def test_unknown_metadata_protocol_is_rejected_before_tracking_creation(
    remote_metadata, monkeypatch
):
    run, state, native = remote_metadata
    monkeypatch.setenv("AUTOML_TRAINING_METADATA_PROTOCOL", "unknown")
    with pytest.raises(StaleFence):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="owned")
    assert state["calls"] == state["metadata"] == []
    native.assert_not_called()


def test_saved_worker_keeps_native_metric_semantics(remote_metadata, monkeypatch):
    _, state, native = remote_metadata
    monkeypatch.delenv("AUTOML_TRAINING_METADATA_PROTOCOL")
    native.side_effect = None
    pipeline._log_metric_synchronously("rmse", 0.2)
    native.assert_called_once_with("rmse", 0.2, step=0, synchronous=True)
    assert state["metadata"] == []


def test_candidate_mirror_uses_its_original_candidate_role(remote_metadata):
    run, state, _ = remote_metadata
    pipeline._mirror_candidate_evidence_to_parent(
        "owned-parent",
        "Ridge",
        {"rmse": 0.2},
        "a" * 32,
        "worker-cannot-choose-this",
        run_id=run.id,
    )
    assert state["metadata"][0]["role"] == "candidate:Ridge"
    assert state["metadata"][0]["kind"] == "mirror"
    assert state["metadata"][0]["data"] == {"metric_count": 1}


def test_v9_metadata_flag_only_training_compute(manifests, monkeypatch):
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
    submitter, head, worker = pods(build(credential_protocol="ray-training-metadata-v9"))
    assert "AUTOML_TRAINING_METADATA_PROTOCOL" not in env(submitter)
    for pod in (head, worker):
        if stage == "training_run":
            assert env(pod)["AUTOML_TRAINING_METADATA_PROTOCOL"]["value"] == "training-metadata-v1"
            assert env(pod)["AUTOML_TRAINING_TRACKING_PROTOCOL"]["value"] == "training-tracking-v1"
        else:
            assert "AUTOML_TRAINING_METADATA_PROTOCOL" not in env(pod)


def test_empty_parameter_logging_is_an_owned_noop(remote_metadata):
    from automl_api.training import training_metadata

    run, state, _ = remote_metadata
    with pipeline._tracked_mlflow_run(run, role="parent", run_name="owned"):
        training_metadata.log_params({})
    assert state["metadata"] == []


def test_parent_execution_routes_ordinary_metadata_and_keeps_artifact_logging(
    remote_metadata, monkeypatch
):
    run, state, native = remote_metadata
    run.dataset_version_id = uuid.uuid4()
    run.params = {}
    result = pipeline.TournamentResult(
        metrics={"rmse": 0.2},
        model=object(),
        params={"owned": True},
        leaderboard=[],
        primary_metric="rmse",
    )
    monkeypatch.setattr(pipeline, "training_context", lambda _: (run, object(), [], None))
    monkeypatch.setattr(
        pipeline, "_load_prepared_training_frames", lambda *args, **kwargs: ([], [])
    )
    monkeypatch.setattr(pipeline, "_fit_model", lambda *args: result)
    monkeypatch.setattr(pipeline, "_persist_training_success", lambda *args: True)
    tags = MagicMock(side_effect=AssertionError("worker-native parent tags reached"))
    params = MagicMock(side_effect=AssertionError("worker-native parent params reached"))
    monkeypatch.setattr(pipeline.mlflow, "set_tags", tags)
    monkeypatch.setattr(pipeline.mlflow, "log_params", params)
    artifact, model = MagicMock(), MagicMock()
    monkeypatch.setattr(pipeline.mlflow, "log_dict", artifact)
    monkeypatch.setattr(pipeline, "_log_sklearn_model", model)
    assert pipeline.execute_training_run(run.id) == {"rmse": 0.2}
    assert [item["kind"] for item in state["metadata"]] == ["tags", "params", "metrics"]
    tags.assert_not_called()
    params.assert_not_called()
    native.assert_not_called()
    artifact.assert_called_once()
    model.assert_called_once_with(result.model, artifact_path="model")


def test_failure_execution_uses_owned_failure_metadata(remote_metadata, monkeypatch):
    import time

    run, state, native = remote_metadata
    run.tags["candidate_parent_run_id"] = "owned-parent"
    monkeypatch.setattr(pipeline.mlflow, "active_run", lambda: None)
    tags = MagicMock(side_effect=AssertionError("worker-native failure tags reached"))
    monkeypatch.setattr(pipeline.mlflow, "set_tags", tags)
    entry = pipeline._failed_candidate(
        SimpleNamespace(name="Ridge", cost_tier="low"),
        time.monotonic(),
        ValueError("owned failure"),
        run,
    )
    assert entry["status"] == "failed"
    assert [item["kind"] for item in state["metadata"]] == ["tags", "metrics"]
    assert all(item["role"] == "failure:Ridge" for item in state["metadata"])
    tags.assert_not_called()
    native.assert_not_called()


def test_integer_metric_outside_native_double_range_is_rejected():
    from automl_api.schemas.training_metadata import TrainingMetadataRequest

    with pytest.raises(ValueError):
        TrainingMetadataRequest(
            run_id=uuid.uuid4(),
            role="parent",
            request_id=uuid.uuid4(),
            kind="metrics",
            data={"rmse": 10**400},
        )


def test_declared_params_preserve_native_string_values_for_numpy_and_infinity(remote_metadata):
    import numpy as np
    from automl_api.training import training_metadata

    run, state, _ = remote_metadata
    with pipeline._tracked_mlflow_run(run, role="parent", run_name="owned"):
        training_metadata.log_params({"count": np.int64(3), "bound": float("inf")})
    assert state["metadata"][0]["data"] == {"count": "3", "bound": "inf"}
