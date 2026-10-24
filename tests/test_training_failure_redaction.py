"""Failure details cannot expose credentials through console, tracking or metadata."""

import time
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import RunStatus
from automl_api.training import pipeline
from test_training_pipeline_runtime import _run, _Session

SECRET = "SYNTHETIC_PRIVATE_CREDENTIAL_098765"


@pytest.fixture
def failure_sinks(monkeypatch):
    tags = MagicMock()
    monkeypatch.setattr(pipeline.mlflow, "active_run", lambda: None)
    monkeypatch.setattr(pipeline.mlflow, "set_tags", tags)
    monkeypatch.setattr(pipeline, "_tracked_mlflow_run", lambda *args, **kwargs: nullcontext())
    metric = MagicMock()
    monkeypatch.setattr(pipeline, "_log_metric_synchronously", metric)
    return tags, metric


@pytest.mark.parametrize(
    "message",
    [
        f"database refused postgresql://worker:{SECRET}@database.example.test/app",
        f"provider refused https://objects.example.test/model?token={SECRET}",
        f"request failed authorization: Bearer {SECRET}",
        "x" * 970 + f' password="{SECRET}"',
        "x" * 220 + f' password="{SECRET}"',
    ],
)
def test_candidate_failure_redacts_before_every_sink_and_clip(failure_sinks, capsys, message):
    tags, metric = failure_sinks
    entry = pipeline._failed_candidate(
        SimpleNamespace(name="Ridge", cost_tier="fast"),
        time.monotonic(),
        RuntimeError(message),
        SimpleNamespace(tags={}),
    )
    sinks = {
        "console": capsys.readouterr().out,
        "tracking": tags.call_args.args[0]["failure_message"],
        "result": entry["error"],
    }
    for name, text in sinks.items():
        assert SECRET[:12] not in text, name
    assert entry["status"] == "failed" and len(entry["error"]) <= 1000
    assert len(sinks["tracking"]) <= 250
    metric.assert_called_once()


def test_legacy_failure_persistence_redacts_credentials(monkeypatch):
    monkeypatch.delenv("AUTOML_ATTEMPT_ID", raising=False)
    run = _run(status=RunStatus.RUNNING)
    db = _Session(run)
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: db)
    pipeline._mark_failed(run.id, RuntimeError(f"provider password={SECRET}"))
    assert SECRET not in run.failure_message
    assert run.status == RunStatus.FAILED and run.failure_code == "TRAINING_PIPELINE_FAILED"
    assert db.commits == 1


def test_ordinary_failure_diagnostics_remain_useful(failure_sinks, capsys):
    message = "invalid rows: feature x must be numeric"
    tags, _ = failure_sinks
    entry = pipeline._failed_candidate(
        SimpleNamespace(name="Ridge", cost_tier="fast"),
        time.monotonic(),
        ValueError(message),
        SimpleNamespace(tags={}),
    )
    assert entry["error"] == tags.call_args.args[0]["failure_message"] == message
    assert message in capsys.readouterr().out


def test_ray_wrapped_failure_metadata_is_redacted(monkeypatch):
    from automl_api.training import candidate_runtime
    from test_candidate_runtime import setup_runtime

    run, jobs, _, _, cancel = setup_runtime(monkeypatch)
    error = candidate_runtime.ray.exceptions.RayTaskError(
        "fit",
        f"provider password={SECRET}",
        RuntimeError(f"password={SECRET}"),
    )

    def get(reference):
        if reference == "b-ref":
            raise error
        return {"model": "A", "status": "succeeded"}

    monkeypatch.setattr(candidate_runtime.ray, "get", get)
    entries = list(candidate_runtime.results("supervised", jobs, run))
    assert entries[0][1]["status"] == "failed"
    assert SECRET not in entries[0][1]["error"]
    assert len(entries[0][1]["error"]) <= 2000
    assert entries[1][1]["status"] == "succeeded"
    cancel.assert_not_called()
