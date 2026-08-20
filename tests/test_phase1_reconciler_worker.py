from __future__ import annotations

import uuid
from contextlib import AbstractContextManager
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api import workflow_reconciler


class _Context(AbstractContextManager):
    def __init__(self, session) -> None:
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, *_args):
        return None


def test_run_once_claims_commits_and_reconciles_each_entry(monkeypatch) -> None:
    entry_ids = [uuid.uuid4(), uuid.uuid4()]
    claim_session = MagicMock()
    worker_sessions = [MagicMock(), MagicMock()]
    worker_sessions[0].get.return_value = SimpleNamespace(id=entry_ids[0])
    worker_sessions[1].get.return_value = None
    sessions = iter([claim_session, *worker_sessions])

    def factory():
        return _Context(next(sessions))
    monkeypatch.setattr(
        workflow_reconciler,
        "claim_outbox",
        lambda *_args, **_kwargs: [SimpleNamespace(id=value) for value in entry_ids],
    )
    reconcile = MagicMock()
    monkeypatch.setattr(workflow_reconciler, "reconcile_entry", reconcile)

    assert workflow_reconciler.run_once(factory, worker_id="worker") == 2
    claim_session.commit.assert_called_once()
    reconcile.assert_called_once_with(
        worker_sessions[0], worker_sessions[0].get.return_value, worker_id="worker"
    )
    worker_sessions[0].commit.assert_called_once()
    worker_sessions[1].commit.assert_not_called()


def test_run_once_commits_requeue_after_handler_failure(monkeypatch) -> None:
    entry_id = uuid.uuid4()
    claim_session = MagicMock()
    worker_session = MagicMock()
    worker_session.get.return_value = SimpleNamespace(id=entry_id)
    sessions = iter([claim_session, worker_session])
    monkeypatch.setattr(
        workflow_reconciler,
        "claim_outbox",
        lambda *_args, **_kwargs: [SimpleNamespace(id=entry_id)],
    )
    monkeypatch.setattr(
        workflow_reconciler,
        "reconcile_entry",
        MagicMock(side_effect=RuntimeError("external unavailable")),
    )

    assert workflow_reconciler.run_once(lambda: _Context(next(sessions)), worker_id="worker") == 1
    worker_session.commit.assert_called_once()


def test_observe_once_commits_success_and_rolls_back_failure(monkeypatch) -> None:
    session = MagicMock()
    observer = MagicMock(return_value=2)
    monkeypatch.setattr(workflow_reconciler, "observe_training_ray_jobs", observer)
    k8s = MagicMock()

    assert workflow_reconciler.observe_once(lambda: _Context(session), k8s=k8s) == 2
    observer.assert_called_once_with(session, k8s)
    session.commit.assert_called_once()

    session.reset_mock()
    observer.side_effect = RuntimeError("observer unavailable")
    assert workflow_reconciler.observe_once(lambda: _Context(session), k8s=k8s) == 0
    session.rollback.assert_called_once()


def test_cleanup_uploads_once_counts_every_cleanup_class_and_rolls_back(monkeypatch) -> None:
    session = MagicMock()
    cleanup = MagicMock(
        return_value={"expired": 1, "completed_deleted": 2, "quarantine_deleted": 3}
    )
    monkeypatch.setattr(workflow_reconciler, "cleanup_abandoned_uploads", cleanup)

    assert workflow_reconciler.cleanup_uploads_once(lambda: _Context(session)) == 6
    cleanup.assert_called_once_with(session)
    session.commit.assert_called_once()

    session.reset_mock()
    cleanup.side_effect = RuntimeError("storage unavailable")
    assert workflow_reconciler.cleanup_uploads_once(lambda: _Context(session)) == 0
    session.rollback.assert_called_once()


def test_main_uses_explicit_worker_identity_and_bounded_poll_interval(monkeypatch) -> None:
    monkeypatch.setenv("RECONCILER_WORKER_ID", "worker-explicit")
    monkeypatch.setenv("RECONCILER_POLL_SECONDS", "0")
    factory = MagicMock()
    monkeypatch.setattr(workflow_reconciler, "get_session_factory", lambda: factory)
    monkeypatch.setattr(workflow_reconciler, "KubernetesTrainingClient", MagicMock())
    monkeypatch.setattr(workflow_reconciler, "observe_once", lambda *_args, **_kwargs: 0)
    processed = iter([1, 0])
    monkeypatch.setattr(
        workflow_reconciler,
        "run_once",
        lambda received_factory, *, worker_id: (
            next(processed)
            if received_factory is factory and worker_id == "worker-explicit"
            else pytest.fail("main passed an unexpected reconciler identity")
        ),
    )
    monkeypatch.setattr(
        workflow_reconciler.time,
        "sleep",
        MagicMock(side_effect=RuntimeError("stop loop")),
    )

    with pytest.raises(RuntimeError, match="stop loop"):
        workflow_reconciler.main()

    workflow_reconciler.time.sleep.assert_called_once_with(0.1)


def test_main_derives_worker_identity_when_not_configured(monkeypatch) -> None:
    monkeypatch.delenv("RECONCILER_WORKER_ID", raising=False)
    monkeypatch.setenv("RECONCILER_POLL_SECONDS", "2")
    monkeypatch.setattr(workflow_reconciler.socket, "gethostname", lambda: "reconciler-host")
    monkeypatch.setattr(
        workflow_reconciler.uuid,
        "uuid4",
        lambda: SimpleNamespace(hex="12345678abcdef"),
    )
    factory = MagicMock()
    monkeypatch.setattr(workflow_reconciler, "get_session_factory", lambda: factory)
    monkeypatch.setattr(workflow_reconciler, "KubernetesTrainingClient", MagicMock())
    monkeypatch.setattr(workflow_reconciler, "observe_once", lambda *_args, **_kwargs: 0)

    def stop(received_factory, *, worker_id):
        assert received_factory is factory
        assert worker_id == "reconciler-host-12345678"
        raise RuntimeError("stop loop")

    monkeypatch.setattr(workflow_reconciler, "run_once", stop)
    with pytest.raises(RuntimeError, match="stop loop"):
        workflow_reconciler.main()
