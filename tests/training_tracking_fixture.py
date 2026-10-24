"""Real, owned tracking prerequisites for model-custody integration tests."""

import uuid
from contextlib import contextmanager
from threading import RLock
from types import SimpleNamespace

from automl_api.services import training_tracking
from mlflow import MlflowClient
from mlflow.store.tracking.sqlalchemy_store import SqlAlchemyStore


@contextmanager
def owned_tracking(scope, client, headers, monkeypatch, tmp_path):
    uri = f"sqlite:///{tmp_path / 'model-tracking.db'}"
    native = MlflowClient(tracking_uri=uri)
    create_experiment = native.create_experiment
    monkeypatch.setattr(
        native,
        "create_experiment",
        lambda name: create_experiment(
            name, artifact_location=(tmp_path / "tracking-artifacts").as_uri()
        ),
    )
    monkeypatch.setattr(
        training_tracking, "get_settings", lambda: SimpleNamespace(mlflow_tracking_uri=uri)
    )

    def client_factory(*, tracking_uri):
        assert tracking_uri == uri
        return native

    monkeypatch.setattr(training_tracking, "MlflowClient", client_factory)
    known = {}
    lock = RLock()

    def ensure(role, *, attempt_id=scope[2]):
        key = attempt_id, role
        with lock:
            if key not in known:
                response = client.post(
                    f"/internal/attempts/{attempt_id}/training/tracking",
                    json={"run_id": str(scope[1]), "role": role},
                    headers=headers,
                )
                assert response.status_code == 200, response.text
                known[key] = response.json()["tracking_run_id"]
                from automl_api.models.runs import ModelRun
                from automl_api.models.workflows import WorkflowAttempt
                from automl_api.services.training_metadata import requires_metadata_receipts
                from sqlalchemy.orm import Session

                with Session(scope[0]) as db:
                    run = db.get(ModelRun, scope[1])
                    required = requires_metadata_receipts(
                        db, db.get(WorkflowAttempt, attempt_id), run
                    )
                    tags = (
                        {"task_type": run.task_type.value}
                        if role == "parent"
                        else {
                            "candidate_model": role.split(":", 1)[1],
                        }
                    )
                if required:
                    result = client.post(
                        f"/internal/attempts/{attempt_id}/training/metadata",
                        json={
                            "run_id": str(scope[1]),
                            "role": role,
                            "request_id": str(uuid.uuid4()),
                            "kind": "tags",
                            "data": tags,
                        },
                        headers=headers,
                    )
                    assert result.status_code == 200, result.text
            return known[key]

    try:
        parent_id = ensure("parent")
        yield SimpleNamespace(
            parent_id=parent_id, ensure=ensure, native=native, uri=uri, known=known
        )
    finally:
        engine = SqlAlchemyStore._engine_map.pop(uri, None)
        if engine is not None:
            engine.dispose()
        for suffix in ["", "-journal", "-wal", "-shm"]:
            (tmp_path / ("model-tracking.db" + suffix)).unlink(missing_ok=True)
