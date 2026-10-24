"""Produce an owned model through the real broker before registry verification."""

import io
from threading import local
from types import SimpleNamespace

import pytest
from automl_api.models.projects import Project
from automl_api.services import operations
from automl_api.training import pipeline
from sqlalchemy import event
from sqlalchemy.orm import Session, sessionmaker


@pytest.fixture
def qualified_model(model_api, monkeypatch, request):
    producer = model_api
    engine, run_id, _, fence, project_id = producer.scope
    params = getattr(getattr(request.node, "callspec", None), "params", {})
    payload = b"012345678" if params.get("fault") == "unknown_size" else b"0123456789"
    response = producer.upload(body=payload)
    assert response.status_code == 200, response.text
    value = response.json()
    entry = {
        "model": producer.name,
        "status": "succeeded",
        "metrics": {"rmse": 0.2},
        "model_artifact_uri": value["uri"],
        "model_artifact_sha256": value["sha256"],
        "mlflow_run_id": producer.ensure_tracking(),
    }
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(producer.scope[2]))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", fence)
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: Session(engine))
    pipeline._persist_partial_leaderboard(run_id, [entry], "rmse")
    with Session(engine) as db:
        user_id = db.get(Project, project_id).owner_id
    c = SimpleNamespace(
        engine=engine,
        factory=sessionmaker(bind=engine),
        project_id=project_id,
        user_id=user_id,
        run_id=run_id,
        producer=producer,
        entry=entry,
        uri=value["uri"],
        payload=payload,
        native_counts=[],
        state={},
        model_name=producer.name,
    )
    activity = local()

    def checkout(*_args):
        activity.connections = getattr(activity, "connections", 0) + 1

    def checkin(*_args):
        activity.connections -= 1

    event.listen(engine, "checkout", checkout)
    event.listen(engine, "checkin", checkin)

    def opened(uri, byte_range=None, *, version=None):
        c.native_counts.append(getattr(activity, "connections", 0))
        assert c.native_counts[-1] == 0 and uri == c.uri and version == "fixture-v1"
        assert byte_range is None
        if callback := c.state.get("during_exists"):
            callback()
        if not c.state.get("exists", True) or c.state.get("unavailable"):
            raise OSError("owned immutable model unavailable")
        source = io.BytesIO(c.state.get("body", payload))
        count = []

        class Stream:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                source.close()

            def read(self, size):
                assert 0 <= size <= 65536
                if not count:
                    count.append(True)
                    c.native_counts.append(getattr(activity, "connections", 0))
                    assert c.native_counts[-1] == 0
                    if callback := c.state.get("during_size"):
                        callback()
                if "size" in c.state and c.state["size"] != len(payload):
                    return b"x" * min(size, max(0, int(c.state["size"])))
                return source.read(size)

        return Stream()

    c.store = SimpleNamespace(
        size=lambda uri: len(payload),
        exists=lambda uri: True,
        open_stream=opened,
    )
    monkeypatch.setattr(operations, "get_object_store", lambda: c.store)
    try:
        yield c
    finally:
        event.remove(engine, "checkout", checkout)
        event.remove(engine, "checkin", checkin)
