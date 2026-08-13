from __future__ import annotations

import asyncio
import inspect
import io
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from automl_api.api.routes import (
    auth,
    datasets,
    monitoring,
    operations,
    profiling,
    projects,
    training,
    validation,
)
from automl_api.models.enums import TaskType
from fastapi import Response
from starlette.requests import Request

ROUTE_MODULES = (
    auth,
    datasets,
    monitoring,
    operations,
    profiling,
    projects,
    training,
    validation,
)

SPECIAL_SERVICE_RESULTS = {
    "authenticate_user": SimpleNamespace(),
    "create_password_reset_token": "reset-token",
    "create_profiling_job": (MagicMock(), True),
    "create_project_share_link": (MagicMock(), "invite-token"),
    "governance_report_download": (b"report", "application/json", "report.json"),
    "list_dataset_versions": [],
    "list_drift_runs": [],
    "list_governance_reports": [],
    "list_model_deployments": [],
    "list_monitoring_metrics": [],
    "list_profile_jobs": [],
    "list_project_members": [],
    "list_project_datasets": [],
    "list_training_estimators": [],
    "list_training_runs": [],
    "list_visible_projects": [],
    "model_audit_document": (b"pdf", "application/pdf", "audit.pdf", "digest"),
    "upload_dataset_version": (SimpleNamespace(), SimpleNamespace()),
}


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/",
            "headers": [(b"user-agent", b"pytest")],
            "client": ("127.0.0.1", 12345),
            "query_string": b"",
            "server": ("testserver", 80),
            "scheme": "http",
        }
    )


def _schema_stub() -> MagicMock:
    stub = MagicMock()
    stub.model_validate.return_value = MagicMock()
    return stub


def _patch_boundaries(monkeypatch: pytest.MonkeyPatch, module: object) -> None:
    for name, value in vars(module).items():
        origin = getattr(value, "__module__", "")
        if origin.startswith("automl_api.services"):
            result = SPECIAL_SERVICE_RESULTS.get(name, MagicMock())
            replacement: MagicMock
            if inspect.iscoroutinefunction(value):
                replacement = AsyncMock(return_value=result)
            else:
                replacement = MagicMock(return_value=result)
            monkeypatch.setattr(module, name, replacement)
        elif origin.startswith("automl_api.schemas"):
            monkeypatch.setattr(module, name, _schema_stub())


def _argument(name: str) -> object:
    if name == "request":
        return _request()
    if name == "response":
        return Response()
    if name == "file":
        return SimpleNamespace(file=io.BytesIO(b"a,b\n1,2\n"), filename="data.csv")
    if name == "db":
        database = MagicMock()
        database.get.return_value = MagicMock()
        return database
    if name in {"current_user", "_current_user", "user"}:
        return MagicMock(id=uuid.uuid4())
    if name == "task_type":
        return TaskType.CLASSIFICATION
    if name == "output_format":
        return "json"
    if name in {"path", "model_name", "column", "dataset_name"}:
        return "value"
    if name == "tags":
        return "{}"
    if name == "token":
        return "token"
    if name in {"description"}:
        return None
    if name.endswith("_id") or name == "entry_id":
        return uuid.uuid4()
    if name == "payload":
        payload = MagicMock()
        payload.refresh_token = "refresh-token"
        return payload
    return MagicMock()


def _route_endpoints(module: object) -> list[object]:
    endpoints = []
    for route in module.router.routes:
        endpoint = route.endpoint
        if endpoint.__module__ == module.__name__ and endpoint not in endpoints:
            endpoints.append(endpoint)
    return endpoints


@pytest.mark.parametrize(
    "module",
    ROUTE_MODULES,
    ids=lambda module: module.__name__.rsplit(".", 1)[-1],
)
def test_http_route_handlers_execute_against_mocked_service_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    module: object,
) -> None:
    _patch_boundaries(monkeypatch, module)
    executed = []

    for endpoint in _route_endpoints(module):
        if endpoint is training.logs_websocket:
            continue
        arguments = {
            name: _argument(name)
            for name, parameter in inspect.signature(endpoint).parameters.items()
            if parameter.default is inspect.Parameter.empty or name in {"output_format"}
        }
        if module is profiling and hasattr(module.get_profiling_job, "return_value"):
            for lookup in (module.get_profiling_job, module.latest_profiling_job):
                job = lookup.return_value
                job.dataset_id = arguments.get("dataset_id")
                job.dataset_version_id = arguments.get("dataset_version_id")
        result = endpoint(**arguments)
        if inspect.isawaitable(result):
            result = asyncio.run(result)
        assert result is not None
        executed.append(endpoint.__name__)

    assert executed
