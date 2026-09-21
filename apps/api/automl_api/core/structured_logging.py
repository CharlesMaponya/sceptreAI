"""Structured JSON logging with correlation identifiers (task-index P7-W01).

Every log record carries request, user, project, dataset, run, candidate,
database attempt, and Ray cluster/job identifiers when they are present on
the logging context. High-cardinality identifiers stay out of metric labels
(P7-W20) — they belong here and in traces instead.
"""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

# Correlation context. Middleware and workers set these; the JSON formatter
# merges them into every record emitted inside the context.
request_id: ContextVar[str | None] = ContextVar("request_id", default=None)
user_id: ContextVar[str | None] = ContextVar("user_id", default=None)
project_id: ContextVar[str | None] = ContextVar("project_id", default=None)
dataset_version_id: ContextVar[str | None] = ContextVar("dataset_version_id", default=None)
run_id: ContextVar[str | None] = ContextVar("run_id", default=None)
candidate_name: ContextVar[str | None] = ContextVar("candidate_name", default=None)
attempt_id: ContextVar[str | None] = ContextVar("attempt_id", default=None)
ray_job_name: ContextVar[str | None] = ContextVar("ray_job_name", default=None)
trace_id: ContextVar[str | None] = ContextVar("trace_id", default=None)

_CORRELATION_FIELDS = (
    "request_id",
    "user_id",
    "project_id",
    "dataset_version_id",
    "run_id",
    "candidate_name",
    "attempt_id",
    "ray_job_name",
    "trace_id",
)


def bind_logging_context(**values: str | None) -> None:
    """Set any subset of the correlation context for the current task."""
    context_map = {
        "request_id": request_id,
        "user_id": user_id,
        "project_id": project_id,
        "dataset_version_id": dataset_version_id,
        "run_id": run_id,
        "candidate_name": candidate_name,
        "attempt_id": attempt_id,
        "ray_job_name": ray_job_name,
        "trace_id": trace_id,
    }
    for key, value in values.items():
        variable = context_map.get(key)
        if variable is not None:
            variable.set(value)


def current_correlation_context() -> dict[str, str]:
    """Return the non-empty correlation identifiers for this context."""
    values = {
        name: variable.get()
        for name, variable in (
            ("request_id", request_id),
            ("user_id", user_id),
            ("project_id", project_id),
            ("dataset_version_id", dataset_version_id),
            ("run_id", run_id),
            ("candidate_name", candidate_name),
            ("attempt_id", attempt_id),
            ("ray_job_name", ray_job_name),
            ("trace_id", trace_id),
        )
    }
    return {key: value for key, value in values.items() if value}


class JsonLogFormatter(logging.Formatter):
    """Render log records as single-line JSON objects."""

    def __init__(self, *, service: str = "sceptre-api") -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": self.service,
        }
        payload.update(current_correlation_context())
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        # Extra fields supplied via `extra={"detail": {...}}` merge under a
        # reserved key so they can never shadow the envelope.
        detail = getattr(record, "detail", None)
        if isinstance(detail, dict):
            payload["detail"] = detail
        return json.dumps(payload, separators=(",", ":"), default=str)


def configure_structured_logging(*, service: str = "sceptre-api") -> None:
    """Install the JSON formatter on the root logger exactly once."""
    root = logging.getLogger()
    for handler in root.handlers:
        if isinstance(getattr(handler, "_sceptre_json", None), bool):
            handler._sceptre_json_service = service  # type: ignore[attr-defined]
            handler.setFormatter(JsonLogFormatter(service=service))
            root.setLevel(logging.INFO)
            return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonLogFormatter(service=service))
    handler._sceptre_json = True  # type: ignore[attr-defined]
    root.handlers = [handler]
    root.setLevel(logging.INFO)
