"""Structured logging contract tests (task-index P7-W01/W20)."""

from __future__ import annotations

import json
import logging

import pytest
from automl_api.core.structured_logging import (
    JsonLogFormatter,
    bind_logging_context,
    configure_structured_logging,
    current_correlation_context,
)


@pytest.fixture(autouse=True)
def _clean_context():
    bind_logging_context(
        request_id=None,
        user_id=None,
        project_id=None,
        dataset_version_id=None,
        run_id=None,
        candidate_name=None,
        attempt_id=None,
        ray_job_name=None,
        trace_id=None,
    )
    yield
    bind_logging_context(request_id=None)


def test_json_formatter_emits_parseable_single_line_records() -> None:
    formatter = JsonLogFormatter(service="test")
    record = logging.LogRecord(
        name="test.logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="upload accepted",
        args=(),
        exc_info=None,
    )

    rendered = formatter.format(record)
    payload = json.loads(rendered)

    assert payload["message"] == "upload accepted"
    assert payload["level"] == "INFO"
    assert payload["service"] == "test"
    assert "\n" not in rendered


def test_correlation_identifiers_merge_into_every_record() -> None:
    bind_logging_context(
        request_id="req-1",
        user_id="user-1",
        project_id="proj-1",
        dataset_version_id="ds-1",
        run_id="run-1",
        candidate_name="RandomForestClassifier",
        attempt_id="attempt-1",
        ray_job_name="ray-job-1",
        trace_id="trace-1",
    )

    context = current_correlation_context()

    assert context == {
        "request_id": "req-1",
        "user_id": "user-1",
        "project_id": "proj-1",
        "dataset_version_id": "ds-1",
        "run_id": "run-1",
        "candidate_name": "RandomForestClassifier",
        "attempt_id": "attempt-1",
        "ray_job_name": "ray-job-1",
        "trace_id": "trace-1",
    }

    formatter = JsonLogFormatter()
    record = logging.LogRecord(
        name="t",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="x",
        args=(),
        exc_info=None,
    )
    payload = json.loads(formatter.format(record))
    assert payload["request_id"] == "req-1"
    assert payload["ray_job_name"] == "ray-job-1"


def test_empty_identifiers_are_omitted_not_null() -> None:
    bind_logging_context(request_id="only-one")

    context = current_correlation_context()

    assert context == {"request_id": "only-one"}


def test_configure_structured_logging_is_idempotent() -> None:
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    try:
        root.handlers = [
            handler
            for handler in original_handlers
            if not getattr(handler, "_sceptre_json", False)
        ]
        configure_structured_logging()
        first = next(handler for handler in root.handlers if handler._sceptre_json)
        configure_structured_logging(service="sceptre-worker")
        configured = [
            handler
            for handler in root.handlers
            if getattr(handler, "_sceptre_json", False)
        ]
        assert configured == [first]
        assert first.formatter.service == "sceptre-worker"
    finally:
        root.handlers = original_handlers


def test_detail_extra_is_namespaced_under_detail_key() -> None:
    formatter = JsonLogFormatter()
    record = logging.LogRecord(
        name="t",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="with detail",
        args=(),
        exc_info=None,
    )
    record.detail = {"byte_size": 10}

    payload = json.loads(formatter.format(record))

    assert payload["detail"] == {"byte_size": 10}
