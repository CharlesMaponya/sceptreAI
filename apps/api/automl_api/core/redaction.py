from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit, urlunsplit

SENSITIVE_KEYS = {
    "password",
    "passwd",
    "api_key",
    "api-key",
    "x-api-key",
    "authorization",
    "cookie",
    "set-cookie",
    "signature",
    "x-amz-signature",
    "x-amz-credential",
    "x-goog-signature",
    "x-goog-credential",
    "sig",
    "se",
    "sp",
    "sv",
    "token",
    "access_token",
    "session_uri",
    "provider_id",
    "upload_id",
}
SIGNED_URL = re.compile(r"[a-z][a-z0-9+.-]*://[^\s'\"]+", re.IGNORECASE)
KEY_VALUE_SECRET = re.compile(
    r"(?i)(authorization|password|passwd|api[_-]?key|access[_-]?key(?:[_-]?id)?|private[_-]?key|signature|credential|token|upload[_-]?id|session[_-]?uri)"
    r"([\"']?\s*[=:]\s*)"
    r"(\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|(?:Bearer|Basic)\s+[^\s,;}]+|[^\s,;}]+)"
)


def _redact_assignment(match):
    value = match.group(3)
    quote = value[0] if len(value) > 1 and value[0] in {"'", '"'} and value[-1] == value[0] else ""
    return f"{match.group(1)}{match.group(2)}{quote}<redacted>{quote}"


def redact_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return "<redacted-url>"
    if not parsed.scheme or not parsed.netloc:
        return value
    host = parsed.netloc.rsplit("@", 1)[-1]
    return urlunsplit((parsed.scheme, host, parsed.path, "<redacted>", ""))


def redact_text(value: str) -> str:
    redacted = SIGNED_URL.sub(lambda match: redact_url(match.group(0)), value)
    return KEY_VALUE_SECRET.sub(_redact_assignment, redacted)


def redact_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, item in value.items():
        normalized = str(key).lower()
        if normalized in SENSITIVE_KEYS or any(
            token in normalized
            for token in (
                "signature",
                "credential",
                "secret",
                "token",
                "password",
                "passwd",
                "api_key",
                "api-key",
                "access_key",
                "access-key",
                "accesskey",
                "private_key",
                "private-key",
                "privatekey",
            )
        ):
            result[str(key)] = "<redacted>"
        else:
            result[str(key)] = _redact_value(item)
    return result


def _redact_value(value):
    if isinstance(value, Mapping):
        return redact_mapping(value)
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    return redact_text(value) if isinstance(value, str) else value
