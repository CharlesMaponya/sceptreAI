from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit, urlunsplit

SENSITIVE_KEYS = {
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
SIGNED_URL = re.compile(r"https?://[^\s'\"]+", re.IGNORECASE)
KEY_VALUE_SECRET = re.compile(
    r"(?i)(authorization|signature|credential|token|upload[_-]?id|session[_-]?uri)"
    r"\s*[=:]\s*([^\s,;}]+)"
)


def redact_url(value: str) -> str:
    parsed = urlsplit(value)
    if not parsed.scheme or not parsed.netloc:
        return value
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "<redacted>", ""))


def redact_text(value: str) -> str:
    redacted = SIGNED_URL.sub(lambda match: redact_url(match.group(0)), value)
    return KEY_VALUE_SECRET.sub(lambda match: f"{match.group(1)}=<redacted>", redacted)


def redact_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, item in value.items():
        normalized = str(key).lower()
        if normalized in SENSITIVE_KEYS or any(
            token in normalized for token in ("signature", "credential", "secret", "token")
        ):
            result[str(key)] = "<redacted>"
        elif isinstance(item, Mapping):
            result[str(key)] = redact_mapping(item)
        elif isinstance(item, list):
            result[str(key)] = [
                redact_mapping(entry)
                if isinstance(entry, Mapping)
                else redact_text(entry)
                if isinstance(entry, str)
                else entry
                for entry in item
            ]
        elif isinstance(item, str):
            result[str(key)] = redact_text(item)
        else:
            result[str(key)] = item
    return result
