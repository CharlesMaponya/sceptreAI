"""Fail closed when raw dataset bytes lack an explicit verified digest."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime

from automl_api.models.datasets import DatasetVersion


class DatasetDigestError(ValueError):
    pass


def require_verified_digest(version: DatasetVersion) -> str:
    if getattr(version, "source_kind", None) not in (None, "raw"):
        raise DatasetDigestError("Provider manifest sources require the verified safe-copy reader.")
    digest = getattr(version, "content_hash", None)
    verified_at = getattr(version, "content_hash_verified_at", None)
    byte_size = getattr(version, "byte_size", None)
    if (
        getattr(version, "content_hash_algorithm", None) != "sha256"
        or getattr(version, "content_hash_scope", None) != "byte_stream"
        or getattr(version, "content_hash_verification_status", None) != "verified"
        or not isinstance(digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        or not isinstance(verified_at, datetime)
        or verified_at.tzinfo is None
        or verified_at.utcoffset() is None
        or not isinstance(byte_size, int)
        or isinstance(byte_size, bool)
        or byte_size < 0
    ):
        raise DatasetDigestError(
            "Dataset requires verified SHA-256 byte-stream provenance before use."
        )
    return digest


def verify_dataset_bytes(version: DatasetVersion, content: bytes) -> None:
    expected = require_verified_digest(version)
    if len(content) != version.byte_size or hashlib.sha256(content).hexdigest() != expected:
        raise DatasetDigestError("Dataset bytes differ from the verified size or SHA-256 digest.")
