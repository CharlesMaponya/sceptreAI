"""Explicit digest provenance for synthetic dataset fixtures, never a guard bypass."""

import hashlib
from datetime import UTC, datetime
from types import SimpleNamespace


def verified_digest_metadata():
    return dict(
        content_hash_algorithm="sha256",
        content_hash_scope="byte_stream",
        content_hash_verification_status="verified",
        content_hash_verified_at=datetime.now(UTC),
    )


def verified_raw_version(content=b"fixture", **values):
    return SimpleNamespace(
        **{
            **verified_digest_metadata(),
            "content_hash": hashlib.sha256(content).hexdigest(),
            "byte_size": len(content),
            **values,
        }
    )


def verified_upload_metadata(content=b"fixture"):
    digest = hashlib.sha256(content).hexdigest()
    return dict(
        digest_algorithm="sha256",
        digest_scope="byte_stream",
        expected_object_digest=digest,
        observed_object_digest=digest,
        checksum_verified_at=datetime.now(UTC),
        scanner_status="allowed",
        byte_size=len(content),
    )
