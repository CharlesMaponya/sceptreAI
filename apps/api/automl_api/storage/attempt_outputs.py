"""Conditional native writes for a declared one-shot attempt output."""

import base64
import hashlib
import re

from automl_api.storage.contracts import (
    ObjectMetadata,
    UploadCapabilityUnavailable,
    UploadContractError,
    immutable_object_version,
)

# shortcut: S3 outputs use one PUT up to 5 GB; larger outputs need tracked multipart custody.
S3_SINGLE_PUT_BYTES = 5_000_000_000


def _verify_bytes(source, byte_size, sha256):
    digest, received = hashlib.sha256(), 0
    while True:
        requested = min(65536, byte_size - received + 1)
        block = source.read(requested)
        if not isinstance(block, bytes) or len(block) > requested:
            raise UploadContractError("Attempt output requires a bounded binary stream")
        if not block:
            break
        received += len(block)
        if received > byte_size:
            raise UploadContractError("Attempt output exceeds its declared bytes")
        digest.update(block)
    if received != byte_size or digest.hexdigest() != sha256:
        raise UploadContractError("Attempt output differs from its declared size or digest")


def write_attempt_output(store, uri, source, *, byte_size, sha256, grant_digest):
    """One conditional write, followed by exact-version byte verification.

    The caller must commit issuance ownership before calling this function. An
    exception can follow an accepted write; it never authorizes another attempt
    or proves that native I/O is physically retired.
    """
    if type(byte_size) is not int or byte_size <= 0:
        raise UploadContractError("Attempt output requires a positive byte budget")
    if any(
        not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value)
        for value in (sha256, grant_digest)
    ):
        raise UploadContractError("Attempt output requires declared content and grant digests")
    driver = store.driver_name
    if driver not in {"aws_s3", "s3_compatible", "gcs", "azure_blob"}:
        raise UploadCapabilityUnavailable("Attempt outputs require immutable native versions")
    if driver in {"aws_s3", "s3_compatible"}:
        if byte_size > S3_SINGLE_PUT_BYTES:
            raise UploadCapabilityUnavailable("Attempt output exceeds the S3 single-PUT budget")
        if store.client.meta.config.retries.get("total_max_attempts") != 1:
            raise UploadContractError("Attempt writes require single-attempt S3 SDK configuration")
    key = store._key_from_uri(uri)
    source.seek(0)
    _verify_bytes(source, byte_size, sha256)
    source.seek(0)
    metadata = {"sceptre_grant": grant_digest, "sceptre_sha256": sha256}
    if driver in {"aws_s3", "s3_compatible"}:
        response = store.client.put_object(
            Bucket=store.bucket,
            Key=key,
            Body=source,
            ContentLength=byte_size,
            ChecksumSHA256=base64.b64encode(bytes.fromhex(sha256)).decode("ascii"),
            IfNoneMatch="*",
            Metadata=metadata,
        )
        version = immutable_object_version(response.get("VersionId"))
        version_key = "version_id"
    elif driver == "gcs":
        blob = store.bucket.blob(key)
        blob.metadata = metadata
        # Verify SHA256 ourselves; SDK corruption cleanup must not delete unknown output.
        blob.upload_from_file(
            source,
            rewind=True,
            size=byte_size,
            if_generation_match=0,
            retry=None,
            checksum=None,
            timeout=60,
        )
        generation = blob.generation
        if (
            not isinstance(generation, (int, str))
            or not str(generation).isascii()
            or not str(generation).isdecimal()
            or int(generation) <= 0
        ):
            raise UploadContractError("Attempt output has no immutable GCS generation")
        version, version_key = str(generation), "generation"
    else:
        response = store.container.get_blob_client(key).upload_blob(
            source,
            blob_type="BlockBlob",
            length=byte_size,
            overwrite=False,
            metadata=metadata,
            max_concurrency=1,
            retry_total=0,
        )
        version = immutable_object_version(response.get("version_id"))
        version_key = "version_id"
    if len(version) > 256:
        raise UploadContractError("Attempt output version exceeds its receipt budget")
    with store.open_stream(uri, version=version) as remote:
        _verify_bytes(remote, byte_size, sha256)
    return ObjectMetadata(
        uri=uri,
        byte_size=byte_size,
        provider_headers={version_key: version, "sceptre_grant": grant_digest},
    )
