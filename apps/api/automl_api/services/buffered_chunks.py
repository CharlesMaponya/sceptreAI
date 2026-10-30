"""Bounded private parts retain exact content without a single PostgreSQL BYTEA."""

import hashlib

from sqlalchemy import delete, insert, select

from automl_api.models.buffered_uploads import BufferedUploadChunk
from automl_api.storage.contracts import UploadContractError

PART_BYTES = 8 * 1024 * 1024


def manifest_hash(parts):
    digest = hashlib.sha256()
    for ordinal, (size, sha) in enumerate(parts):
        digest.update(f"{ordinal}:{size}:{sha}\n".encode("ascii"))
    return digest.hexdigest()


def describe(content):
    if len(content) <= PART_BYTES:
        return []
    return [
        (len(part), hashlib.sha256(part).hexdigest())
        for offset in range(0, len(content), PART_BYTES)
        for part in (content[offset : offset + PART_BYTES],)
    ]


def store_parts(db, command_id, project_id, content, parts):
    for ordinal, (size, sha) in enumerate(parts):
        db.execute(
            insert(BufferedUploadChunk).values(
                command_id=command_id,
                project_id=project_id,
                ordinal=ordinal,
                byte_size=size,
                content_sha256=sha,
                content=content[ordinal * PART_BYTES : ordinal * PART_BYTES + size],
            )
        )


def validate_layout(db, row):
    metadata = list(
        db.execute(
            select(
                BufferedUploadChunk.ordinal,
                BufferedUploadChunk.byte_size,
                BufferedUploadChunk.content_sha256,
            )
            .where(
                BufferedUploadChunk.command_id == row.command_id,
                BufferedUploadChunk.project_id == row.project_id,
            )
            .order_by(BufferedUploadChunk.ordinal)
        )
    )
    if row.phase in {"published", "retired"} or row.chunk_count == 0:
        if metadata:
            raise UploadContractError("Closed or inline buffered source retained private parts.")
        return
    if (
        len(metadata) != row.chunk_count
        or [part.ordinal for part in metadata] != list(range(row.chunk_count))
        or sum(part.byte_size for part in metadata) != row.byte_size
        or manifest_hash([(part.byte_size, part.content_sha256) for part in metadata])
        != row.chunk_manifest_sha256
    ):
        raise UploadContractError("Original buffered part layout is missing or changed.")


def load_parts(db, row):
    if row.chunk_count == 0:
        if not isinstance(row.content, bytes):
            raise UploadContractError("Original inline buffered source is missing.")
        return [row.content]
    parts = []
    for ordinal, body in db.execute(
        select(BufferedUploadChunk.ordinal, BufferedUploadChunk.content)
        .where(
            BufferedUploadChunk.command_id == row.command_id,
            BufferedUploadChunk.project_id == row.project_id,
        )
        .order_by(BufferedUploadChunk.ordinal)
        .execution_options(stream_results=True, yield_per=1)
    ):
        if ordinal != len(parts) or not isinstance(body, bytes) or not 0 < len(body) <= PART_BYTES:
            raise UploadContractError("Original buffered part content is missing or changed.")
        parts.append(body)
    if len(parts) != row.chunk_count:
        raise UploadContractError("Original buffered part content is incomplete.")
    return parts


def clear_parts(db, row):
    if row.phase not in {"published", "retired"}:
        raise UploadContractError("Private buffered parts still retain custody.")
    db.flush()
    if row.chunk_count:
        db.execute(
            delete(BufferedUploadChunk).where(
                BufferedUploadChunk.command_id == row.command_id,
                BufferedUploadChunk.project_id == row.project_id,
            )
        )
