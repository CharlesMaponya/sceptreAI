"""Private source bytes and original native custody, never an API response model."""

import uuid
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    ForeignKeyConstraint,
    Integer,
    LargeBinary,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from automl_api.models.base import Base, TimestampMixin


class BufferedUploadInput(TimestampMixin, Base):
    __tablename__ = "buffered_upload_inputs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "command_id"],
            ["workflow_commands.project_id", "workflow_commands.id"],
            ondelete="RESTRICT",
            name="fk_buffered_input_command_project",
        ),
        CheckConstraint("byte_size > 0", name="buffered_input_size"),
        CheckConstraint(
            "content_sha256::text ~ '^[0-9a-f]{64}$'::text", name="buffered_input_sha256"
        ),
        CheckConstraint(
            "manifest_digest::text ~ '^[0-9a-f]{64}$'::text", name="buffered_input_manifest_digest"
        ),
        CheckConstraint(
            "content IS NULL OR (octet_length(content) = byte_size "
            "AND encode(sha256(content), 'hex'::text) = content_sha256::text)",
            name="buffered_input_bytes",
        ),
        CheckConstraint(
            "phase IN ('admitted', 'issued', 'recorded', 'published', 'retired')",
            name="buffered_input_phase",
        ),
        CheckConstraint(
            "content IS NOT NULL OR phase IN ('published', 'retired') OR chunk_count > 0",
            name="buffered_input_custody",
        ),
        CheckConstraint(
            "phase IN ('admitted', 'issued', 'recorded', 'published') OR content IS NULL",
            name="buffered_input_retired",
        ),
        CheckConstraint(
            "jsonb_typeof(manifest) = 'object'::text AND manifest <> '{}'::jsonb "
            "AND jsonb_typeof(native_receipt) = 'object'::text",
            name="buffered_input_objects",
        ),
        CheckConstraint(
            "phase IN ('admitted', 'issued', 'retired') OR native_receipt <> '{}'::jsonb",
            name="buffered_input_receipt_required",
        ),
        CheckConstraint(
            "phase IN ('recorded', 'published', 'retired') OR native_receipt = '{}'::jsonb",
            name="buffered_input_receipt_phase",
        ),
        CheckConstraint(
            "chunk_count >= 0 AND ((chunk_count = 0 AND chunk_manifest_sha256 IS NULL) OR "
            "(chunk_count > 0 AND content IS NULL AND chunk_manifest_sha256 IS NOT NULL AND "
            "chunk_manifest_sha256::text ~ '^[0-9a-f]{64}$'::text))",
            name="buffered_input_chunk_layout",
        ),
    )

    command_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    project_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    content: Mapped[bytes | None] = mapped_column(LargeBinary, deferred=True)
    byte_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    manifest: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    manifest_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    phase: Mapped[str] = mapped_column(
        String(16), nullable=False, default="admitted", server_default="admitted"
    )
    chunk_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    chunk_manifest_sha256: Mapped[str | None] = mapped_column(String(64))
    native_receipt: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )


class BufferedUploadChunk(Base):
    __tablename__ = "buffered_upload_chunks"
    __table_args__ = (
        ForeignKeyConstraint(
            ["command_id"],
            ["buffered_upload_inputs.command_id"],
            ondelete="CASCADE",
            deferrable=True,
            initially="DEFERRED",
            name="fk_buffered_chunk_input",
        ),
        ForeignKeyConstraint(
            ["project_id", "command_id"],
            ["workflow_commands.project_id", "workflow_commands.id"],
            ondelete="RESTRICT",
            name="fk_buffered_chunk_command_project",
        ),
        CheckConstraint("ordinal >= 0", name="buffered_chunk_ordinal"),
        CheckConstraint("byte_size > 0 AND byte_size <= 8388608", name="buffered_chunk_size"),
        CheckConstraint(
            "content_sha256::text ~ '^[0-9a-f]{64}$'::text", name="buffered_chunk_sha256"
        ),
        CheckConstraint(
            "octet_length(content) = byte_size AND "
            "encode(sha256(content), 'hex'::text) = content_sha256::text",
            name="buffered_chunk_bytes",
        ),
    )
    command_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    ordinal: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    byte_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    content: Mapped[bytes] = mapped_column(LargeBinary, nullable=False, deferred=True)
