from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from automl_api.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from automl_api.models.enums import FinalTestStatus

ENUM_VALUES = lambda enum: [item.value for item in enum]  # noqa: E731


class FinalTestAllocation(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Central qualification-store row; deliberately has no provider-DB foreign keys."""

    __tablename__ = "final_test_allocations"
    __table_args__ = (
        UniqueConstraint("split_digest", name="uq_final_test_split_digest"),
        UniqueConstraint("scope_id", name="uq_final_test_scope"),
        UniqueConstraint("fixture_revision", "split_revision", name="uq_final_test_revision_pair"),
        CheckConstraint(
            "(fixture_revision IS NULL AND split_revision IS NULL) OR "
            "(fixture_revision IS NOT NULL AND split_revision IS NOT NULL "
            "AND fixture_revision::text ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'::text "
            "AND split_revision::text ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'::text)",
            name="allocation_revision_pair",
        ),
        Index("ix_final_test_status", "status", "updated_at"),
    )

    split_digest: Mapped[str] = mapped_column(String(128), nullable=False)
    fixture_revision: Mapped[str | None] = mapped_column(String(128))
    split_revision: Mapped[str | None] = mapped_column(String(128))
    project_reference: Mapped[str] = mapped_column(String(255), nullable=False)
    scope_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    canonical_provider: Mapped[str] = mapped_column(String(32), nullable=False)
    provider_manifest_digest: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[FinalTestStatus] = mapped_column(
        SQLEnum(
            FinalTestStatus,
            name="final_test_status",
            native_enum=False,
            values_callable=ENUM_VALUES,
        ),
        nullable=False,
        default=FinalTestStatus.ALLOCATED,
        server_default=FinalTestStatus.ALLOCATED.value,
    )
    cas_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    committed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result_digest: Mapped[str | None] = mapped_column(String(128))
    terminal_reason: Mapped[str | None] = mapped_column(Text)


class FinalTestAuthorityReceipt(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "final_test_authority_receipts"
    __table_args__ = (
        UniqueConstraint("allocation_id", "operation", name="uq_final_test_receipt_operation"),
        UniqueConstraint("receipt_digest", name="uq_final_test_receipt_digest"),
    )

    allocation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("final_test_allocations.id", ondelete="CASCADE"), nullable=False
    )
    operation: Mapped[str] = mapped_column(String(32), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    request_digest: Mapped[str] = mapped_column(String(128), nullable=False)
    receipt_digest: Mapped[str] = mapped_column(String(128), nullable=False)
    signature_algorithm: Mapped[str] = mapped_column(String(32), nullable=False)
    signature: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
