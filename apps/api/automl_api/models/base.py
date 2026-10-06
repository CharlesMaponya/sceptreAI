from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, MetaData, String, Uuid, func, type_coerce
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import operators
from sqlalchemy.types import TypeDecorator

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class StoredEnum(TypeDecorator):
    """Read legacy names and migration values, retaining name-based writes."""

    impl = String
    cache_ok = True

    def __init__(self, enum_class, length=None):
        self.enum_class = enum_class
        self.length = length or max(len(member.name) for member in enum_class)
        super().__init__(length=self.length)

    def member(self, value):
        member = self.enum_class.__members__.get(value)
        return member if member is not None else self.enum_class(value)

    def process_bind_param(self, value, dialect):
        return None if value is None else self.member(value).name

    def process_result_value(self, value, dialect):
        return None if value is None else self.member(value)

    class comparator_factory(TypeDecorator.Comparator):
        def operate(self, op, *other, **kwargs):
            # Literal identity filters must include both historical spellings.
            value = other[0] if other else None
            if op in (operators.eq, operators.ne) and isinstance(value, str):
                op = operators.in_op if op is operators.eq else operators.not_in_op
                value = [value]
            if op in (operators.in_op, operators.not_in_op) and isinstance(
                value, (list, tuple, set, frozenset)
            ):
                spellings = []
                for item in value:
                    if item is None:
                        spellings.append(None)
                    else:
                        member = self.type.member(item)
                        spellings.extend((member.name, member.value))
                return type_coerce(self.expr, String(self.type.length)).operate(
                    op, list(dict.fromkeys(spellings)), **kwargs
                )
            return super().operate(op, *other, **kwargs)


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class UUIDPrimaryKeyMixin:
    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
