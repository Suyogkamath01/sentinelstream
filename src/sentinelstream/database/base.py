"""Declarative SQLAlchemy metadata shared by models and Alembic."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all persistence models."""


def utc_now() -> datetime:
    return datetime.now(UTC)


class TimestampMixin:
    """Creation timestamp used for append-only records."""

    created_at: Mapped[datetime] = mapped_column(default=utc_now, nullable=False)
