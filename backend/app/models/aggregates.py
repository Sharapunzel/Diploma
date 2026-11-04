"""Immutable aggregate definitions and historical snapshots."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from ..db import Base


class Aggregate(Base):
    __tablename__ = "aggregates"
    __table_args__ = (
        CheckConstraint("state IN ('active', 'archived', 'deleted')", name="state_valid"),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    schedule_id: Mapped[UUID] = mapped_column(ForeignKey("logs.aggregate_schedules.id", ondelete="RESTRICT"), unique=True)
    name: Mapped[str] = mapped_column(Text)
    source_identity: Mapped[UUID] = mapped_column()
    source_name: Mapped[str] = mapped_column(Text)
    source_type: Mapped[str] = mapped_column(Text)
    rule: Mapped[dict] = mapped_column(JSONB)
    rule_hash: Mapped[str] = mapped_column(Text)
    original_boundary: Mapped[str] = mapped_column(Text)
    timezone: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_by: Mapped[UUID] = mapped_column()
    state: Mapped[str] = mapped_column(Text, server_default=text("'active'"))
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deleted_by: Mapped[UUID | None] = mapped_column()
    skipped_ranges: Mapped[list] = mapped_column(JSONB, server_default=text("'[]'::jsonb"))
    request_key: Mapped[str] = mapped_column(Text, unique=True)
    request_hash: Mapped[str] = mapped_column(Text)
    last_error: Mapped[str | None] = mapped_column(Text)
    last_error_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class HistoricalAggregatePoint(Base):
    __tablename__ = "historical_aggregate_points"
    __table_args__ = (
        Index("uq_historical_aggregate_window", "aggregate_id", "start_utc", "end_utc", unique=True),
        CheckConstraint("end_utc > start_utc", name="bounds_valid"),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    aggregate_id: Mapped[UUID] = mapped_column(ForeignKey("logs.aggregates.id", ondelete="RESTRICT"))
    start_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    end_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    actual_started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    actual_completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    event_count: Mapped[int] = mapped_column()
    event_time_min: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    event_time_max: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    features: Mapped[list] = mapped_column(JSONB)
    time_basis: Mapped[str] = mapped_column(Text)
    completeness: Mapped[str] = mapped_column(Text)
    completeness_reason: Mapped[str] = mapped_column(Text)
    evidence_refs: Mapped[dict] = mapped_column(JSONB)
