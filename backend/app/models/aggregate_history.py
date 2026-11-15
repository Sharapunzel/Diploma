"""Permanent closure identities and removable event-time payload metadata."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from ..db import Base


class AggregateClosure(Base):
    __tablename__ = "aggregate_closures"
    __table_args__ = (
        UniqueConstraint("schedule_id", "window_index", name="uq_closure_canonical"),
        {"schema": "logs"},
    )
    origin_point_id: Mapped[UUID] = mapped_column(primary_key=True)
    aggregate_id: Mapped[UUID] = mapped_column(ForeignKey("logs.aggregates.id", ondelete="RESTRICT"))
    schedule_id: Mapped[UUID] = mapped_column(ForeignKey("logs.aggregate_schedules.id", ondelete="RESTRICT"))
    source_identity: Mapped[UUID] = mapped_column()
    window_index: Mapped[int | None] = mapped_column(BigInteger)
    start_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    end_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    time_basis: Mapped[str] = mapped_column(Text)
    origin: Mapped[str] = mapped_column(Text)
    snapshot_reference_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    closed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    close_not_before_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    payload_state: Mapped[str] = mapped_column(Text)
    point_kind: Mapped[str] = mapped_column(Text)


class AggregateMembership(Base):
    __tablename__ = "aggregate_memberships"
    __table_args__ = ({"schema": "logs"},)
    closure_id: Mapped[UUID] = mapped_column(ForeignKey("logs.aggregate_closures.origin_point_id", ondelete="RESTRICT"), primary_key=True)
    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    fingerprint: Mapped[str | None] = mapped_column(Text)


class AggregateSnapshot(Base):
    __tablename__ = "aggregate_snapshots"
    __table_args__ = ({"schema": "logs"},)
    closure_id: Mapped[UUID] = mapped_column(ForeignKey("logs.aggregate_closures.origin_point_id", ondelete="RESTRICT"), primary_key=True)
    metadata_json: Mapped[dict] = mapped_column(JSONB)


class AggregateLateEvent(Base):
    __tablename__ = "aggregate_late_events"
    __table_args__ = ({"schema": "logs"},)
    closure_id: Mapped[UUID] = mapped_column(ForeignKey("logs.aggregate_closures.origin_point_id", ondelete="RESTRICT"), primary_key=True)
    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    reason: Mapped[str] = mapped_column(Text)


class AggregateTimeExclusion(Base):
    __tablename__ = "aggregate_time_exclusions"
    __table_args__ = ({"schema": "logs"},)
    aggregate_id: Mapped[UUID] = mapped_column(ForeignKey("logs.aggregates.id", ondelete="RESTRICT"), primary_key=True)
    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    reason: Mapped[str] = mapped_column(Text)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class AggregateHistorySpan(Base):
    __tablename__ = "aggregate_history_spans"
    __table_args__ = ({"schema": "logs"},)
    aggregate_id: Mapped[UUID] = mapped_column(ForeignKey("logs.aggregates.id", ondelete="RESTRICT"), primary_key=True)
    cohort_key: Mapped[str] = mapped_column(Text, primary_key=True)
    first_index: Mapped[int] = mapped_column(BigInteger)
    last_index: Mapped[int] = mapped_column(BigInteger)
    next_index: Mapped[int] = mapped_column(BigInteger)
    load_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    load_end: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    coordinates: Mapped[dict] = mapped_column(JSONB)


class AggregateCommand(Base):
    __tablename__ = "aggregate_commands"
    __table_args__ = (
        UniqueConstraint("actor_id", "action", "key", name="uq_aggregate_command_scope"),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    actor_id: Mapped[UUID] = mapped_column()
    aggregate_id: Mapped[UUID] = mapped_column(ForeignKey("logs.aggregates.id", ondelete="RESTRICT"))
    action: Mapped[str] = mapped_column(Text)
    key: Mapped[str] = mapped_column(Text)
    fingerprint: Mapped[str] = mapped_column(Text)
    receipt: Mapped[dict] = mapped_column(JSONB)


class AggregateCleanupOperation(Base):
    __tablename__ = "aggregate_cleanup_operations"
    __table_args__ = (Index("ix_cleanup_aggregate", "aggregate_id"), {"schema": "logs"})
    id: Mapped[UUID] = mapped_column(primary_key=True)
    aggregate_id: Mapped[UUID] = mapped_column(ForeignKey("logs.aggregates.id", ondelete="RESTRICT"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    requested_keep: Mapped[int] = mapped_column(BigInteger)
    reference_tail: Mapped[dict | None] = mapped_column(JSONB)
    cutoff: Mapped[dict | None] = mapped_column(JSONB)
    planned_delete: Mapped[int] = mapped_column(BigInteger)
    deleted_points: Mapped[int] = mapped_column(BigInteger)
    deleted_bytes_if_measured: Mapped[int | None] = mapped_column(BigInteger)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(Text)
    safe_error: Mapped[str | None] = mapped_column(Text)
    progress: Mapped[dict] = mapped_column(JSONB)
