"""Durable technical history maintenance; independent of manual commands."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, Index, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from ..db import Base


class AggregateRetentionState(Base):
    __tablename__ = "aggregate_retention_state"
    __table_args__ = (
        CheckConstraint("policy_version = 'model_history_v1'", name="policy_valid"),
        CheckConstraint("status IN ('scheduled','idle','compacting','protected','deferred','error','inactive')", name="status_valid"),
        CheckConstraint("retry_count >= 0 AND auto_cleared_total >= 0 AND history_revision >= 0", name="counters_valid"),
        CheckConstraint("pending_phase IS NULL OR pending_phase IN ('claim','memberships','outcomes')", name="phase_valid"),
        Index("ix_retention_due", "next_check_at", "aggregate_id"),
        Index("ix_retention_pending", "aggregate_id", postgresql_where=text("pending_closure_id IS NOT NULL")),
        {"schema": "logs"},
    )
    aggregate_id: Mapped[UUID] = mapped_column(ForeignKey("logs.aggregates.id", ondelete="RESTRICT"), primary_key=True)
    policy_version: Mapped[str] = mapped_column(Text)
    next_check_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    retry_count: Mapped[int] = mapped_column(BigInteger, default=0)
    reserve_windows: Mapped[int | None] = mapped_column(BigInteger)
    keep_from_index: Mapped[int | None] = mapped_column(BigInteger)
    protected_from_index: Mapped[int | None] = mapped_column(BigInteger)
    protected_total: Mapped[int | None] = mapped_column(BigInteger)
    protected_by: Mapped[list] = mapped_column(JSONB, default=list)
    pending_closure_id: Mapped[UUID | None] = mapped_column(ForeignKey("logs.aggregate_closures.origin_point_id", ondelete="RESTRICT"))
    pending_phase: Mapped[str | None] = mapped_column(Text)
    pending_cursor: Mapped[UUID | None] = mapped_column()
    authorization_txid: Mapped[str | None] = mapped_column(Text)
    auto_cleared_total: Mapped[int] = mapped_column(BigInteger, default=0)
    history_revision: Mapped[int] = mapped_column(BigInteger, default=0)
    last_cleared_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
