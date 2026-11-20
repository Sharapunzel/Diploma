"""One bounded active context per aggregate, never intermediate window data."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, DateTime, Float, ForeignKey, Text
from sqlalchemy.orm import Mapped, mapped_column

from ..db import Base


class AggregateWindowAttempt(Base):
    __tablename__ = "aggregate_window_attempts"
    __table_args__ = (
        CheckConstraint("origin IN ('online','initial_history')", name="origin_valid"),
        CheckConstraint("state IN ('running','retryable','finalizing')", name="state_valid"),
        CheckConstraint("window_index >= 0 AND hard_budget_seconds > 0 AND hard_budget_seconds <= 30", name="budget_valid"),
        CheckConstraint("deadline_at > started_at AND deadline_at-started_at=hard_budget_seconds*interval '1 second'", name="deadline_valid"),
        {"schema": "logs"},
    )
    schedule_id: Mapped[UUID] = mapped_column(ForeignKey("logs.aggregate_schedules.id", ondelete="CASCADE"), primary_key=True)
    aggregate_id: Mapped[UUID] = mapped_column(ForeignKey("logs.aggregates.id", ondelete="CASCADE"), unique=True)
    attempt_id: Mapped[UUID] = mapped_column(unique=True)
    execution_generation: Mapped[int] = mapped_column(BigInteger)
    window_index: Mapped[int] = mapped_column(BigInteger)
    origin: Mapped[str] = mapped_column(Text)
    cohort_key: Mapped[str | None] = mapped_column(Text)
    start_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    end_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    deadline_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    hard_budget_seconds: Mapped[float] = mapped_column(Float)
    state: Mapped[str] = mapped_column(Text)
