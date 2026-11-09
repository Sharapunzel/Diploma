"""Durable detector checkpoints and immutable input/result snapshots."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from ..db import Base


class DetectorCommand(Base):
    __tablename__ = "detector_commands"
    __table_args__ = (
        UniqueConstraint("actor_id", "action", "key", name="uq_detector_command_scope"),
        CheckConstraint("action IN ('create','start','stop')", name="action_valid"),
        CheckConstraint("length(key) BETWEEN 1 AND 200 AND btrim(key) <> ''", name="key_valid"),
        CheckConstraint("length(fingerprint) = 64", name="fingerprint_valid"),
        CheckConstraint("execution_generation >= 0", name="generation_valid"),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    actor_id: Mapped[UUID] = mapped_column()
    action: Mapped[str] = mapped_column(Text)
    key: Mapped[str] = mapped_column(Text)
    fingerprint: Mapped[str] = mapped_column(Text)
    detector_id: Mapped[UUID] = mapped_column(ForeignKey("logs.detectors.id", ondelete="RESTRICT"))
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    execution_generation: Mapped[int] = mapped_column(BigInteger)
    start_request_id: Mapped[UUID | None] = mapped_column()


class Detector(Base):
    __tablename__ = "detectors"
    __table_args__ = (
        CheckConstraint("execution_state IN ('created','requested','starting','running','stopped','failed','terminated')", name="execution_valid"),
        CheckConstraint("execution_generation >= 0", name="generation_valid"),
        CheckConstraint("state IN ('warming','waiting','ready','stopped','error')", name="state_valid"),
        CheckConstraint("revision >= 0", name="revision_valid"),
        CheckConstraint("step_seconds > 0", name="step_valid"),
        CheckConstraint("evaluation_start_index IS NULL OR evaluation_start_index >= 0", name="start_valid"),
        Index("ix_detector_aggregate", "aggregate_id"),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    aggregate_id: Mapped[UUID] = mapped_column(ForeignKey("logs.aggregates.id", ondelete="RESTRICT"))
    name: Mapped[str] = mapped_column(Text)
    created_by: Mapped[UUID] = mapped_column()
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    config: Mapped[dict] = mapped_column(JSONB)
    config_hash: Mapped[str] = mapped_column(Text)
    feature_keys: Mapped[list] = mapped_column(JSONB)
    feature_names: Mapped[list] = mapped_column(JSONB)
    rule_hash: Mapped[str] = mapped_column(Text)
    state: Mapped[str] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    revision: Mapped[int] = mapped_column(BigInteger, default=0)
    accelerated: Mapped[bool] = mapped_column(default=False)
    step_seconds: Mapped[int] = mapped_column()
    accelerated_used: Mapped[bool] = mapped_column(default=False)
    active_attempt_id: Mapped[UUID | None] = mapped_column(ForeignKey(
        "logs.detector_warmups.id", name="fk_detector_active_attempt", use_alter=True,
        deferrable=True, initially="DEFERRED", ondelete="RESTRICT"))
    evaluation_start_index: Mapped[int | None] = mapped_column(BigInteger)
    consumed_index: Mapped[int | None] = mapped_column(BigInteger)
    evaluated_index: Mapped[int | None] = mapped_column(BigInteger)
    checkpoint: Mapped[dict | None] = mapped_column(JSONB)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    execution_state: Mapped[str] = mapped_column(Text, default="created", server_default="created")
    execution_generation: Mapped[int] = mapped_column(BigInteger, default=0, server_default="0")
    start_request_id: Mapped[UUID | None] = mapped_column()
    start_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    execution_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failure_code: Mapped[str | None] = mapped_column(Text)
    last_failure: Mapped[dict | None] = mapped_column(JSONB)


class DetectorCoordinator(Base):
    __tablename__ = "detector_coordinator"
    __table_args__ = (CheckConstraint("id = 1 AND generation >= 0", name="singleton_valid"), {"schema": "logs"})
    id: Mapped[int] = mapped_column(primary_key=True)
    generation: Mapped[int] = mapped_column(BigInteger)
    owner_id: Mapped[UUID | None] = mapped_column()
    backend_pid: Mapped[int | None] = mapped_column()
    acquired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DetectorRecovery(Base):
    __tablename__ = "detector_recoveries"
    __table_args__ = (
        CheckConstraint("first_index >= 0 AND last_index >= first_index", name="range_valid"),
        CheckConstraint("reason IN ('crash_recovery','manual_resume')", name="reason_valid"),
        Index("ix_detector_recovery", "detector_id", "first_index", "last_index"),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    detector_id: Mapped[UUID] = mapped_column(ForeignKey("logs.detectors.id", ondelete="RESTRICT"))
    first_index: Mapped[int] = mapped_column(BigInteger)
    last_index: Mapped[int] = mapped_column(BigInteger)
    reason: Mapped[str] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    coordinator_generation: Mapped[int] = mapped_column(BigInteger)


class DetectorWarmup(Base):
    __tablename__ = "detector_warmups"
    __table_args__ = (
        CheckConstraint("jsonb_typeof(inputs) = 'array'", name="inputs_array"),
        CheckConstraint("status IN ('preparing','prepared','waiting','ready')", name="status_valid"),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    detector_id: Mapped[UUID] = mapped_column(ForeignKey("logs.detectors.id", ondelete="RESTRICT"), index=True)
    cutoff: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    plan: Mapped[dict] = mapped_column(JSONB)
    inputs: Mapped[list] = mapped_column(JSONB)
    inputs_hash: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class DetectorOutcome(Base):
    __tablename__ = "detector_outcomes"
    __table_args__ = (
        Index("uq_detector_outcome_input", "detector_id", "working_point_id", unique=True),
        CheckConstraint("kind IN ('evaluated','skipped')", name="kind_valid"),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    detector_id: Mapped[UUID] = mapped_column(ForeignKey("logs.detectors.id", ondelete="RESTRICT"))
    working_point_id: Mapped[UUID] = mapped_column(ForeignKey("logs.working_window_points.id", ondelete="RESTRICT"))
    window_index: Mapped[int] = mapped_column(BigInteger)
    revision: Mapped[int] = mapped_column(BigInteger)
    kind: Mapped[str] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    data: Mapped[dict] = mapped_column(JSONB)


class Finding(Base):
    __tablename__ = "findings"
    __table_args__ = (
        Index("uq_finding_input", "detector_id", "working_point_id", unique=True),
        {"schema": "logs"},
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    detector_id: Mapped[UUID] = mapped_column(ForeignKey("logs.detectors.id", ondelete="RESTRICT"))
    working_point_id: Mapped[UUID] = mapped_column(ForeignKey("logs.working_window_points.id", ondelete="RESTRICT"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    late: Mapped[bool] = mapped_column(default=False, server_default=text("false"))
    data: Mapped[dict] = mapped_column(JSONB)
