"""Bounded active window attempts and terminal uncomputed diagnostics."""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "0024_aggregate_execution_budget"
down_revision = "0023_aggregate_retention"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("aggregate_schedules", sa.Column("last_execution", JSONB()), schema="logs")
    for table in ("working_window_points", "aggregate_closures"):
        op.add_column(table, sa.Column("calculation_status", sa.Text(), nullable=False, server_default="computed"), schema="logs")
        op.add_column(table, sa.Column("calculation_reason", sa.Text()), schema="logs")
        op.create_check_constraint("calculation_valid", table,
            "(calculation_status='computed' AND calculation_reason IS NULL) OR "
            "(calculation_status='uncomputed' AND calculation_reason IS NOT NULL AND calculation_reason='execution_budget_exceeded')", schema="logs")
    op.add_column("aggregate_closures", sa.Column("execution", JSONB()), schema="logs")
    op.alter_column("aggregate_closures", "snapshot_reference_at", nullable=True, schema="logs")
    op.create_check_constraint("calculation_snapshot_valid", "aggregate_closures",
        "(calculation_status='computed' AND snapshot_reference_at IS NOT NULL) OR "
        "(calculation_status='uncomputed' AND snapshot_reference_at IS NULL AND execution IS NOT NULL AND jsonb_typeof(execution)='object')", schema="logs")
    for name in ("event_count", "completeness", "completeness_reason", "evidence_refs"):
        op.alter_column("working_window_points", name, nullable=True, schema="logs")
    op.create_check_constraint("calculation_payload_valid", "working_window_points",
        "(calculation_status='computed' AND event_count IS NOT NULL AND completeness IS NOT NULL "
        "AND completeness_reason IS NOT NULL AND evidence_refs IS NOT NULL AND evidence_refs <> 'null'::jsonb) OR "
        "(calculation_status='uncomputed' AND event_count IS NULL AND event_time_min IS NULL AND event_time_max IS NULL "
        "AND features IS NULL AND completeness IS NULL AND completeness_reason IS NULL AND evidence_refs IS NULL)", schema="logs")
    op.create_table("aggregate_window_attempts",
        sa.Column("schedule_id", sa.Uuid(), primary_key=True),
        sa.Column("aggregate_id", sa.Uuid(), nullable=False, unique=True),
        sa.Column("attempt_id", sa.Uuid(), nullable=False, unique=True),
        sa.Column("execution_generation", sa.BigInteger(), nullable=False),
        sa.Column("window_index", sa.BigInteger(), nullable=False),
        sa.Column("origin", sa.Text(), nullable=False), sa.Column("cohort_key", sa.Text()),
        *[sa.Column(name, sa.DateTime(timezone=True), nullable=False)
          for name in ("start_utc", "end_utc", "started_at", "deadline_at")],
        sa.Column("hard_budget_seconds", sa.Float(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["aggregate_id"], ["logs.aggregates.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["schedule_id"], ["logs.aggregate_schedules.id"], ondelete="CASCADE"),
        sa.CheckConstraint("origin IN ('online','initial_history')", name="origin_valid"),
        sa.CheckConstraint("state IN ('running','retryable','finalizing')", name="state_valid"),
        sa.CheckConstraint("window_index >= 0 AND hard_budget_seconds > 0 AND hard_budget_seconds <= 30", name="budget_valid"), schema="logs")
    op.create_check_constraint("deadline_valid", "aggregate_window_attempts",
        "deadline_at > started_at AND deadline_at-started_at=hard_budget_seconds*interval '1 second'", schema="logs")
    op.execute("""CREATE FUNCTION logs.guard_window_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (to_jsonb(NEW)-'state') IS DISTINCT FROM (to_jsonb(OLD)-'state')
          OR (OLD.state='finalizing' AND NEW.state<>'finalizing')
        THEN RAISE EXCEPTION 'immutable window attempt' USING ERRCODE='23514'; END IF;
        RETURN NEW;
      END $$""")
    op.execute("CREATE TRIGGER guard_window_attempt BEFORE UPDATE ON logs.aggregate_window_attempts FOR EACH ROW EXECUTE FUNCTION logs.guard_window_attempt()")
    _clear_guard(True)


def _clear_guard(protect_uncomputed):
    check = "IF OLD.calculation_status='uncomputed' THEN RAISE EXCEPTION 'uncomputed diagnostic is permanent' USING ERRCODE='23514'; END IF;" if protect_uncomputed else ""
    op.execute("""CREATE OR REPLACE FUNCTION logs.guard_aggregate_closure() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE manual boolean; automatic boolean;
      BEGIN
      """ + check + """
        SELECT active_cleanup_id IS NOT NULL INTO manual FROM logs.aggregate_schedules WHERE id=OLD.schedule_id;
        SELECT EXISTS (SELECT 1 FROM logs.aggregate_retention_state r
          JOIN logs.aggregates a ON a.id=r.aggregate_id JOIN logs.aggregate_schedules s ON s.id=a.schedule_id
          WHERE r.aggregate_id=OLD.aggregate_id AND r.pending_closure_id=OLD.origin_point_id
            AND r.pending_phase='claim' AND r.authorization_txid=pg_current_xact_id()::text
            AND a.state='active' AND a.execution_mode='current' AND a.semantic_basis='event_time'
            AND NOT s.paused AND s.active_cleanup_id IS NULL)
          INTO automatic;
        IF (to_jsonb(NEW)-ARRAY['payload_state','cleared_at','clear_reason','clear_policy_version'])
              IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['payload_state','cleared_at','clear_reason','clear_policy_version'])
          OR NOT (OLD.payload_state='present' AND NEW.payload_state='cleared')
          OR NEW.cleared_at IS NULL
          OR NOT ((manual AND NEW.clear_reason='manual' AND NEW.clear_policy_version IS NULL)
               OR (automatic AND NEW.clear_reason='automatic' AND NEW.clear_policy_version='model_history_v1'))
        THEN RAISE EXCEPTION 'immutable aggregate closure' USING ERRCODE='23514'; END IF;
        RETURN NEW;
      END $$""")


def downgrade():
    op.execute("""DO $$ BEGIN IF EXISTS (SELECT 1 FROM logs.aggregates) THEN
      RAISE EXCEPTION 'nonempty aggregate execution requires explicit data-preserving migration'; END IF; END $$""")
    op.execute("DROP TRIGGER guard_window_attempt ON logs.aggregate_window_attempts")
    op.execute("DROP FUNCTION logs.guard_window_attempt()")
    _clear_guard(False)
    op.drop_table("aggregate_window_attempts", schema="logs")
    for name in ("calculation_valid", "calculation_payload_valid"):
        op.drop_constraint(op.f("ck_working_window_points_" + name), "working_window_points", type_="check", schema="logs")
    for name in ("event_count", "completeness", "completeness_reason", "evidence_refs"):
        op.alter_column("working_window_points", name, nullable=False, schema="logs")
    for name in ("calculation_valid", "calculation_snapshot_valid"):
        op.drop_constraint(op.f("ck_aggregate_closures_" + name), "aggregate_closures", type_="check", schema="logs")
    op.alter_column("aggregate_closures", "snapshot_reference_at", nullable=False, schema="logs")
    op.drop_column("aggregate_closures", "execution", schema="logs")
    for table in ("aggregate_closures", "working_window_points"):
        for name in ("calculation_reason", "calculation_status"):
            op.drop_column(table, name, schema="logs")
    op.drop_column("aggregate_schedules", "last_execution", schema="logs")
