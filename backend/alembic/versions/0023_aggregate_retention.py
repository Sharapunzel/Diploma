"""Durable model-history maintenance and transaction-scoped clear authority."""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "0023_aggregate_retention"
down_revision = "0022_event_time_projection"
branch_labels = None
depends_on = None


def upgrade():
    for name, kind in (("cleared_at", sa.DateTime(timezone=True)),
                       ("clear_reason", sa.Text()), ("clear_policy_version", sa.Text())):
        op.add_column("aggregate_closures", sa.Column(name, kind), schema="logs")
    op.create_table("aggregate_retention_state",
        sa.Column("aggregate_id", sa.Uuid(), primary_key=True),
        sa.Column("policy_version", sa.Text(), nullable=False),
        sa.Column("next_check_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_checked_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.Text(), nullable=False), sa.Column("reason", sa.Text()),
        sa.Column("retry_count", sa.BigInteger(), nullable=False),
        *[sa.Column(name, sa.BigInteger()) for name in
          ("reserve_windows", "keep_from_index", "protected_from_index", "protected_total")],
        sa.Column("protected_by", JSONB(), nullable=False),
        sa.Column("pending_closure_id", sa.Uuid()), sa.Column("pending_phase", sa.Text()),
        sa.Column("pending_cursor", sa.Uuid()), sa.Column("authorization_txid", sa.Text()),
        sa.Column("auto_cleared_total", sa.BigInteger(), nullable=False),
        sa.Column("history_revision", sa.BigInteger(), nullable=False),
        sa.Column("last_cleared_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["aggregate_id"], ["logs.aggregates.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["pending_closure_id"], ["logs.aggregate_closures.origin_point_id"], ondelete="RESTRICT"),
        sa.CheckConstraint("policy_version = 'model_history_v1'", name="policy_valid"),
        sa.CheckConstraint("status IN ('scheduled','idle','compacting','protected','deferred','error','inactive')", name="status_valid"),
        sa.CheckConstraint("retry_count >= 0 AND auto_cleared_total >= 0 AND history_revision >= 0", name="counters_valid"),
        sa.CheckConstraint("pending_phase IS NULL OR pending_phase IN ('claim','memberships','outcomes')", name="phase_valid"),
        schema="logs")
    # A deterministic phase spread, without making any existing payload eligible
    # or inventing clearing timestamps for already-cleared legacy identities.
    op.execute("""INSERT INTO logs.aggregate_retention_state
        (aggregate_id,policy_version,next_check_at,status,retry_count,protected_by,auto_cleared_total,history_revision)
        SELECT id,'model_history_v1',clock_timestamp()+
          (((('x'||substr(replace(id::text,'-',''),1,8))::bit(32)::bigint)%60)*interval '1 second'),
          'scheduled',0,'[]'::jsonb,0,0 FROM logs.aggregates""")
    op.create_index("ix_retention_due", "aggregate_retention_state", ["next_check_at", "aggregate_id"], schema="logs")
    op.create_index("ix_retention_pending", "aggregate_retention_state", ["aggregate_id"],
        schema="logs", postgresql_where=sa.text("pending_closure_id IS NOT NULL"))
    op.create_index("ix_closure_retention_present", "aggregate_closures",
        ["aggregate_id", "window_index", "origin_point_id"], schema="logs",
        postgresql_where=sa.text("payload_state = 'present' AND time_basis = 'event_time'"))
    # Search from the beginning of only the still-heavy rows. A diagnostic UUID
    # cursor cannot exclude a late inserted/re-expanded row below that cursor.
    op.create_index("ix_membership_retention_remaining", "aggregate_memberships", ["closure_id", "event_id"],
        schema="logs", postgresql_where=sa.text("fingerprint IS NOT NULL OR event_time IS NOT NULL"))
    op.create_index("ix_outcome_retention_remaining", "detector_outcomes", ["working_point_id", "id"],
        schema="logs", postgresql_where=sa.text("(data ->> 'compacted')::boolean IS DISTINCT FROM true"))
    # ANALYZE can estimate the actual heavy fraction instead of the default
    # 50% for this JSON boolean. No maintenance turn runs ANALYZE under locks.
    op.execute("""CREATE STATISTICS logs.st_outcome_retention_compacted
        ON (((data ->> 'compacted')::boolean)) FROM logs.detector_outcomes""")
    # Retention compares the immutable attempt UUID, not a large plan hash
    # recomputed while holding lifecycle locks. Close identity-recycling and
    # prepared->preparing loopholes so that equivalence holds at the DB boundary.
    op.execute("""CREATE OR REPLACE FUNCTION logs.protect_warmup_inputs() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (NEW.id,NEW.detector_id,NEW.cutoff,NEW.plan) IS DISTINCT FROM (OLD.id,OLD.detector_id,OLD.cutoff,OLD.plan)
           OR (OLD.status <> 'preparing' AND
               (NEW.inputs,NEW.inputs_hash) IS DISTINCT FROM (OLD.inputs,OLD.inputs_hash))
           OR NOT (NEW.status=OLD.status
             OR (OLD.status='preparing' AND NEW.status IN ('prepared','waiting'))
             OR (OLD.status='prepared' AND NEW.status='ready')) THEN
          RAISE EXCEPTION 'immutable warmup inputs' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$""")
    op.execute("""CREATE OR REPLACE FUNCTION logs.guard_aggregate_closure() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE manual boolean; automatic boolean;
      BEGIN
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
    op.execute("""CREATE OR REPLACE FUNCTION logs.guard_aggregate_membership() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (NEW.closure_id,NEW.event_id) IS DISTINCT FROM (OLD.closure_id,OLD.event_id)
          OR NEW.fingerprint IS NOT NULL OR NEW.event_time IS NOT NULL
          OR NOT EXISTS (SELECT 1 FROM logs.aggregate_closures c JOIN logs.aggregate_schedules s ON s.id=c.schedule_id
            LEFT JOIN logs.aggregate_retention_state r ON r.aggregate_id=c.aggregate_id
            WHERE c.origin_point_id=OLD.closure_id AND (s.active_cleanup_id IS NOT NULL OR
              (c.payload_state='cleared' AND r.pending_closure_id=c.origin_point_id
               AND r.pending_phase='memberships' AND r.authorization_txid=pg_current_xact_id()::text)))
        THEN RAISE EXCEPTION 'immutable aggregate membership' USING ERRCODE='23514'; END IF;
        RETURN NEW;
      END $$""")


def downgrade():
    # Returning to a schema without maintenance would silently lose resumable
    # physical work and clearing provenance. The same empty-subsystem rule as
    # event-time retirement keeps downgrade honest.
    op.execute("""DO $$ BEGIN IF EXISTS (SELECT 1 FROM logs.aggregates) THEN
      RAISE EXCEPTION 'nonempty aggregate retention requires explicit data-preserving migration'; END IF; END $$""")
    op.execute("""CREATE OR REPLACE FUNCTION logs.protect_warmup_inputs() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (NEW.detector_id,NEW.cutoff,NEW.plan) IS DISTINCT FROM (OLD.detector_id,OLD.cutoff,OLD.plan)
           OR (OLD.status <> 'preparing' AND
               (NEW.inputs,NEW.inputs_hash) IS DISTINCT FROM (OLD.inputs,OLD.inputs_hash)) THEN
          RAISE EXCEPTION 'immutable warmup inputs' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$""")
    op.execute("""CREATE OR REPLACE FUNCTION logs.guard_aggregate_closure() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (to_jsonb(NEW)-'payload_state') IS DISTINCT FROM (to_jsonb(OLD)-'payload_state')
          OR NOT (OLD.payload_state='present' AND NEW.payload_state='cleared')
          OR NOT EXISTS (SELECT 1 FROM logs.aggregate_schedules WHERE id=OLD.schedule_id AND active_cleanup_id IS NOT NULL)
        THEN RAISE EXCEPTION 'immutable aggregate closure' USING ERRCODE='23514'; END IF;
        RETURN NEW;
      END $$""")
    op.execute("""CREATE OR REPLACE FUNCTION logs.guard_aggregate_membership() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (NEW.closure_id,NEW.event_id) IS DISTINCT FROM (OLD.closure_id,OLD.event_id)
          OR NEW.fingerprint IS NOT NULL OR NEW.event_time IS NOT NULL
          OR NOT EXISTS (SELECT 1 FROM logs.aggregate_closures c JOIN logs.aggregate_schedules s ON s.id=c.schedule_id
            WHERE c.origin_point_id=OLD.closure_id AND s.active_cleanup_id IS NOT NULL)
        THEN RAISE EXCEPTION 'immutable aggregate membership' USING ERRCODE='23514'; END IF;
        RETURN NEW;
      END $$""")
    op.execute("DROP STATISTICS logs.st_outcome_retention_compacted")
    op.drop_index("ix_outcome_retention_remaining", table_name="detector_outcomes", schema="logs")
    op.drop_index("ix_membership_retention_remaining", table_name="aggregate_memberships", schema="logs")
    op.drop_index("ix_closure_retention_present", table_name="aggregate_closures", schema="logs")
    op.drop_table("aggregate_retention_state", schema="logs")
    for name in ("clear_policy_version", "clear_reason", "cleared_at"):
        op.drop_column("aggregate_closures", name, schema="logs")
