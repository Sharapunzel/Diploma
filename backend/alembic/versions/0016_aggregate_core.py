"""Aggregate rules, snapshots, permissions and proven capture boundaries."""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "0016_aggregate_core"
down_revision = "0015_aggregation_foundation"
branch_labels = None
depends_on = None


def upgrade():
    for name, typ in [("legacy_cutover_at", sa.DateTime(timezone=True)),
                      ("availability_verified_from_at", sa.DateTime(timezone=True)),
                      ("boundary_origin", sa.Text()), ("boundary_barrier_id", sa.Uuid())]:
        op.add_column("replication_progress", sa.Column(name, typ), schema="logs")
    op.add_column("working_window_points", sa.Column("features", JSONB()), schema="logs")
    op.create_table("aggregates",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("schedule_id", sa.Uuid(), sa.ForeignKey("logs.aggregate_schedules.id", ondelete="RESTRICT"), nullable=False, unique=True),
        *[sa.Column(n, sa.Text(), nullable=False) for n in ["name", "source_name", "source_type", "rule_hash", "original_boundary", "timezone", "request_key", "request_hash"]],
        sa.Column("source_identity", sa.Uuid(), nullable=False),
        sa.Column("rule", JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.Uuid(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False, server_default=sa.text("'active'")),
        sa.Column("archived_at", sa.DateTime(timezone=True)),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        sa.Column("deleted_by", sa.Uuid()),
        sa.Column("skipped_ranges", JSONB(), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("last_error", sa.Text()),
        sa.Column("last_error_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("state IN ('active', 'archived', 'deleted')", name="state_valid"),
        sa.UniqueConstraint("request_key"), schema="logs")
    op.create_table("historical_aggregate_points",
        sa.Column("id", sa.Uuid(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("aggregate_id", sa.Uuid(), sa.ForeignKey("logs.aggregates.id", ondelete="RESTRICT"), nullable=False),
        *[sa.Column(n, sa.DateTime(timezone=True), nullable=False) for n in ["start_utc", "end_utc", "actual_started_at", "actual_completed_at"]],
        sa.Column("event_count", sa.Integer(), nullable=False),
        sa.Column("event_time_min", sa.DateTime(timezone=True)),
        sa.Column("event_time_max", sa.DateTime(timezone=True)),
        sa.Column("features", JSONB(), nullable=False),
        *[sa.Column(n, sa.Text(), nullable=False) for n in ["time_basis", "completeness", "completeness_reason"]],
        sa.Column("evidence_refs", JSONB(), nullable=False),
        sa.CheckConstraint("end_utc > start_utc", name="bounds_valid"), schema="logs")
    op.create_index("uq_historical_aggregate_window", "historical_aggregate_points", ["aggregate_id", "start_utc", "end_utc"], unique=True, schema="logs")
    op.execute("""UPDATE app.roles SET permissions = permissions || '["aggregates.read", "aggregates.write"]'::jsonb
        WHERE id = '00000000-0000-4000-8000-000000000001'""")
    # Defense in depth: definitions and snapshots cannot be silently revised.
    op.execute("""CREATE FUNCTION logs.guard_aggregate_definition() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF (to_jsonb(NEW) - ARRAY['state','archived_at','deleted_at','deleted_by','skipped_ranges','last_error','last_error_at'])
             IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['state','archived_at','deleted_at','deleted_by','skipped_ranges','last_error','last_error_at']) THEN
            RAISE EXCEPTION 'aggregate definition is immutable';
          END IF;
          RETURN NEW;
        END $$""")
    op.execute("CREATE TRIGGER guard_aggregate_definition BEFORE UPDATE ON logs.aggregates FOR EACH ROW EXECUTE FUNCTION logs.guard_aggregate_definition()")
    op.execute("""CREATE FUNCTION logs.guard_aggregate_schedule() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF (NEW.source_identity,NEW.anchor_utc,NEW.duration_seconds) IS DISTINCT FROM
             (OLD.source_identity,OLD.anchor_utc,OLD.duration_seconds)
             AND EXISTS (SELECT 1 FROM logs.aggregates WHERE schedule_id=OLD.id) THEN
            RAISE EXCEPTION 'aggregate schedule is immutable';
          END IF;
          RETURN NEW;
        END $$""")
    op.execute("CREATE TRIGGER guard_aggregate_schedule BEFORE UPDATE ON logs.aggregate_schedules FOR EACH ROW EXECUTE FUNCTION logs.guard_aggregate_schedule()")
    op.execute("""CREATE FUNCTION logs.guard_capture_boundaries() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF (OLD.legacy_cutover_at IS NOT NULL AND NEW.legacy_cutover_at IS DISTINCT FROM OLD.legacy_cutover_at)
            OR (OLD.availability_verified_from_at IS NOT NULL AND NEW.availability_verified_from_at IS DISTINCT FROM OLD.availability_verified_from_at) THEN
            RAISE EXCEPTION 'capture boundary is immutable';
          END IF;
          RETURN NEW;
        END $$""")
    op.execute("CREATE TRIGGER guard_capture_boundaries BEFORE UPDATE ON logs.replication_progress FOR EACH ROW EXECUTE FUNCTION logs.guard_capture_boundaries()")


def downgrade():
    op.execute("DROP TRIGGER guard_aggregate_schedule ON logs.aggregate_schedules")
    op.execute("DROP FUNCTION logs.guard_aggregate_schedule()")
    op.execute("DROP TRIGGER guard_capture_boundaries ON logs.replication_progress")
    op.execute("DROP FUNCTION logs.guard_capture_boundaries()")
    op.execute("DROP TRIGGER guard_aggregate_definition ON logs.aggregates")
    op.execute("DROP FUNCTION logs.guard_aggregate_definition()")
    op.execute("UPDATE app.roles SET permissions = permissions - 'aggregates.read' - 'aggregates.write'")
    op.drop_table("historical_aggregate_points", schema="logs")
    op.drop_table("aggregates", schema="logs")
    op.drop_column("working_window_points", "features", schema="logs")
    for name in ["legacy_cutover_at", "availability_verified_from_at", "boundary_origin", "boundary_barrier_id"]:
        op.drop_column("replication_progress", name, schema="logs")
