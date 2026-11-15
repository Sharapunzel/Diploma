"""Retire availability execution; preserve identities independently of payloads."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0021_event_time"
down_revision = "0020_detector_commands"
branch_labels = None
depends_on = None


def column(name, type_, **kwargs):
    return sa.Column(name, type_, nullable=kwargs.pop("nullable", False), **kwargs)


def upgrade():
    # The same mutex order as lifecycle and final model saves. An old writer
    # either finishes before these locks or sees the retired fence afterwards.
    op.execute("SELECT id FROM logs.aggregate_schedules ORDER BY id FOR UPDATE")
    op.execute("SELECT id FROM logs.detectors ORDER BY id FOR UPDATE")
    # Retirement updates rows with a deferred active-attempt FK. Check those
    # references in this transaction before subsequent table/trigger DDL.
    op.execute("SET CONSTRAINTS ALL IMMEDIATE")
    op.execute("DROP TRIGGER guard_aggregate_definition ON logs.aggregates")
    for table in ("aggregates", "aggregate_schedules"):
        op.add_column(table, sa.Column("semantic_basis", sa.Text()), schema="logs")
        op.execute(f"UPDATE logs.{table} SET semantic_basis='availability_v1'")
        op.alter_column(table, "semantic_basis", nullable=False, schema="logs")
    op.add_column("aggregates", column("execution_mode", sa.Text(), server_default="retired"), schema="logs")
    op.alter_column("aggregates", "execution_mode", server_default=None, schema="logs")
    for name, type_, default, nullable in (
        ("execution_generation", sa.BigInteger(), "0", False),
        ("bootstrap_at", sa.DateTime(timezone=True), None, True),
        ("online_start_index", sa.BigInteger(), None, True),
        ("paused", sa.Boolean(), sa.text("false"), False),
        ("active_cleanup_id", sa.Uuid(), None, True),
        ("scan_after", sa.Uuid(), None, True),
        ("scan_cycle", sa.BigInteger(), "0", False),
        ("turn", sa.Integer(), "0", False),
    ):
        op.add_column("aggregate_schedules", column(name, type_, server_default=default, nullable=nullable), schema="logs")
    op.execute("UPDATE logs.aggregate_schedules SET execution_generation=execution_generation+1")
    # Metadata added by this migration is not a revision of the saved rule.
    op.execute("""CREATE OR REPLACE FUNCTION logs.guard_aggregate_definition() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (to_jsonb(NEW) - ARRAY['state','archived_at','deleted_at','deleted_by','skipped_ranges','last_error','last_error_at'])
           IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['state','archived_at','deleted_at','deleted_by','skipped_ranges','last_error','last_error_at'])
        THEN RAISE EXCEPTION 'aggregate definition is immutable' USING ERRCODE='23514'; END IF;
        RETURN NEW;
      END $$""")
    op.execute("CREATE TRIGGER guard_aggregate_definition BEFORE UPDATE ON logs.aggregates FOR EACH ROW EXECUTE FUNCTION logs.guard_aggregate_definition()")
    op.execute("""UPDATE logs.detectors SET execution_state='terminated',
      execution_generation=execution_generation+1, revision=revision+1,
      reason='aggregate_time_semantics_retired', failure_code='aggregate_time_semantics_retired'""")
    op.add_column("working_window_points", column("time_basis", sa.Text(), server_default="availability_at"), schema="logs")
    op.alter_column("working_window_points", "time_basis", server_default=None, schema="logs")
    for name in ("last_consumed_projection", "last_evaluated_projection"):
        op.add_column("detectors", sa.Column(name, JSONB()), schema="logs")
    op.drop_constraint(op.f("ck_detector_recoveries_reason_valid"), "detector_recoveries", schema="logs", type_="check")
    op.create_check_constraint("reason_valid", "detector_recoveries",
        "reason IN ('crash_recovery','manual_resume','aggregate_resume')", schema="logs")
    op.create_table("aggregate_closures",
        column("origin_point_id", sa.Uuid(), primary_key=True),
        column("aggregate_id", sa.Uuid(),), column("schedule_id", sa.Uuid()),
        column("source_identity", sa.Uuid()), column("window_index", sa.BigInteger(), nullable=True),
        *[column(n, sa.DateTime(timezone=True)) for n in ("start_utc", "end_utc", "snapshot_reference_at", "closed_at")],
        column("close_not_before_at", sa.DateTime(timezone=True), nullable=True),
        *[column(n, sa.Text()) for n in ("time_basis", "origin", "payload_state", "point_kind")],
        sa.ForeignKeyConstraint(["aggregate_id"], ["logs.aggregates.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["schedule_id"], ["logs.aggregate_schedules.id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("schedule_id", "window_index", name="uq_closure_canonical"), schema="logs")
    # Preserve each old basis; historical rows have no invented grid index.
    op.execute("""INSERT INTO logs.aggregate_closures
      (origin_point_id,aggregate_id,schedule_id,source_identity,window_index,start_utc,end_utc,
       snapshot_reference_at,closed_at,close_not_before_at,time_basis,origin,payload_state,point_kind)
      SELECT p.id,a.id,a.schedule_id,a.source_identity,p.window_index,p.start_utc,p.end_utc,
        p.actual_started_at,p.actual_completed_at,p.scheduled_at,p.time_basis,'legacy','present','working'
      FROM logs.working_window_points p JOIN logs.aggregates a ON a.schedule_id=p.schedule_id
      UNION ALL SELECT p.id,a.id,a.schedule_id,a.source_identity,NULL,p.start_utc,p.end_utc,
        p.actual_started_at,p.actual_completed_at,NULL,p.time_basis,'legacy','present','historical'
      FROM logs.historical_aggregate_points p JOIN logs.aggregates a ON a.id=p.aggregate_id""")
    for table, columns in (
        ("aggregate_memberships", [column("event_id", sa.Uuid(), primary_key=True),
          column("event_time", sa.DateTime(timezone=True), nullable=True), column("fingerprint", sa.Text(), nullable=True)]),
        ("aggregate_snapshots", [column("metadata_json", JSONB())]),
        ("aggregate_late_events", [column("event_id", sa.Uuid(), primary_key=True),
          column("event_time", sa.DateTime(timezone=True)), column("detected_at", sa.DateTime(timezone=True)), column("reason", sa.Text())]),
    ):
        op.create_table(table, column("closure_id", sa.Uuid(), primary_key=True), *columns,
            sa.ForeignKeyConstraint(["closure_id"], ["logs.aggregate_closures.origin_point_id"], ondelete="RESTRICT"), schema="logs")
    op.create_table("aggregate_time_exclusions", column("aggregate_id", sa.Uuid(), primary_key=True),
        column("event_id", sa.Uuid(), primary_key=True), column("reason", sa.Text()),
        column("detected_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["aggregate_id"], ["logs.aggregates.id"], ondelete="RESTRICT"), schema="logs")
    op.create_table("aggregate_history_spans", column("aggregate_id", sa.Uuid(), primary_key=True),
        column("cohort_key", sa.Text(), primary_key=True),
        *[column(n, sa.BigInteger()) for n in ("first_index", "last_index", "next_index")],
        *[column(n, sa.DateTime(timezone=True)) for n in ("load_start", "load_end")], column("coordinates", JSONB()),
        sa.ForeignKeyConstraint(["aggregate_id"], ["logs.aggregates.id"], ondelete="RESTRICT"), schema="logs")
    op.create_table("aggregate_commands", column("id", sa.Uuid(), primary_key=True),
        column("actor_id", sa.Uuid()), column("aggregate_id", sa.Uuid()),
        *[column(n, sa.Text()) for n in ("action", "key", "fingerprint")], column("receipt", JSONB()),
        sa.ForeignKeyConstraint(["aggregate_id"], ["logs.aggregates.id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("actor_id", "action", "key", name="uq_aggregate_command_scope"), schema="logs")
    op.create_table("aggregate_cleanup_operations", column("id", sa.Uuid(), primary_key=True),
        column("aggregate_id", sa.Uuid()), column("created_at", sa.DateTime(timezone=True)),
        *[column(n, sa.BigInteger()) for n in ("requested_keep", "planned_delete", "deleted_points")],
        column("deleted_bytes_if_measured", sa.BigInteger(), nullable=True),
        column("completed_at", sa.DateTime(timezone=True), nullable=True),
        column("reference_tail", JSONB(), nullable=True), column("cutoff", JSONB(), nullable=True),
        column("status", sa.Text()), column("safe_error", sa.Text(), nullable=True), column("progress", JSONB()),
        sa.ForeignKeyConstraint(["aggregate_id"], ["logs.aggregates.id"], ondelete="RESTRICT"), schema="logs")
    op.create_index("ix_cleanup_aggregate", "aggregate_cleanup_operations", ["aggregate_id"], schema="logs")
    # Snapshot readable results BEFORE removing their physical point dependency.
    op.execute("""UPDATE logs.findings f SET data=f.data || jsonb_build_object(
      'origin_point_id',p.id,'time_basis',p.time_basis,'origin',COALESCE(f.data->>'origin','legacy'),
      'model_config',(SELECT d.config FROM logs.detectors d WHERE d.id=f.detector_id),
      'aggregate_rule',(SELECT a.rule FROM logs.aggregates a WHERE a.schedule_id=p.schedule_id),
      'evaluation_started_at',(SELECT o.data->'actual_started_at' FROM logs.detector_outcomes o
         WHERE o.detector_id=f.detector_id AND o.working_point_id=p.id),
      'evaluation_completed_at',(SELECT o.data->'actual_completed_at' FROM logs.detector_outcomes o
         WHERE o.detector_id=f.detector_id AND o.working_point_id=p.id),
      'quality',jsonb_build_object('completeness',p.completeness,'reason',p.completeness_reason,
                                 'evidence',p.evidence_refs),
      'aggregation_started_at',p.actual_started_at,'aggregation_completed_at',p.actual_completed_at)
      FROM logs.working_window_points p WHERE p.id=f.working_point_id""")
    for table in ("findings", "detector_outcomes"):
        op.drop_constraint(f"fk_{table}_working_point_id_working_window_points", table, schema="logs", type_="foreignkey")
        op.create_foreign_key(f"fk_{table}_working_point_id_aggregate_closures", table, "aggregate_closures",
            ["working_point_id"], ["origin_point_id"], source_schema="logs", referent_schema="logs", ondelete="RESTRICT")
    # Projection is bounded to two results, never a second copy of the series.
    for index, projection in (("consumed_index", "last_consumed_projection"), ("evaluated_index", "last_evaluated_projection")):
        op.execute(f"""UPDATE logs.detectors d SET {projection}=jsonb_build_object(
          'point',to_jsonb(p),'outcome',to_jsonb(o)),revision=d.revision+1
          FROM logs.detector_outcomes o JOIN logs.working_window_points p ON p.id=o.working_point_id
          WHERE o.detector_id=d.id AND o.window_index=d.{index}""")
    _guards()


def _guards():
    op.execute("""CREATE FUNCTION logs.guard_event_time_execution() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE sid uuid; mode text; paused boolean; cleanup uuid;
    BEGIN
      IF TG_OP='UPDATE' AND TG_TABLE_NAME IN ('working_window_points','historical_aggregate_points') THEN
        RAISE EXCEPTION 'aggregate point is immutable' USING ERRCODE='23514';
      END IF;
      IF TG_TABLE_NAME='working_window_points' THEN sid:=NEW.schedule_id;
      ELSIF TG_TABLE_NAME='historical_aggregate_points' THEN
        SELECT schedule_id INTO sid FROM logs.aggregates WHERE id=NEW.aggregate_id;
      ELSIF TG_TABLE_NAME='detectors' THEN
        SELECT schedule_id INTO sid FROM logs.aggregates WHERE id=NEW.aggregate_id;
      ELSE
        SELECT a.schedule_id INTO sid FROM logs.detectors d JOIN logs.aggregates a ON a.id=d.aggregate_id WHERE d.id=NEW.detector_id;
      END IF;
      SELECT a.execution_mode,s.paused,s.active_cleanup_id INTO mode,paused,cleanup
        FROM logs.aggregate_schedules s JOIN logs.aggregates a ON a.schedule_id=s.id
        WHERE s.id=sid FOR UPDATE OF s;
      IF mode='retired' THEN
        IF TG_TABLE_NAME='detectors' AND TG_OP='UPDATE' THEN
          IF NEW.execution_state='terminated' AND NEW.checkpoint IS NOT DISTINCT FROM OLD.checkpoint
            AND NEW.consumed_index IS NOT DISTINCT FROM OLD.consumed_index
            AND NEW.evaluated_index IS NOT DISTINCT FROM OLD.evaluated_index THEN RETURN NEW; END IF;
        END IF;
        IF TG_OP='UPDATE' AND TG_TABLE_NAME IN ('findings','detector_outcomes') THEN RETURN NEW; END IF;
        RAISE EXCEPTION 'aggregate_time_semantics_retired' USING ERRCODE='23514';
      END IF;
      IF TG_OP='INSERT' AND TG_TABLE_NAME IN ('working_window_points','historical_aggregate_points','detectors')
        AND (cleanup IS NOT NULL OR (paused AND TG_TABLE_NAME<>'detectors')) THEN
        RAISE EXCEPTION 'aggregate execution fenced' USING ERRCODE='23514';
      END IF;
      IF TG_OP='UPDATE' AND TG_TABLE_NAME='detectors' THEN
        IF (paused OR cleanup IS NOT NULL) AND (NEW.checkpoint IS DISTINCT FROM OLD.checkpoint
            OR NEW.consumed_index IS DISTINCT FROM OLD.consumed_index
            OR NEW.evaluated_index IS DISTINCT FROM OLD.evaluated_index
            OR NEW.execution_state IN ('requested','starting','running') AND NEW.execution_state IS DISTINCT FROM OLD.execution_state)
        THEN RAISE EXCEPTION 'aggregate execution fenced' USING ERRCODE='23514'; END IF;
      END IF;
      RETURN NEW;
    END $$""")
    for table in ("working_window_points", "historical_aggregate_points", "detectors", "detector_outcomes", "findings"):
        op.execute(f"CREATE TRIGGER guard_event_time_execution BEFORE INSERT OR UPDATE ON logs.{table} FOR EACH ROW EXECUTE FUNCTION logs.guard_event_time_execution()")
    op.execute("""CREATE FUNCTION logs.guard_event_time_schedule() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.semantic_basis IS DISTINCT FROM OLD.semantic_basis OR
          NEW.bootstrap_at IS DISTINCT FROM OLD.bootstrap_at OR
          NEW.online_start_index IS DISTINCT FROM OLD.online_start_index THEN
          RAISE EXCEPTION 'immutable event-time plan' USING ERRCODE='23514'; END IF;
        IF NEW.next_index IS DISTINCT FROM OLD.next_index AND EXISTS
          (SELECT 1 FROM logs.aggregates WHERE schedule_id=OLD.id AND execution_mode='retired') THEN
          RAISE EXCEPTION 'aggregate_time_semantics_retired' USING ERRCODE='23514'; END IF;
        RETURN NEW;
      END $$""")
    op.execute("CREATE TRIGGER guard_event_time_schedule BEFORE UPDATE ON logs.aggregate_schedules FOR EACH ROW EXECUTE FUNCTION logs.guard_event_time_schedule()")
    op.execute("""CREATE FUNCTION logs.guard_aggregate_closure() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (to_jsonb(NEW)-'payload_state') IS DISTINCT FROM (to_jsonb(OLD)-'payload_state')
          OR NOT (OLD.payload_state='present' AND NEW.payload_state='cleared')
          OR NOT EXISTS (SELECT 1 FROM logs.aggregate_schedules WHERE id=OLD.schedule_id AND active_cleanup_id IS NOT NULL)
        THEN RAISE EXCEPTION 'immutable aggregate closure' USING ERRCODE='23514'; END IF;
        RETURN NEW;
      END $$""")
    op.execute("CREATE TRIGGER guard_aggregate_closure BEFORE UPDATE ON logs.aggregate_closures FOR EACH ROW EXECUTE FUNCTION logs.guard_aggregate_closure()")
    op.execute("""CREATE FUNCTION logs.guard_aggregate_membership() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (NEW.closure_id,NEW.event_id) IS DISTINCT FROM (OLD.closure_id,OLD.event_id)
          OR NEW.fingerprint IS NOT NULL OR NEW.event_time IS NOT NULL
          OR NOT EXISTS (SELECT 1 FROM logs.aggregate_closures c JOIN logs.aggregate_schedules s ON s.id=c.schedule_id
            WHERE c.origin_point_id=OLD.closure_id AND s.active_cleanup_id IS NOT NULL)
        THEN RAISE EXCEPTION 'immutable aggregate membership' USING ERRCODE='23514'; END IF;
        RETURN NEW;
      END $$""")
    op.execute("CREATE TRIGGER guard_aggregate_membership BEFORE UPDATE ON logs.aggregate_memberships FOR EACH ROW EXECUTE FUNCTION logs.guard_aggregate_membership()")
    op.execute("""CREATE FUNCTION logs.guard_saved_aggregate_result() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'immutable saved aggregate result' USING ERRCODE='23514'; END $$""")
    for table in ("aggregate_snapshots", "aggregate_commands", "findings"):
        op.execute(f"CREATE TRIGGER guard_saved_aggregate_result BEFORE UPDATE ON logs.{table} FOR EACH ROW EXECUTE FUNCTION logs.guard_saved_aggregate_result()")


def downgrade():
    # An empty subsystem can be rolled back by schema regression tests. Once
    # any execution/history exists, retirement cannot safely be undone.
    present = op.get_bind().scalar(sa.text("""SELECT EXISTS(SELECT 1 FROM logs.aggregates)
        OR EXISTS(SELECT 1 FROM logs.aggregate_schedules)
        OR EXISTS(SELECT 1 FROM logs.aggregate_cleanup_operations)"""))
    if present:
        raise RuntimeError("event_time downgrade requires an explicit data-preserving migration")
    for table in ("aggregate_snapshots", "aggregate_commands", "findings"):
        op.execute(f"DROP TRIGGER guard_saved_aggregate_result ON logs.{table}")
    op.execute("DROP FUNCTION logs.guard_saved_aggregate_result()")
    op.execute("DROP TRIGGER guard_aggregate_membership ON logs.aggregate_memberships")
    op.execute("DROP FUNCTION logs.guard_aggregate_membership()")
    op.execute("DROP TRIGGER guard_aggregate_closure ON logs.aggregate_closures")
    op.execute("DROP FUNCTION logs.guard_aggregate_closure()")
    op.execute("DROP TRIGGER guard_event_time_schedule ON logs.aggregate_schedules")
    op.execute("DROP FUNCTION logs.guard_event_time_schedule()")
    for table in ("working_window_points", "historical_aggregate_points", "detectors", "detector_outcomes", "findings"):
        op.execute(f"DROP TRIGGER guard_event_time_execution ON logs.{table}")
    op.execute("DROP FUNCTION logs.guard_event_time_execution()")
    for table in ("findings", "detector_outcomes"):
        op.drop_constraint(f"fk_{table}_working_point_id_aggregate_closures", table, schema="logs", type_="foreignkey")
        op.create_foreign_key(f"fk_{table}_working_point_id_working_window_points", table, "working_window_points",
            ["working_point_id"], ["id"], source_schema="logs", referent_schema="logs", ondelete="RESTRICT")
    for table in ("aggregate_memberships", "aggregate_snapshots", "aggregate_late_events",
                  "aggregate_time_exclusions", "aggregate_history_spans", "aggregate_commands",
                  "aggregate_cleanup_operations", "aggregate_closures"):
        op.drop_table(table, schema="logs")
    for name in ("last_consumed_projection", "last_evaluated_projection"):
        op.drop_column("detectors", name, schema="logs")
    op.drop_constraint(op.f("ck_detector_recoveries_reason_valid"), "detector_recoveries", schema="logs", type_="check")
    op.create_check_constraint("reason_valid", "detector_recoveries", "reason IN ('crash_recovery','manual_resume')", schema="logs")
    op.drop_column("working_window_points", "time_basis", schema="logs")
    for name in ("execution_generation", "bootstrap_at", "online_start_index", "paused",
                 "active_cleanup_id", "scan_after", "scan_cycle", "turn", "semantic_basis"):
        op.drop_column("aggregate_schedules", name, schema="logs")
    for name in ("semantic_basis", "execution_mode"):
        op.drop_column("aggregates", name, schema="logs")
