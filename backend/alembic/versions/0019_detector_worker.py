"""Explicit detector execution, coordinator fencing and durable recovery ranges."""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

from alembic import op

revision = "0019_detector_worker"
down_revision = "0018_detector_core"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("detectors", sa.Column("execution_state", sa.Text(), nullable=False, server_default="created"), schema="logs")
    op.add_column("detectors", sa.Column("execution_generation", sa.BigInteger(), nullable=False, server_default="0"), schema="logs")
    for name, type_ in (("start_request_id", pg.UUID()), ("start_requested_at", sa.DateTime(timezone=True)),
                        ("execution_started_at", sa.DateTime(timezone=True)), ("stopped_at", sa.DateTime(timezone=True)),
                        ("failure_code", sa.Text())):
        op.add_column("detectors", sa.Column(name, type_, nullable=True), schema="logs")
    op.add_column("detectors", sa.Column("last_failure", pg.JSONB(), nullable=True), schema="logs")
    # Preserve every old checkpoint/input; no existing detector is authorized.
    # The definition/revision trigger remains active during this migration.
    op.execute("UPDATE logs.detectors SET execution_state='terminated', revision=revision+1 WHERE state='stopped'")
    op.create_check_constraint("execution_valid", "detectors", "execution_state IN ('created','requested','starting','running','stopped','failed','terminated')", schema="logs")
    op.create_check_constraint("generation_valid", "detectors", "execution_generation >= 0", schema="logs")
    op.create_table("detector_coordinator", sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("generation", sa.BigInteger(), nullable=False), sa.Column("owner_id", pg.UUID()),
        sa.Column("backend_pid", sa.Integer()), sa.Column("acquired_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("id = 1 AND generation >= 0", name="singleton_valid"), schema="logs")
    op.execute("INSERT INTO logs.detector_coordinator(id,generation) VALUES(1,0)")
    op.create_table("detector_recoveries", sa.Column("id", pg.UUID(), primary_key=True),
        sa.Column("detector_id", pg.UUID(), sa.ForeignKey("logs.detectors.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("first_index", sa.BigInteger(), nullable=False), sa.Column("last_index", sa.BigInteger(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False), sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("coordinator_generation", sa.BigInteger(), nullable=False),
        sa.CheckConstraint("first_index >= 0 AND last_index >= first_index", name="range_valid"),
        sa.CheckConstraint("reason IN ('crash_recovery','manual_resume')", name="reason_valid"), schema="logs")
    op.create_index("ix_detector_recovery", "detector_recoveries", ["detector_id", "first_index", "last_index"], schema="logs")
    # A worker transaction carries a LOCAL fencing token. Deferred checks also
    # cover a physical lock connection lost after flush but before commit.
    op.execute("""CREATE FUNCTION logs.verify_detector_owner() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE token jsonb; raw text;
    BEGIN
      raw := current_setting('diploma.detector_owner', true);
      IF raw IS NULL OR raw = '' THEN RETURN NULL; END IF;
      token := raw::jsonb;
      IF NOT EXISTS (SELECT 1 FROM logs.detector_coordinator c JOIN pg_locks l
          ON l.pid=c.backend_pid AND l.locktype='advisory' AND l.classid=1380205362
          AND l.objid=1 AND l.objsubid=2 AND l.granted AND l.mode='ExclusiveLock'
          AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database())
          WHERE c.id=1 AND c.owner_id=(token->>'id')::uuid
          AND c.generation=(token->>'generation')::bigint AND c.backend_pid=(token->>'pid')::integer)
      THEN RAISE EXCEPTION 'detector ownership lost at commit' USING ERRCODE='40001'; END IF;
      RETURN NULL;
    END $$""")
    for table in ("detectors", "detector_warmups", "detector_outcomes", "findings", "detector_recoveries"):
        op.execute(f"CREATE CONSTRAINT TRIGGER verify_detector_owner AFTER INSERT OR UPDATE ON logs.{table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION logs.verify_detector_owner()")


def downgrade():
    for table in ("detectors", "detector_warmups", "detector_outcomes", "findings", "detector_recoveries"):
        op.execute(f"DROP TRIGGER verify_detector_owner ON logs.{table}")
    op.execute("DROP FUNCTION logs.verify_detector_owner()")
    op.drop_table("detector_recoveries", schema="logs")
    op.drop_table("detector_coordinator", schema="logs")
    op.drop_constraint(op.f("ck_detectors_execution_valid"), "detectors", schema="logs", type_="check")
    op.drop_constraint(op.f("ck_detectors_generation_valid"), "detectors", schema="logs", type_="check")
    for name in ("last_failure", "failure_code", "stopped_at", "execution_started_at", "start_requested_at", "start_request_id", "execution_generation", "execution_state"):
        op.drop_column("detectors", name, schema="logs")
