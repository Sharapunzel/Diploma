"""Store UTC schedules, availability markers and immutable working points."""

import sqlalchemy as sa

from alembic import op

revision = "0015_aggregation_foundation"
down_revision = "0014_oidc_provider"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "aggregate_schedules",
        sa.Column("id", sa.Uuid(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("source_identity", sa.Uuid(), nullable=False),
        sa.Column("anchor_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("duration_seconds", sa.Integer(), nullable=False),
        sa.Column("next_index", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.CheckConstraint("duration_seconds BETWEEN 1 AND 604800", name="duration_valid"),
        sa.CheckConstraint("next_index >= 0", name="next_index_nonnegative"),
        schema="logs",
    )
    op.create_index("ix_logs_aggregate_schedules_source_identity", "aggregate_schedules",
                    ["source_identity"], schema="logs")
    op.create_table(
        "availability_markers",
        sa.Column("id", sa.Uuid(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("source_identity", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Uuid(), unique=True),
        sa.Column("availability_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("clock_timestamp()")),
        sa.CheckConstraint("kind IN ('event', 'barrier')", name="kind_valid"),
        sa.CheckConstraint("(kind = 'event' AND event_id IS NOT NULL) OR "
                           "(kind = 'barrier' AND event_id IS NULL)", name="payload_valid"),
        schema="logs",
    )
    op.create_index("ix_availability_markers_source_time", "availability_markers",
                    ["source_identity", "availability_at"], schema="logs")
    op.create_table(
        "replication_progress",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("durable_lsn", sa.Numeric(20, 0), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("id = 1", name="singleton"),
        sa.CheckConstraint("durable_lsn >= 0 AND durable_lsn <= 18446744073709551615", name="lsn_valid"),
        schema="logs",
    )
    op.create_table(
        "input_probes",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("source_identity", sa.Uuid(), nullable=False),
        sa.Column("input_generation", sa.Uuid(), nullable=False),
        sa.Column("public_key", sa.LargeBinary(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.CheckConstraint("octet_length(public_key) = 32", name="public_key_length"),
        schema="logs",
    )
    op.create_index("uq_input_probe_generation", "input_probes",
                    ["source_identity", "input_generation"], unique=True, schema="logs")
    op.create_index("uq_input_probe_active_source", "input_probes", ["source_identity"], unique=True,
                    postgresql_where=sa.text("is_active"), schema="logs")
    op.create_table(
        "input_attestations",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("probe_id", sa.Uuid(), sa.ForeignKey("logs.input_probes.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("source_identity", sa.Uuid(), nullable=False),
        sa.Column("input_generation", sa.Uuid(), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("start_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("topic_identity", sa.Text(), nullable=False),
        sa.Column("start_positions", sa.dialects.postgresql.JSONB(), nullable=False),
        sa.Column("end_positions", sa.dialects.postgresql.JSONB(), nullable=False),
        sa.Column("signed_payload", sa.dialects.postgresql.JSONB(), nullable=False),
        sa.Column("signature", sa.LargeBinary(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("clock_timestamp()")),
        sa.CheckConstraint("sequence >= 0", name="sequence_nonnegative"),
        sa.CheckConstraint("end_utc > start_utc", name="bounds_valid"),
        sa.CheckConstraint("state IN ('covered', 'interrupted')", name="state_valid"),
        sa.CheckConstraint("octet_length(signature) = 64", name="signature_length"),
        schema="logs",
    )
    op.create_index("uq_input_attestation_sequence", "input_attestations", ["probe_id", "sequence"], unique=True, schema="logs")
    op.create_index("ix_input_attestation_source_interval", "input_attestations", ["source_identity", "start_utc", "end_utc"], schema="logs")
    op.create_table(
        "source_kafka_checkpoints",
        sa.Column("id", sa.Uuid(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("source_identity", sa.Uuid(), nullable=False),
        sa.Column("topic_identity", sa.Text(), nullable=False),
        sa.Column("partitions", sa.dialects.postgresql.JSONB(), nullable=False),
        sa.Column("worker_instance", sa.Uuid(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("clock_timestamp()")),
        schema="logs",
    )
    op.create_index("ix_source_checkpoint_observed", "source_kafka_checkpoints", ["source_identity", "observed_at"], schema="logs")
    op.create_table(
        "source_interruptions",
        sa.Column("id", sa.Uuid(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("source_identity", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("clock_timestamp()")),
        sa.Column("ended_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("kind IN ('source_disabled', 'processing_stopped')", name="kind_valid"),
        sa.CheckConstraint("ended_at IS NULL OR ended_at >= started_at", name="bounds_valid"),
        schema="logs",
    )
    op.create_index("ix_source_interruption_interval", "source_interruptions", ["source_identity", "started_at", "ended_at"], schema="logs")
    op.execute("""CREATE FUNCTION logs.track_source_interruption() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP = 'INSERT' AND NEW.source_type = 'kafka' AND NOT NEW.is_enabled THEN
            INSERT INTO logs.source_interruptions (source_identity, kind) VALUES (NEW.id, 'source_disabled');
          ELSIF TG_OP = 'UPDATE' AND NEW.source_type = 'kafka' AND OLD.is_enabled AND NOT NEW.is_enabled THEN
            INSERT INTO logs.source_interruptions (source_identity, kind) VALUES (NEW.id, 'source_disabled');
          ELSIF TG_OP = 'UPDATE' AND NOT OLD.is_enabled AND NEW.is_enabled THEN
            UPDATE logs.source_interruptions SET ended_at = clock_timestamp()
            WHERE source_identity = NEW.id AND kind = 'source_disabled' AND ended_at IS NULL;
          END IF;
          RETURN NEW;
        END $$""")
    op.execute("""CREATE TRIGGER track_source_interruption AFTER INSERT OR UPDATE OF is_enabled
        ON app.sources FOR EACH ROW EXECUTE FUNCTION logs.track_source_interruption()""")
    op.execute("""INSERT INTO logs.source_interruptions (source_identity, kind)
        SELECT id, 'source_disabled' FROM app.sources WHERE source_type = 'kafka' AND NOT is_enabled""")
    op.create_table(
        "working_window_points",
        sa.Column("id", sa.Uuid(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("schedule_id", sa.Uuid(),
                  sa.ForeignKey("logs.aggregate_schedules.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("window_index", sa.BigInteger(), nullable=False),
        sa.Column("start_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("actual_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("actual_completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_count", sa.BigInteger(), nullable=False),
        sa.Column("event_time_min", sa.DateTime(timezone=True)),
        sa.Column("event_time_max", sa.DateTime(timezone=True)),
        sa.Column("completeness", sa.Text(), nullable=False),
        sa.Column("completeness_reason", sa.Text(), nullable=False),
        sa.Column("evidence_refs", sa.dialects.postgresql.JSONB(), nullable=False),
        sa.CheckConstraint("window_index >= 0", name="index_nonnegative"),
        sa.CheckConstraint("end_utc > start_utc AND scheduled_at = end_utc", name="bounds_valid"),
        sa.CheckConstraint("actual_started_at >= scheduled_at", name="start_not_early"),
        sa.CheckConstraint("actual_completed_at >= actual_started_at", name="completion_valid"),
        sa.CheckConstraint("event_count >= 0", name="count_nonnegative"),
        sa.CheckConstraint("completeness IN ('complete', 'unknown', 'interrupted')", name="completeness_valid"),
        schema="logs",
    )
    op.create_index("uq_working_window_schedule_index", "working_window_points",
                    ["schedule_id", "window_index"], unique=True, schema="logs")


def downgrade() -> None:
    op.drop_table("working_window_points", schema="logs")
    op.execute("DROP TRIGGER track_source_interruption ON app.sources")
    op.execute("DROP FUNCTION logs.track_source_interruption()")
    op.drop_table("source_interruptions", schema="logs")
    op.drop_table("source_kafka_checkpoints", schema="logs")
    op.drop_table("input_attestations", schema="logs")
    op.drop_table("input_probes", schema="logs")
    op.drop_table("replication_progress", schema="logs")
    op.drop_table("availability_markers", schema="logs")
    op.drop_table("aggregate_schedules", schema="logs")
