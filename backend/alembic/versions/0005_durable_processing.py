"""Add durable Kafka-record identity and processing outcomes.

Revision ID: 0005_durable_processing
Revises: 0004_normalizer_json_rules
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0005_durable_processing"
down_revision = "0004_normalizer_json_rules"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "processed_kafka_records",
        sa.Column(
            "id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("connection_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("connection_identity", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("normalizer_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("kafka_topic", sa.Text(), nullable=False),
        sa.Column("kafka_partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=False),
        sa.Column("result_status", sa.Text(), nullable=False),
        sa.Column("backend_received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("backend_processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("btrim(kafka_topic) <> ''", name="topic_not_blank"),
        sa.CheckConstraint("kafka_partition >= 0", name="partition_nonnegative"),
        sa.CheckConstraint("kafka_offset >= 0", name="offset_nonnegative"),
        sa.CheckConstraint(
            "result_status IN ('complete', 'partial', 'failed')",
            name="result_status_valid",
        ),
        sa.CheckConstraint(
            "backend_processed_at >= backend_received_at",
            name="processing_after_receipt",
        ),
        sa.ForeignKeyConstraint(
            ["connection_id"], ["app.kafka_connections.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["source_id"], ["app.sources.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["normalizer_id"], ["app.normalizers.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "connection_identity", "kafka_topic", "kafka_partition", "kafka_offset",
            name="uq_processed_kafka_record_coordinates",
        ),
        schema="logs",
    )
    op.create_index(
        "ix_processed_kafka_records_source_received",
        "processed_kafka_records",
        ["source_id", "backend_received_at"],
        schema="logs",
    )

    op.add_column(
        "parsed_logs",
        sa.Column("processed_record_id", postgresql.UUID(as_uuid=True), nullable=True),
        schema="logs",
    )
    op.add_column(
        "parsed_logs",
        sa.Column(
            "normalization_status", sa.Text(), server_default="complete", nullable=False
        ),
        schema="logs",
    )
    op.add_column(
        "parsed_logs",
        sa.Column(
            "normalization_diagnostics",
            postgresql.JSONB(),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        schema="logs",
    )
    op.create_check_constraint(
        "topic_not_blank", "parsed_logs", "btrim(kafka_topic) <> ''", schema="logs"
    )
    op.create_check_constraint(
        "normalization_status_valid",
        "parsed_logs",
        "normalization_status IN ('complete', 'partial')",
        schema="logs",
    )
    op.create_check_constraint(
        "normalization_diagnostics_array",
        "parsed_logs",
        "jsonb_typeof(normalization_diagnostics) = 'array'",
        schema="logs",
    )
    op.create_unique_constraint(
        "uq_parsed_logs_processed_record_id",
        "parsed_logs",
        ["processed_record_id"],
        schema="logs",
    )
    op.create_foreign_key(
        "fk_parsed_logs_processed_record_id_processed_kafka_records",
        "parsed_logs",
        "processed_kafka_records",
        ["processed_record_id"],
        ["id"],
        source_schema="logs",
        referent_schema="logs",
        ondelete="RESTRICT",
    )

    op.create_table(
        "processing_errors",
        sa.Column(
            "id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("processed_record_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connection_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("source_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("normalizer_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("connection_name", sa.Text(), nullable=False),
        sa.Column("source_name", sa.Text(), nullable=False),
        sa.Column("normalizer_name", sa.Text(), nullable=False),
        sa.Column("normalizer_version", sa.Integer(), nullable=False),
        sa.Column("kafka_topic", sa.Text(), nullable=False),
        sa.Column("kafka_partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=False),
        sa.Column("raw_payload", sa.LargeBinary(), nullable=False),
        sa.Column("stage", sa.Text(), nullable=False),
        sa.Column("diagnostics", postgresql.JSONB(), nullable=False),
        sa.Column("fluent_bit_collected_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("backend_received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("backend_processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "stage IN ('decode', 'envelope', 'normalization')", name="stage_valid"
        ),
        sa.CheckConstraint("btrim(connection_name) <> ''", name="connection_name_not_blank"),
        sa.CheckConstraint("btrim(source_name) <> ''", name="source_name_not_blank"),
        sa.CheckConstraint("btrim(normalizer_name) <> ''", name="normalizer_name_not_blank"),
        sa.CheckConstraint("normalizer_version > 0", name="normalizer_version_positive"),
        sa.CheckConstraint("btrim(kafka_topic) <> ''", name="topic_not_blank"),
        sa.CheckConstraint("kafka_partition >= 0", name="partition_nonnegative"),
        sa.CheckConstraint("kafka_offset >= 0", name="offset_nonnegative"),
        sa.CheckConstraint(
            "jsonb_typeof(diagnostics) = 'array'", name="diagnostics_array"
        ),
        sa.CheckConstraint(
            "backend_processed_at >= backend_received_at",
            name="processing_after_receipt",
        ),
        sa.ForeignKeyConstraint(
            ["processed_record_id"], ["logs.processed_kafka_records.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["connection_id"], ["app.kafka_connections.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["source_id"], ["app.sources.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["normalizer_id"], ["app.normalizers.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("processed_record_id", name="uq_processing_errors_processed_record_id"),
        schema="logs",
    )
    op.create_index(
        "ix_processing_errors_source_received",
        "processing_errors",
        ["source_id", "backend_received_at"],
        schema="logs",
    )
    op.create_index(
        "ix_processing_errors_connection_id",
        "processing_errors",
        ["connection_id"],
        schema="logs",
    )
    op.create_index(
        "ix_processing_errors_normalizer_id",
        "processing_errors",
        ["normalizer_id"],
        schema="logs",
    )


def downgrade() -> None:
    op.drop_index("ix_processing_errors_normalizer_id", table_name="processing_errors", schema="logs")
    op.drop_index("ix_processing_errors_connection_id", table_name="processing_errors", schema="logs")
    op.drop_index("ix_processing_errors_source_received", table_name="processing_errors", schema="logs")
    op.drop_table("processing_errors", schema="logs")
    op.drop_constraint(
        op.f("fk_parsed_logs_processed_record_id_processed_kafka_records"),
        "parsed_logs", schema="logs", type_="foreignkey",
    )
    op.drop_constraint(
        op.f("uq_parsed_logs_processed_record_id"), "parsed_logs", schema="logs", type_="unique"
    )
    op.drop_constraint(
        op.f("ck_parsed_logs_normalization_diagnostics_array"), "parsed_logs", schema="logs", type_="check"
    )
    op.drop_constraint(
        op.f("ck_parsed_logs_normalization_status_valid"), "parsed_logs", schema="logs", type_="check"
    )
    op.drop_constraint(
        op.f("ck_parsed_logs_topic_not_blank"), "parsed_logs", schema="logs", type_="check"
    )
    op.drop_column("parsed_logs", "normalization_diagnostics", schema="logs")
    op.drop_column("parsed_logs", "normalization_status", schema="logs")
    op.drop_column("parsed_logs", "processed_record_id", schema="logs")
    op.drop_index(
        "ix_processed_kafka_records_source_received",
        table_name="processed_kafka_records",
        schema="logs",
    )
    op.drop_table("processed_kafka_records", schema="logs")
