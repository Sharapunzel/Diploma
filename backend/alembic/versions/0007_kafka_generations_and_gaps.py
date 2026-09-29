"""Record retention gaps and isolate Kafka topic generations.

Downgrade refuses to discard operational events or collapse duplicate coordinates.
Legacy ledger rows retain NULL topic identities and are never attributed automatically.
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0007_kafka_generations_and_gaps"
down_revision = "0006_kafka_topic_identity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("kafka_connections", sa.Column("cluster_identity", sa.Text()), schema="app")
    op.create_index(
        "uq_kafka_connections_cluster_identity", "kafka_connections",
        ["cluster_identity"], unique=True, schema="app",
        postgresql_where=sa.text("cluster_identity IS NOT NULL"),
    )
    op.add_column(
        "sources", sa.Column("is_archived", sa.Boolean(), nullable=False,
                              server_default=sa.text("false")), schema="app",
    )
    op.drop_constraint("uq_sources_connection_id", "sources", schema="app", type_="unique")
    op.create_index(
        "uq_sources_current_connection_topic", "sources", ["connection_id", "topic_name"],
        unique=True, schema="app", postgresql_where=sa.text("is_archived = false"),
    )
    op.create_index(
        "uq_sources_current_topic_identity", "sources",
        ["connection_id", "kafka_topic_identity"], unique=True, schema="app",
        postgresql_where=sa.text("is_archived = false AND kafka_topic_identity IS NOT NULL"),
    )
    op.drop_constraint(
        "uq_processed_kafka_record_coordinates", "processed_kafka_records",
        schema="logs", type_="unique",
    )
    op.create_index(
        "uq_processed_kafka_record_generation", "processed_kafka_records",
        ["connection_identity", "kafka_topic_identity", "kafka_partition", "kafka_offset"],
        unique=True, schema="logs", postgresql_where=sa.text("kafka_topic_identity IS NOT NULL"),
    )
    op.create_index(
        "uq_processed_kafka_record_legacy", "processed_kafka_records",
        ["connection_identity", "kafka_topic", "kafka_partition", "kafka_offset"],
        unique=True, schema="logs", postgresql_where=sa.text("kafka_topic_identity IS NULL"),
    )
    uuid = postgresql.UUID(as_uuid=True)
    op.create_table(
        "kafka_operational_events",
        sa.Column("id", uuid, primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("reason_code", sa.Text(), nullable=False),
        sa.Column("source_id", uuid, sa.ForeignKey("app.sources.id", ondelete="SET NULL")),
        sa.Column("source_identity", uuid, nullable=False),
        sa.Column("source_name", sa.Text(), nullable=False),
        sa.Column("connection_id", uuid, sa.ForeignKey("app.kafka_connections.id", ondelete="SET NULL")),
        sa.Column("connection_identity", uuid, nullable=False),
        sa.Column("connection_name", sa.Text(), nullable=False),
        sa.Column("cluster_identity", sa.Text(), nullable=False),
        sa.Column("topic_name", sa.Text(), nullable=False),
        sa.Column("old_topic_identity", sa.Text(), nullable=False),
        sa.Column("new_topic_identity", sa.Text()),
        sa.Column("kafka_partition", sa.Integer()),
        sa.Column("offset_start", sa.BigInteger()),
        sa.Column("offset_end", sa.BigInteger()),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.CheckConstraint("kind IN ('retention_gap', 'topic_recreated')", name="kind_valid"),
        sa.CheckConstraint("btrim(topic_name) <> ''", name="topic_not_blank"),
        sa.CheckConstraint("btrim(old_topic_identity) <> ''", name="old_identity_not_blank"),
        sa.CheckConstraint(
            "(kind = 'retention_gap' AND kafka_partition >= 0 AND offset_start >= 0 "
            "AND offset_end > offset_start AND new_topic_identity IS NULL) OR "
            "(kind = 'topic_recreated' AND kafka_partition IS NULL AND offset_start IS NULL "
            "AND offset_end IS NULL AND new_topic_identity IS NOT NULL AND "
            "btrim(new_topic_identity) <> '' AND new_topic_identity <> old_topic_identity)",
            name="payload_valid",
        ),
        schema="logs",
    )
    op.create_index(
        "uq_kafka_retention_gap", "kafka_operational_events",
        ["source_identity", "old_topic_identity", "kafka_partition", "offset_start", "offset_end"],
        unique=True, schema="logs", postgresql_where=sa.text("kind = 'retention_gap'"),
    )
    op.create_index(
        "uq_kafka_topic_recreated", "kafka_operational_events",
        ["source_identity", "old_topic_identity", "new_topic_identity"],
        unique=True, schema="logs", postgresql_where=sa.text("kind = 'topic_recreated'"),
    )
    op.create_index(
        "ix_kafka_operational_events_source_detected", "kafka_operational_events",
        ["source_identity", "detected_at"], schema="logs",
    )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.scalar(sa.text("SELECT EXISTS (SELECT 1 FROM logs.kafka_operational_events)")):
        raise RuntimeError("downgrade would discard Kafka operational events")
    if bind.scalar(sa.text("SELECT EXISTS (SELECT 1 FROM app.sources WHERE is_archived)")):
        raise RuntimeError("downgrade would discard archived source state")
    if bind.scalar(sa.text(
        "SELECT EXISTS (SELECT 1 FROM logs.processed_kafka_records "
        "GROUP BY connection_identity, kafka_topic, kafka_partition, kafka_offset "
        "HAVING count(*) > 1)"
    )):
        raise RuntimeError("downgrade would merge Kafka topic generations")
    op.drop_index("ix_kafka_operational_events_source_detected", table_name="kafka_operational_events", schema="logs")
    op.drop_index("uq_kafka_topic_recreated", table_name="kafka_operational_events", schema="logs")
    op.drop_index("uq_kafka_retention_gap", table_name="kafka_operational_events", schema="logs")
    op.drop_table("kafka_operational_events", schema="logs")
    op.drop_index("uq_processed_kafka_record_legacy", table_name="processed_kafka_records", schema="logs")
    op.drop_index("uq_processed_kafka_record_generation", table_name="processed_kafka_records", schema="logs")
    op.create_unique_constraint(
        "uq_processed_kafka_record_coordinates", "processed_kafka_records",
        ["connection_identity", "kafka_topic", "kafka_partition", "kafka_offset"], schema="logs",
    )
    op.drop_index("uq_sources_current_connection_topic", table_name="sources", schema="app")
    op.drop_index("uq_sources_current_topic_identity", table_name="sources", schema="app")
    op.create_unique_constraint(
        "uq_sources_connection_id", "sources", ["connection_id", "topic_name"], schema="app",
    )
    op.drop_column("sources", "is_archived", schema="app")
    op.drop_index("uq_kafka_connections_cluster_identity", table_name="kafka_connections", schema="app")
    op.drop_column("kafka_connections", "cluster_identity", schema="app")
