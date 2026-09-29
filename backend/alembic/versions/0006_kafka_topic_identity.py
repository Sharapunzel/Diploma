"""Persist Kafka topic identity for consumer safety.

Revision ID: 0006_kafka_topic_identity
Revises: 0005_durable_processing
"""

import sqlalchemy as sa

from alembic import op

revision = "0006_kafka_topic_identity"
down_revision = "0005_durable_processing"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "sources",
        sa.Column("kafka_topic_identity", sa.Text(), nullable=True),
        schema="app",
    )
    op.add_column(
        "processed_kafka_records",
        sa.Column("kafka_topic_identity", sa.Text(), nullable=True),
        schema="logs",
    )


def downgrade() -> None:
    op.drop_column(
        "processed_kafka_records", "kafka_topic_identity", schema="logs"
    )
    op.drop_column("sources", "kafka_topic_identity", schema="app")
