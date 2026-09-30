"""Add explicit external target types and logical data stream targets."""

import sqlalchemy as sa

from alembic import op

revision = "0010_data_stream_targets"
down_revision = "0009_external_sources"
branch_labels = None
depends_on = None

OLD_CHECK = (
    "(source_type = 'kafka' AND connection_id IS NOT NULL AND "
    "external_connection_id IS NULL AND topic_name IS NOT NULL AND "
    "index_name IS NULL AND index_pattern IS NULL) OR "
    "(source_type = 'external' AND connection_id IS NULL AND "
    "external_connection_id IS NOT NULL AND topic_name IS NULL AND "
    "kafka_topic_identity IS NULL AND normalizer_id IS NULL AND "
    "is_archived = false AND ((index_name IS NOT NULL AND index_pattern IS NULL) OR "
    "(index_name IS NULL AND index_pattern IS NOT NULL)))"
)

NEW_CHECK = (
    "(source_type = 'kafka' AND connection_id IS NOT NULL AND "
    "external_connection_id IS NULL AND topic_name IS NOT NULL AND "
    "target_type IS NULL AND index_name IS NULL AND index_pattern IS NULL AND "
    "data_stream_name IS NULL AND data_stream_pattern IS NULL) OR "
    "(source_type = 'external' AND connection_id IS NULL AND "
    "external_connection_id IS NOT NULL AND target_type IS NOT NULL AND topic_name IS NULL AND "
    "kafka_topic_identity IS NULL AND normalizer_id IS NULL AND "
    "is_archived = false AND ((target_type = 'index' AND index_name IS NOT NULL AND "
    "index_pattern IS NULL AND data_stream_name IS NULL AND data_stream_pattern IS NULL) OR "
    "(target_type = 'index_pattern' AND index_name IS NULL AND index_pattern IS NOT NULL AND "
    "data_stream_name IS NULL AND data_stream_pattern IS NULL) OR "
    "(target_type = 'data_stream' AND index_name IS NULL AND index_pattern IS NULL AND "
    "data_stream_name IS NOT NULL AND data_stream_pattern IS NULL) OR "
    "(target_type = 'data_stream_pattern' AND index_name IS NULL AND index_pattern IS NULL AND "
    "data_stream_name IS NULL AND data_stream_pattern IS NOT NULL)))"
)


def upgrade() -> None:
    op.add_column("sources", sa.Column("target_type", sa.Text()), schema="app")
    op.add_column("sources", sa.Column("data_stream_name", sa.Text()), schema="app")
    op.add_column("sources", sa.Column("data_stream_pattern", sa.Text()), schema="app")
    op.execute(sa.text("UPDATE app.sources SET target_type = CASE WHEN index_name IS NOT NULL "
                       "THEN 'index' ELSE 'index_pattern' END WHERE source_type = 'external'"))
    op.drop_constraint("source_type_fields", "sources", schema="app", type_="check")
    op.create_check_constraint("source_type_fields", "sources", NEW_CHECK, schema="app")
    op.create_check_constraint("data_stream_name_not_blank", "sources",
                               "data_stream_name IS NULL OR btrim(data_stream_name) <> ''", schema="app")
    op.create_check_constraint("data_stream_pattern_not_blank", "sources",
                               "data_stream_pattern IS NULL OR btrim(data_stream_pattern) <> ''", schema="app")
    op.create_index("uq_sources_external_data_stream", "sources",
                    ["external_connection_id", "data_stream_name"], unique=True, schema="app",
                    postgresql_where=sa.text("source_type = 'external' AND data_stream_name IS NOT NULL"))
    op.create_index("uq_sources_external_data_stream_pattern", "sources",
                    ["external_connection_id", "data_stream_pattern"], unique=True, schema="app",
                    postgresql_where=sa.text("source_type = 'external' AND data_stream_pattern IS NOT NULL"))


def downgrade() -> None:
    if op.get_bind().scalar(sa.text("SELECT EXISTS (SELECT 1 FROM app.sources WHERE "
                                    "source_type = 'external' AND target_type IN "
                                    "('data_stream', 'data_stream_pattern'))")):
        raise RuntimeError("downgrade would discard data stream sources")
    op.drop_index("uq_sources_external_data_stream_pattern", table_name="sources", schema="app")
    op.drop_index("uq_sources_external_data_stream", table_name="sources", schema="app")
    op.drop_constraint("data_stream_pattern_not_blank", "sources", schema="app", type_="check")
    op.drop_constraint("data_stream_name_not_blank", "sources", schema="app", type_="check")
    op.drop_constraint("source_type_fields", "sources", schema="app", type_="check")
    op.create_check_constraint("source_type_fields", "sources", OLD_CHECK, schema="app")
    op.drop_column("sources", "data_stream_pattern", schema="app")
    op.drop_column("sources", "data_stream_name", schema="app")
    op.drop_column("sources", "target_type", schema="app")
