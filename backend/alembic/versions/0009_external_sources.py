"""Add external indexer connections and typed sources."""

import sqlalchemy as sa

from alembic import op

revision = "0009_external_sources"
down_revision = "0008_processing_error_identity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "external_connections",
        sa.Column("id", sa.Uuid(), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.Text(), nullable=False, unique=True),
        sa.Column("base_url", sa.Text(), nullable=False),
        sa.Column("username", sa.Text(), nullable=False),
        sa.Column("encrypted_password", sa.Text(), nullable=False),
        sa.Column("ca_pem", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.CheckConstraint("btrim(name) <> ''", name="name_not_blank"),
        sa.CheckConstraint("btrim(base_url) <> ''", name="base_url_not_blank"),
        sa.CheckConstraint("btrim(username) <> ''", name="username_not_blank"),
        schema="app",
    )
    op.add_column("sources", sa.Column("source_type", sa.Text(), nullable=False,
                  server_default="kafka"), schema="app")
    op.add_column("sources", sa.Column("external_connection_id", sa.Uuid()), schema="app")
    op.add_column("sources", sa.Column("index_name", sa.Text()), schema="app")
    op.add_column("sources", sa.Column("index_pattern", sa.Text()), schema="app")
    op.create_foreign_key("fk_sources_external_connection_id_external_connections",
                          "sources", "external_connections", ["external_connection_id"], ["id"],
                          source_schema="app", referent_schema="app", ondelete="CASCADE")
    op.alter_column("sources", "connection_id", nullable=True, schema="app")
    op.alter_column("sources", "topic_name", nullable=True, schema="app")
    op.drop_constraint("topic_name_not_blank", "sources", schema="app", type_="check")
    op.create_check_constraint("topic_name_not_blank", "sources",
                               "topic_name IS NULL OR btrim(topic_name) <> ''", schema="app")
    op.create_check_constraint("index_name_not_blank", "sources",
                               "index_name IS NULL OR btrim(index_name) <> ''", schema="app")
    op.create_check_constraint("index_pattern_not_blank", "sources",
                               "index_pattern IS NULL OR btrim(index_pattern) <> ''", schema="app")
    op.create_check_constraint(
        "source_type_fields", "sources",
        "(source_type = 'kafka' AND connection_id IS NOT NULL AND "
        "external_connection_id IS NULL AND topic_name IS NOT NULL AND "
        "index_name IS NULL AND index_pattern IS NULL) OR "
        "(source_type = 'external' AND connection_id IS NULL AND "
        "external_connection_id IS NOT NULL AND topic_name IS NULL AND "
        "kafka_topic_identity IS NULL AND normalizer_id IS NULL AND "
        "is_archived = false AND ((index_name IS NOT NULL AND index_pattern IS NULL) OR "
        "(index_name IS NULL AND index_pattern IS NOT NULL)))", schema="app")
    op.create_index("ix_sources_external_connection_id", "sources", ["external_connection_id"],
                    schema="app")
    op.create_index("uq_sources_external_index", "sources", ["external_connection_id", "index_name"],
                    unique=True, schema="app",
                    postgresql_where=sa.text("source_type = 'external' AND index_name IS NOT NULL"))
    op.create_index("uq_sources_external_pattern", "sources", ["external_connection_id", "index_pattern"],
                    unique=True, schema="app",
                    postgresql_where=sa.text("source_type = 'external' AND index_pattern IS NOT NULL"))


def downgrade() -> None:
    bind = op.get_bind()
    if bind.scalar(sa.text("SELECT EXISTS (SELECT 1 FROM app.sources WHERE source_type = 'external')")):
        raise RuntimeError("downgrade would discard external sources")
    if bind.scalar(sa.text("SELECT EXISTS (SELECT 1 FROM app.external_connections)")):
        raise RuntimeError("downgrade would discard external connections")
    op.drop_index("uq_sources_external_pattern", table_name="sources", schema="app")
    op.drop_index("uq_sources_external_index", table_name="sources", schema="app")
    op.drop_index("ix_sources_external_connection_id", table_name="sources", schema="app")
    op.drop_constraint("source_type_fields", "sources", schema="app", type_="check")
    op.drop_constraint("index_name_not_blank", "sources", schema="app", type_="check")
    op.drop_constraint("index_pattern_not_blank", "sources", schema="app", type_="check")
    op.drop_constraint("topic_name_not_blank", "sources", schema="app", type_="check")
    op.alter_column("sources", "topic_name", nullable=False, schema="app")
    op.alter_column("sources", "connection_id", nullable=False, schema="app")
    op.create_check_constraint("topic_name_not_blank", "sources",
                               "btrim(topic_name) <> ''", schema="app")
    op.drop_constraint("fk_sources_external_connection_id_external_connections", "sources",
                       schema="app", type_="foreignkey")
    op.drop_column("sources", "index_pattern", schema="app")
    op.drop_column("sources", "index_name", schema="app")
    op.drop_column("sources", "external_connection_id", schema="app")
    op.drop_column("sources", "source_type", schema="app")
    op.drop_table("external_connections", schema="app")
