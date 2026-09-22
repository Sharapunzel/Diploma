"""Create application and parsed-log storage."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_database_foundation"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.execute("CREATE SCHEMA app")
    op.execute("CREATE SCHEMA logs")
    uuid = postgresql.UUID(as_uuid=True)
    now = sa.text("CURRENT_TIMESTAMP")
    op.create_table(
        "roles",
        sa.Column(
            "id", uuid, primary_key=True, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("permissions", postgresql.JSONB, nullable=False),
        sa.Column("priority", sa.Integer, nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=now,
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=now,
            nullable=False,
        ),
        sa.UniqueConstraint("name"),
        sa.UniqueConstraint("priority"),
        sa.CheckConstraint(
            "jsonb_typeof(permissions) = 'array'", name="permissions_array"
        ),
        sa.CheckConstraint("priority >= 0", name="priority_nonnegative"),
        schema="app",
    )
    op.create_table(
        "users",
        sa.Column(
            "id", uuid, primary_key=True, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("username", sa.Text),
        sa.Column("password_hash", sa.Text),
        sa.Column("oidc_issuer", sa.Text),
        sa.Column("oidc_subject", sa.Text),
        sa.Column("email", sa.Text),
        sa.Column("display_name", sa.Text, nullable=False),
        sa.Column("role_id", uuid, sa.ForeignKey("app.roles.id", ondelete="SET NULL")),
        sa.Column(
            "role_managed_by_oidc",
            sa.Boolean,
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "is_active", sa.Boolean, server_default=sa.text("true"), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=now,
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=now,
            nullable=False,
        ),
        sa.Column("last_login_at", sa.TIMESTAMP(timezone=True)),
        sa.CheckConstraint(
            "(username IS NULL) = (password_hash IS NULL)",
            name="local_credentials_pair",
        ),
        sa.CheckConstraint(
            "(oidc_issuer IS NULL) = (oidc_subject IS NULL)",
            name="oidc_credentials_pair",
        ),
        sa.CheckConstraint(
            "(username IS NOT NULL AND password_hash IS NOT NULL) OR "
            "(oidc_issuer IS NOT NULL AND oidc_subject IS NOT NULL)",
            name="one_login_method",
        ),
        sa.CheckConstraint(
            "username IS NULL OR btrim(username) <> ''", name="username_not_blank"
        ),
        sa.CheckConstraint(
            "password_hash IS NULL OR btrim(password_hash) <> ''",
            name="password_hash_not_blank",
        ),
        sa.CheckConstraint(
            "oidc_issuer IS NULL OR btrim(oidc_issuer) <> ''",
            name="oidc_issuer_not_blank",
        ),
        sa.CheckConstraint(
            "oidc_subject IS NULL OR btrim(oidc_subject) <> ''",
            name="oidc_subject_not_blank",
        ),
        sa.CheckConstraint("btrim(display_name) <> ''", name="display_name_not_blank"),
        schema="app",
    )
    op.create_index("ix_users_role_id", "users", ["role_id"], schema="app")
    op.execute(
        "CREATE UNIQUE INDEX uq_users_username_lower "
        "ON app.users (lower(username)) WHERE username IS NOT NULL"
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_users_oidc_identity "
        "ON app.users (oidc_issuer, oidc_subject) "
        "WHERE oidc_issuer IS NOT NULL AND oidc_subject IS NOT NULL"
    )
    op.create_table(
        "oidc_role_mappings",
        sa.Column(
            "id", uuid, primary_key=True, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("issuer", sa.Text, nullable=False),
        sa.Column("claim_name", sa.Text, nullable=False),
        sa.Column("claim_value", sa.Text, nullable=False),
        sa.Column("role_id", uuid, sa.ForeignKey("app.roles.id", ondelete="SET NULL")),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=now,
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=now,
            nullable=False,
        ),
        sa.UniqueConstraint("issuer", "claim_name", "claim_value"),
        schema="app",
    )
    op.create_index(
        "ix_oidc_role_mappings_role_id", "oidc_role_mappings", ["role_id"], schema="app"
    )
    op.create_table(
        "app_settings",
        sa.Column(
            "id", uuid, primary_key=True, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("key", sa.Text, nullable=False),
        sa.Column("value", postgresql.JSONB, nullable=False),
        sa.Column("category", sa.Text, nullable=False),
        sa.Column("version", sa.Integer, server_default=sa.text("1"), nullable=False),
        sa.Column(
            "is_public", sa.Boolean, server_default=sa.text("false"), nullable=False
        ),
        sa.Column(
            "updated_by_user_id",
            uuid,
            sa.ForeignKey("app.users.id", ondelete="SET NULL"),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=now,
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=now,
            nullable=False,
        ),
        sa.UniqueConstraint("key"),
        sa.CheckConstraint("btrim(key) <> ''", name="key_not_blank"),
        sa.CheckConstraint("btrim(category) <> ''", name="category_not_blank"),
        sa.CheckConstraint("version > 0", name="version_positive"),
        schema="app",
    )
    op.create_index(
        "ix_app_settings_updated_by_user_id",
        "app_settings",
        ["updated_by_user_id"],
        schema="app",
    )
    op.create_table(
        "normalizers",
        sa.Column(
            "id", uuid, primary_key=True, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("description", sa.Text),
        sa.Column("rule", sa.Text, nullable=False),
        sa.Column("version", sa.Integer, server_default=sa.text("1"), nullable=False),
        sa.Column(
            "created_by_user_id",
            uuid,
            sa.ForeignKey("app.users.id", ondelete="SET NULL"),
        ),
        sa.Column(
            "updated_by_user_id",
            uuid,
            sa.ForeignKey("app.users.id", ondelete="SET NULL"),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=now,
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=now,
            nullable=False,
        ),
        sa.UniqueConstraint("name"),
        sa.CheckConstraint("btrim(name) <> ''", name="name_not_blank"),
        sa.CheckConstraint("version > 0", name="version_positive"),
        schema="app",
    )
    op.create_index(
        "ix_normalizers_created_by_user_id",
        "normalizers",
        ["created_by_user_id"],
        schema="app",
    )
    op.create_index(
        "ix_normalizers_updated_by_user_id",
        "normalizers",
        ["updated_by_user_id"],
        schema="app",
    )
    op.create_table(
        "kafka_connections",
        sa.Column(
            "id", uuid, primary_key=True, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("bootstrap_servers", postgresql.ARRAY(sa.Text), nullable=False),
        sa.Column(
            "security_protocol",
            sa.Text,
            server_default=sa.text("'PLAINTEXT'"),
            nullable=False,
        ),
        sa.Column(
            "extra_config",
            postgresql.JSONB,
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=now,
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=now,
            nullable=False,
        ),
        sa.UniqueConstraint("name"),
        sa.CheckConstraint("btrim(name) <> ''", name="name_not_blank"),
        sa.CheckConstraint(
            "btrim(security_protocol) <> ''", name="security_protocol_not_blank"
        ),
        sa.CheckConstraint(
            "cardinality(bootstrap_servers) >= 1 AND "
            "btrim(array_to_string(bootstrap_servers, '')) <> ''",
            name="bootstrap_servers_nonempty",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(extra_config) = 'object'", name="extra_config_object"
        ),
        schema="app",
    )
    op.create_table(
        "sources",
        sa.Column(
            "id", uuid, primary_key=True, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column(
            "connection_id",
            uuid,
            sa.ForeignKey("app.kafka_connections.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "normalizer_id",
            uuid,
            sa.ForeignKey("app.normalizers.id", ondelete="SET NULL"),
        ),
        sa.Column("topic_name", sa.Text, nullable=False),
        sa.Column(
            "is_enabled", sa.Boolean, server_default=sa.text("false"), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=now,
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=now,
            nullable=False,
        ),
        sa.UniqueConstraint("connection_id", "topic_name"),
        sa.CheckConstraint("btrim(name) <> ''", name="name_not_blank"),
        sa.CheckConstraint("btrim(topic_name) <> ''", name="topic_name_not_blank"),
        schema="app",
    )
    op.create_index(
        "ix_sources_connection_id", "sources", ["connection_id"], schema="app"
    )
    op.create_index(
        "ix_sources_normalizer_id", "sources", ["normalizer_id"], schema="app"
    )
    op.create_table(
        "parsed_logs",
        sa.Column(
            "id", uuid, primary_key=True, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column(
            "source_id", uuid, sa.ForeignKey("app.sources.id", ondelete="SET NULL")
        ),
        sa.Column(
            "connection_id",
            uuid,
            sa.ForeignKey("app.kafka_connections.id", ondelete="SET NULL"),
        ),
        sa.Column(
            "normalizer_id",
            uuid,
            sa.ForeignKey("app.normalizers.id", ondelete="SET NULL"),
        ),
        sa.Column("normalizer_version", sa.Integer, nullable=False),
        sa.Column("source_name", sa.Text, nullable=False),
        sa.Column("connection_name", sa.Text, nullable=False),
        sa.Column("kafka_topic", sa.Text, nullable=False),
        sa.Column("kafka_partition", sa.Integer, nullable=False),
        sa.Column("kafka_offset", sa.BigInteger, nullable=False),
        sa.Column("deduplication_key", sa.Text, nullable=False),
        sa.Column(
            "fluent_bit_collected_at", sa.TIMESTAMP(timezone=True), nullable=False
        ),
        sa.Column("backend_received_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("backend_processed_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("raw", sa.Text, nullable=False),
        sa.Column("ecs_data", postgresql.JSONB, nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=now,
            nullable=False,
        ),
        sa.UniqueConstraint("deduplication_key"),
        sa.CheckConstraint(
            "normalizer_version > 0", name="normalizer_version_positive"
        ),
        sa.CheckConstraint("kafka_partition >= 0", name="kafka_partition_nonnegative"),
        sa.CheckConstraint("kafka_offset >= 0", name="kafka_offset_nonnegative"),
        sa.CheckConstraint("jsonb_typeof(ecs_data) = 'object'", name="ecs_data_object"),
        sa.CheckConstraint(
            "backend_processed_at >= backend_received_at",
            name="processing_after_receipt",
        ),
        schema="logs",
    )
    for name, cols in [
        ("ix_parsed_logs_source_processed", ["source_id", "backend_processed_at"]),
        ("ix_parsed_logs_connection_id", ["connection_id"]),
        ("ix_parsed_logs_normalizer_id", ["normalizer_id"]),
        ("ix_parsed_logs_fluent_collected", ["fluent_bit_collected_at"]),
    ]:
        op.create_index(name, "parsed_logs", cols, schema="logs")
    op.create_index(
        "ix_parsed_logs_ecs_data",
        "parsed_logs",
        ["ecs_data"],
        schema="logs",
        postgresql_using="gin",
    )
    op.execute("""
        CREATE OR REPLACE FUNCTION app.bump_version_and_timestamp()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            NEW.version = OLD.version + 1;
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END
        $$
        """)
    op.execute(
        "CREATE TRIGGER normalizers_bump_version "
        "BEFORE UPDATE ON app.normalizers "
        "FOR EACH ROW EXECUTE FUNCTION app.bump_version_and_timestamp()"
    )
    op.execute(
        "CREATE TRIGGER app_settings_bump_version "
        "BEFORE UPDATE ON app.app_settings "
        "FOR EACH ROW EXECUTE FUNCTION app.bump_version_and_timestamp()"
    )
    op.execute("""
        INSERT INTO app.roles (id, name, permissions, priority)
        VALUES
        (
            '00000000-0000-4000-8000-000000000001',
            'Administrator',
            '["users.read", "users.write", "settings.read", "settings.write",
              "connections.read", "connections.write", "sources.read",
              "sources.write", "normalizers.read", "normalizers.write",
              "events.read"]'::jsonb,
            100
        ),
        (
            '00000000-0000-4000-8000-000000000002',
            'Guest',
            '["users.read", "settings.read", "connections.read", "sources.read",
              "normalizers.read", "events.read"]'::jsonb,
            10
        )
        """)


def downgrade():
    op.execute("DROP TRIGGER IF EXISTS app_settings_bump_version ON app.app_settings")
    op.execute("DROP TRIGGER IF EXISTS normalizers_bump_version ON app.normalizers")
    op.execute("DROP FUNCTION IF EXISTS app.bump_version_and_timestamp()")
    op.drop_table("parsed_logs", schema="logs")
    op.drop_table("sources", schema="app")
    op.drop_table("kafka_connections", schema="app")
    op.drop_table("normalizers", schema="app")
    op.drop_table("app_settings", schema="app")
    op.drop_table("oidc_role_mappings", schema="app")
    op.drop_table("users", schema="app")
    op.drop_table("roles", schema="app")
    op.execute("DROP SCHEMA logs")
    op.execute("DROP SCHEMA app")
    op.execute("DROP EXTENSION pgcrypto")
