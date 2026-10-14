"""Add durable Fluent Bit agent registration and Kafka provisioning state."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0011_fluentbit_agents"
down_revision = "0010_data_stream_targets"
branch_labels = None
depends_on = None


def upgrade() -> None:
    uuid = postgresql.UUID(as_uuid=True)
    now = sa.text("CURRENT_TIMESTAMP")
    op.create_table(
        "fluentbit_agents",
        sa.Column("id", uuid, primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text()),
        sa.Column("state", sa.Text(), nullable=False, server_default=sa.text("'provisioning'")),
        sa.Column("access_revision", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("safe_error", sa.Text()),
        sa.Column("pending_credentials_ciphertext", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=now),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=now),
        sa.CheckConstraint("btrim(name) <> ''", name="name_not_blank"),
        sa.CheckConstraint(
            "state IN ('provisioning', 'ready', 'error', 'revocation_pending', 'revoked')",
            name="state_valid",
        ),
        sa.CheckConstraint("access_revision > 0", name="access_revision_positive"),
        schema="app",
    )
    op.create_table(
        "fluentbit_agent_topics",
        sa.Column("id", uuid, primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column(
            "agent_id",
            uuid,
            sa.ForeignKey("app.fluentbit_agents.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "connection_id",
            uuid,
            sa.ForeignKey("app.kafka_connections.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "source_id", uuid, sa.ForeignKey("app.sources.id", ondelete="RESTRICT"), nullable=False
        ),
        sa.Column("cluster_identity", sa.Text(), nullable=False),
        sa.Column("topic_name", sa.Text(), nullable=False),
        sa.Column("topic_identity", sa.Text()),
        sa.Column("state", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=now),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=now),
        sa.CheckConstraint("btrim(cluster_identity) <> ''", name="cluster_not_blank"),
        sa.CheckConstraint("btrim(topic_name) <> ''", name="topic_not_blank"),
        sa.CheckConstraint(
            "topic_identity IS NULL OR btrim(topic_identity) <> ''", name="topic_identity_not_blank"
        ),
        sa.CheckConstraint(
            "state IN ('pending', 'ready', 'error', 'removal_pending', 'revocation_pending')",
            name="state_valid",
        ),
        sa.CheckConstraint(
            "state <> 'ready' OR topic_identity IS NOT NULL", name="ready_has_identity"
        ),
        sa.UniqueConstraint(
            "cluster_identity", "topic_name", name="uq_agent_topics_cluster_topic_name"
        ),
        schema="app",
    )
    op.create_index(
        "uq_agent_topics_physical_topic",
        "fluentbit_agent_topics",
        ["cluster_identity", "topic_identity"],
        unique=True,
        schema="app",
        postgresql_where=sa.text("topic_identity IS NOT NULL"),
    )
    op.create_index(
        "ix_agent_topics_agent_id", "fluentbit_agent_topics", ["agent_id"], schema="app"
    )
    op.create_table(
        "kafka_provisioning_operations",
        sa.Column("id", uuid, primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column(
            "agent_id",
            uuid,
            sa.ForeignKey("app.fluentbit_agents.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("request_key", sa.Text(), nullable=False, unique=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("safe_error", sa.Text()),
        sa.Column("owner_token", uuid),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=now),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=now),
        sa.CheckConstraint(
            "kind IN ('register', 'add_topic', 'remove_topic', 'revoke', 'rotate')",
            name="kind_valid",
        ),
        sa.CheckConstraint(
            "state IN ('pending', 'running', 'retry', 'succeeded', 'failed')", name="state_valid"
        ),
        sa.CheckConstraint("attempts >= 0", name="attempts_nonnegative"),
        schema="app",
    )
    op.create_index(
        "ix_kafka_provisioning_operations_queue",
        "kafka_provisioning_operations",
        ["state", "updated_at"],
        schema="app",
    )
    op.execute(
        'UPDATE app.roles SET permissions = permissions || \'["agents.read", "agents.write"]\'::jsonb WHERE id = \'00000000-0000-4000-8000-000000000001\' AND NOT permissions @> \'["agents.read", "agents.write"]\'::jsonb'
    )
    op.execute(
        "UPDATE app.roles SET permissions = permissions || '[\"agents.read\"]'::jsonb WHERE id = '00000000-0000-4000-8000-000000000002' AND NOT permissions @> '[\"agents.read\"]'::jsonb"
    )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.scalar(sa.text("SELECT EXISTS (SELECT 1 FROM app.fluentbit_agents)")):
        raise RuntimeError("downgrade would discard registered agents")
    op.execute("UPDATE app.roles SET permissions = permissions - 'agents.read' - 'agents.write'")
    op.drop_index(
        "ix_kafka_provisioning_operations_queue",
        table_name="kafka_provisioning_operations",
        schema="app",
    )
    op.drop_table("kafka_provisioning_operations", schema="app")
    op.drop_index("ix_agent_topics_agent_id", table_name="fluentbit_agent_topics", schema="app")
    op.drop_index(
        "uq_agent_topics_physical_topic", table_name="fluentbit_agent_topics", schema="app"
    )
    op.drop_table("fluentbit_agent_topics", schema="app")
    op.drop_table("fluentbit_agents", schema="app")
