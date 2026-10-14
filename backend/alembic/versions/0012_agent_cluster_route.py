"""Keep the Kafka administration route after the last topic is removed."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0012_agent_cluster_route"
down_revision = "0011_fluentbit_agents"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "fluentbit_agents", sa.Column("connection_id", postgresql.UUID(as_uuid=True)), schema="app"
    )
    op.add_column("fluentbit_agents", sa.Column("cluster_identity", sa.Text()), schema="app")
    op.execute("""
        UPDATE app.fluentbit_agents AS agent
        SET connection_id = (operation.payload->'topics'->0->>'connection_id')::uuid,
            cluster_identity = operation.payload->'topics'->0->>'cluster_identity'
        FROM app.kafka_provisioning_operations AS operation
        WHERE operation.agent_id = agent.id AND operation.kind = 'register'
    """)
    bind = op.get_bind()
    if bind.scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM app.fluentbit_agents WHERE connection_id IS NULL OR cluster_identity IS NULL)"
        )
    ):
        raise RuntimeError("registered agent lacks a Kafka cluster route")
    op.alter_column("fluentbit_agents", "cluster_identity", nullable=False, schema="app")
    op.create_foreign_key(
        "fk_agent_connection",
        "fluentbit_agents",
        "kafka_connections",
        ["connection_id"],
        ["id"],
        source_schema="app",
        referent_schema="app",
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        "cluster_not_blank", "fluentbit_agents", "btrim(cluster_identity) <> ''", schema="app"
    )
    op.create_check_constraint(
        "active_has_connection",
        "fluentbit_agents",
        "state = 'revoked' OR connection_id IS NOT NULL",
        schema="app",
    )


def downgrade() -> None:
    op.drop_constraint("active_has_connection", "fluentbit_agents", schema="app", type_="check")
    op.drop_constraint("cluster_not_blank", "fluentbit_agents", schema="app", type_="check")
    op.drop_constraint("fk_agent_connection", "fluentbit_agents", schema="app", type_="foreignkey")
    op.drop_column("fluentbit_agents", "cluster_identity", schema="app")
    op.drop_column("fluentbit_agents", "connection_id", schema="app")
