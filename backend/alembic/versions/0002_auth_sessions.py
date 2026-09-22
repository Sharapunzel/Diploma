"""Add server sessions and the administrative event-delete permission."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0002_auth_sessions"
down_revision = "0001_database_foundation"
branch_labels = None
depends_on = None


def upgrade():
    uuid = postgresql.UUID(as_uuid=True)
    now = sa.text("CURRENT_TIMESTAMP")
    op.create_table(
        "auth_sessions",
        sa.Column("id", uuid, primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", uuid, sa.ForeignKey("app.users.id", ondelete="SET NULL")),
        sa.Column("token_hash", sa.LargeBinary, nullable=False),
        sa.Column("csrf_token_hash", sa.LargeBinary, nullable=False),
        sa.Column("authentication_method", sa.Text, nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=now, nullable=False),
        sa.Column("last_seen_at", sa.TIMESTAMP(timezone=True), server_default=now, nullable=False),
        sa.Column("expires_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.TIMESTAMP(timezone=True)),
        sa.UniqueConstraint("token_hash"),
        sa.CheckConstraint("octet_length(token_hash) = 32", name="token_hash_sha256"),
        sa.CheckConstraint("octet_length(csrf_token_hash) = 32", name="csrf_token_hash_sha256"),
        sa.CheckConstraint("last_seen_at >= created_at", name="last_seen_after_created"),
        sa.CheckConstraint("last_seen_at <= expires_at", name="last_seen_before_expiry"),
        sa.CheckConstraint("expires_at > created_at", name="expires_after_created"),
        sa.CheckConstraint(
            "revoked_at IS NULL OR revoked_at >= created_at",
            name="revoked_after_created",
        ),
        sa.CheckConstraint("authentication_method IN ('local', 'oidc')", name="auth_method_valid"),
        schema="app",
    )
    op.create_index("ix_auth_sessions_user_id", "auth_sessions", ["user_id"], schema="app")
    op.create_index("ix_auth_sessions_expiry", "auth_sessions", ["expires_at"], schema="app")
    op.execute(
        """
        UPDATE app.roles
        SET permissions = CASE
            WHEN permissions @> '["events.delete"]'::jsonb THEN permissions
            ELSE permissions || '["events.delete"]'::jsonb
        END
        WHERE id = '00000000-0000-4000-8000-000000000001'
        """
    )


def downgrade():
    op.execute(
        """
        UPDATE app.roles
        SET permissions = permissions - 'events.delete'
        WHERE id = '00000000-0000-4000-8000-000000000001'
        """
    )
    op.drop_index("ix_auth_sessions_expiry", table_name="auth_sessions", schema="app")
    op.drop_index("ix_auth_sessions_user_id", table_name="auth_sessions", schema="app")
    op.drop_table("auth_sessions", schema="app")
