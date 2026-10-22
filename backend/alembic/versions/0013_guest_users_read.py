"""Revoke administration read access from the system Guest role."""

from alembic import op

revision = "0013_guest_users_read"
down_revision = "0012_agent_cluster_route"
branch_labels = None
depends_on = None

GUEST_ID = "00000000-0000-4000-8000-000000000002"


def upgrade() -> None:
    op.execute(f"""
        UPDATE app.roles
        SET permissions = permissions - 'users.read'
        WHERE id = '{GUEST_ID}'
          AND permissions ? 'users.read'
    """)


def downgrade() -> None:
    op.execute(f"""
        UPDATE app.roles
        SET permissions = permissions || '["users.read"]'::jsonb
        WHERE id = '{GUEST_ID}'
          AND NOT permissions ? 'users.read'
    """)
