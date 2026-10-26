"""Store one versioned OIDC provider and revoke guest settings access."""

import sqlalchemy as sa

from alembic import op

revision = "0014_oidc_provider"
down_revision = "0013_guest_users_read"
branch_labels = None
depends_on = None

GUEST_ID = "00000000-0000-4000-8000-000000000002"


def upgrade() -> None:
    op.create_table(
        "oidc_provider",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("active_version", sa.Integer()),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("draft", sa.JSON()),
        sa.Column("active", sa.JSON()),
        sa.Column("checked_version", sa.Integer()),
        sa.Column("trial_version", sa.Integer()),
        sa.Column("trial_session_id", sa.Uuid()),
        sa.Column("trial_expires_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("id = 1", name="single_provider"),
        sa.CheckConstraint("mode IN ('env', 'managed', 'disabled')", name="mode_valid"),
        schema="app",
    )
    op.execute("INSERT INTO app.oidc_provider (id, version, mode) VALUES (1, 0, 'env')")
    op.execute(f"""
        UPDATE app.roles SET permissions = permissions - 'settings.read'
        WHERE id = '{GUEST_ID}' AND permissions ? 'settings.read'
    """)


def downgrade() -> None:
    op.drop_table("oidc_provider", schema="app")
    op.execute(f"""
        UPDATE app.roles SET permissions = permissions || '["settings.read"]'::jsonb
        WHERE id = '{GUEST_ID}' AND NOT permissions ? 'settings.read'
    """)
