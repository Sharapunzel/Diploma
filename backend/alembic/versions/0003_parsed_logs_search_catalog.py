"""Add normalizer snapshot and raw substring index.

Revision ID: 0003_parsed_logs_search
Revises: 0002_auth_sessions
"""

import sqlalchemy as sa

from alembic import op

revision = "0003_parsed_logs_search"
down_revision = "0002_auth_sessions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.add_column(
        "parsed_logs",
        sa.Column("normalizer_name", sa.Text(), nullable=True),
        schema="logs",
    )
    op.execute(
        """
        UPDATE logs.parsed_logs AS parsed
        SET normalizer_name = normalizers.name
        FROM app.normalizers AS normalizers
        WHERE parsed.normalizer_id = normalizers.id
        """
    )
    op.execute(
        "UPDATE logs.parsed_logs SET normalizer_name = 'legacy-unknown' "
        "WHERE normalizer_name IS NULL"
    )
    op.alter_column(
        "parsed_logs", "normalizer_name", nullable=False, schema="logs"
    )
    op.create_index(
        "ix_parsed_logs_raw_trgm",
        "parsed_logs",
        ["raw"],
        unique=False,
        schema="logs",
        postgresql_using="gin",
        postgresql_ops={"raw": "gin_trgm_ops"},
    )


def downgrade() -> None:
    op.drop_index("ix_parsed_logs_raw_trgm", table_name="parsed_logs", schema="logs")
    op.drop_column("parsed_logs", "normalizer_name", schema="logs")
