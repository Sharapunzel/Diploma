"""Add durable source identity to processing errors and raw read permission."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0008_processing_error_identity"
down_revision = "0007_kafka_generations_and_gaps"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "processing_errors",
        sa.Column("source_identity", postgresql.UUID(as_uuid=True), nullable=True),
        schema="logs",
    )
    op.execute(
        "UPDATE logs.processing_errors SET source_identity = source_id WHERE source_id IS NOT NULL"
    )
    op.create_index(
        "ix_processing_errors_source_identity_processed",
        "processing_errors",
        ["source_identity", "backend_processed_at"],
        schema="logs",
    )
    op.execute("""
        UPDATE app.roles SET permissions = permissions || '["processing_errors.raw.read"]'::jsonb
        WHERE id = '00000000-0000-4000-8000-000000000001'
          AND NOT permissions @> '["processing_errors.raw.read"]'::jsonb
    """)


def downgrade():
    op.execute("""
        UPDATE app.roles SET permissions = permissions - 'processing_errors.raw.read'
        WHERE id = '00000000-0000-4000-8000-000000000001'
    """)
    op.drop_index(
        "ix_processing_errors_source_identity_processed",
        table_name="processing_errors",
        schema="logs",
    )
    op.drop_column("processing_errors", "source_identity", schema="logs")
