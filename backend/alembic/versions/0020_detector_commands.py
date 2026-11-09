"""Atomic detector HTTP receipts and administrator permissions."""
import sqlalchemy as sa

from alembic import op

revision = "0020_detector_commands"
down_revision = "0019_detector_worker"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table("detector_commands",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("actor_id", sa.Uuid(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("fingerprint", sa.Text(), nullable=False),
        sa.Column("detector_id", sa.Uuid(), sa.ForeignKey("logs.detectors.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("execution_generation", sa.BigInteger(), nullable=False),
        sa.Column("start_request_id", sa.Uuid()),
        sa.UniqueConstraint("actor_id", "action", "key", name="uq_detector_command_scope"),
        sa.CheckConstraint("action IN ('create','start','stop')", name="action_valid"),
        sa.CheckConstraint("length(key) BETWEEN 1 AND 200 AND btrim(key) <> ''", name="key_valid"),
        sa.CheckConstraint("length(fingerprint) = 64", name="fingerprint_valid"),
        sa.CheckConstraint("execution_generation >= 0", name="generation_valid"), schema="logs")
    op.execute("""UPDATE app.roles SET permissions = permissions || '["detectors.read", "detectors.write"]'::jsonb
        WHERE id = '00000000-0000-4000-8000-000000000001'""")


def downgrade():
    op.execute("UPDATE app.roles SET permissions = permissions - 'detectors.read' - 'detectors.write'")
    op.drop_table("detector_commands", schema="logs")
