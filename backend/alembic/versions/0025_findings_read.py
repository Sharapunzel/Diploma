"""Grant access to stored findings to the two system roles."""

from alembic import op

revision = "0025_findings_read"
down_revision = "0024_aggregate_execution_budget"
branch_labels = None
depends_on = None

SYSTEM_ROLES = """id IN ('00000000-0000-4000-8000-000000000001',
                         '00000000-0000-4000-8000-000000000002')"""


def upgrade():
    op.execute(f"""UPDATE app.roles SET permissions = permissions || '["findings.read"]'::jsonb
        WHERE {SYSTEM_ROLES} AND NOT permissions ? 'findings.read'""")


def downgrade():
    op.execute(f"""UPDATE app.roles SET permissions = permissions - 'findings.read'
        WHERE {SYSTEM_ROLES}""")
