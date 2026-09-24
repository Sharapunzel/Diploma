"""Convert normalizer rules to JSONB while preserving legacy rule text.

Revision ID: 0004_normalizer_json_rules
Revises: 0003_parsed_logs_search
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0004_normalizer_json_rules"
down_revision = "0003_parsed_logs_search"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "normalizers", "rule", schema="app",
        existing_type=sa.Text(), type_=postgresql.JSONB(),
        postgresql_using="jsonb_build_object('format_version', 0, 'legacy_rule_text', rule)",
    )
    op.create_check_constraint(
        "rule_object", "normalizers", "jsonb_typeof(rule) = 'object'", schema="app"
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("ck_normalizers_rule_object"),
        "normalizers", schema="app", type_="check",
    )
    op.alter_column(
        "normalizers", "rule", schema="app",
        existing_type=postgresql.JSONB(), type_=sa.Text(),
        postgresql_using=(
            "CASE WHEN rule->>'format_version' = '0' "
            "AND rule ? 'legacy_rule_text' THEN rule->>'legacy_rule_text' "
            "ELSE rule::text END"
        ),
    )
