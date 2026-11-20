"""Index the exact event-time classification without changing retained ECS."""

import sqlalchemy as sa
from alembic import op

revision = "0022_event_time_projection"
down_revision = "0021_event_time"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("parsed_logs", sa.Column("event_time_utc", sa.DateTime(timezone=False)), schema="logs")
    op.add_column("parsed_logs", sa.Column("event_time_reason", sa.Text()), schema="logs")
    # Explicit ISO components and interval arithmetic use a technical UTC carrier.
    # Neither session TimeZone nor a normalization/availability fallback is used.
    op.execute(r"""CREATE FUNCTION logs.project_event_time(document jsonb, diagnostics jsonb)
      RETURNS TABLE(event_time_utc timestamp without time zone, event_time_reason text)
      LANGUAGE plpgsql IMMUTABLE SET search_path = pg_catalog AS $$
      DECLARE
        raw jsonb := document -> '@timestamp';
        stamp text := raw #>> '{}';
        local_stamp text;
        offset_stamp text;
        parsed timestamp without time zone;
      BEGIN
        IF jsonb_typeof(raw) = 'string' AND stamp ~
          '^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9](\.[0-9]{1,6})?)?)$'
        THEN
          local_stamp := split_part(stamp, 'T', 1) || 'T' || substring(split_part(stamp, 'T', 2)
            FROM '^[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?');
          offset_stamp := CASE WHEN right(stamp, 1) = 'Z' THEN '00:00' ELSE
            substring(split_part(stamp, 'T', 2) FROM '[+-][0-9]{2}:[0-9]{2}(?::[0-9]{2}(?:\.[0-9]{1,6})?)?$') END;
          IF pg_input_is_valid(local_stamp, 'timestamp without time zone')
             AND pg_input_is_valid(offset_stamp, 'interval') THEN
            parsed := local_stamp::timestamp without time zone - offset_stamp::interval;
            IF parsed >= timestamp '0001-01-01 00:00:00'
               AND parsed <= timestamp '9999-12-31 23:59:59.999999' THEN
              event_time_utc := parsed;
            END IF;
          END IF;
        END IF;
        event_time_reason := CASE
          WHEN diagnostics @> '[{"code":"event_time_fallback"}]'::jsonb THEN 'event_time_fallback'
          WHEN raw IS NULL OR jsonb_typeof(raw) = 'null' THEN 'event_time_missing'
          WHEN event_time_utc IS NULL THEN 'event_time_invalid'
          ELSE NULL END;
        RETURN NEXT;
      END $$""")
    op.execute("""UPDATE logs.parsed_logs p SET event_time_utc=v.event_time_utc,
      event_time_reason=v.event_time_reason FROM (
        SELECT p.id, t.* FROM logs.parsed_logs p
        CROSS JOIN LATERAL logs.project_event_time(p.ecs_data,p.normalization_diagnostics) t
      ) v WHERE v.id=p.id""")
    op.execute("""CREATE FUNCTION logs.maintain_event_time_projection() RETURNS trigger
      LANGUAGE plpgsql SET search_path = pg_catalog AS $$
      BEGIN
        SELECT p.event_time_utc,p.event_time_reason INTO NEW.event_time_utc,NEW.event_time_reason
          FROM logs.project_event_time(NEW.ecs_data,NEW.normalization_diagnostics) p;
        RETURN NEW;
      END $$""")
    # Projection-only assignments cannot forge eligibility or leave stale dates.
    op.execute("""CREATE TRIGGER maintain_event_time_projection BEFORE INSERT OR UPDATE OF
      ecs_data,normalization_diagnostics,event_time_utc,event_time_reason ON logs.parsed_logs
      FOR EACH ROW EXECUTE FUNCTION logs.maintain_event_time_projection()""")
    op.create_index("ix_parsed_logs_source_event_time", "parsed_logs",
        ["source_id", "event_time_utc", "id"], schema="logs",
        postgresql_where=sa.text("event_time_reason IS NULL"))


def downgrade():
    op.drop_index("ix_parsed_logs_source_event_time", table_name="parsed_logs", schema="logs")
    op.execute("DROP TRIGGER maintain_event_time_projection ON logs.parsed_logs")
    op.execute("DROP FUNCTION logs.maintain_event_time_projection()")
    op.execute("DROP FUNCTION logs.project_event_time(jsonb,jsonb)")
    op.drop_column("parsed_logs", "event_time_reason", schema="logs")
    op.drop_column("parsed_logs", "event_time_utc", schema="logs")
