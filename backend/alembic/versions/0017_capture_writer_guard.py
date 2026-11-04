"""Reject markerless writers after a controlled capture bootstrap."""

from alembic import op

revision = "0017_capture_writer_guard"
down_revision = "0016_aggregate_core"
branch_labels = None
depends_on = None


def upgrade():
    # Deferred until commit: the supported writer inserts the event first and
    # its marker in the same transaction. Pre-capture legacy imports stay valid.
    op.execute("""CREATE FUNCTION logs.require_event_capture() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF EXISTS (SELECT 1 FROM logs.replication_progress WHERE id=1 AND
              (legacy_cutover_at IS NOT NULL OR availability_verified_from_at IS NOT NULL
               OR boundary_barrier_id IS NOT NULL))
             AND NOT EXISTS (SELECT 1 FROM logs.availability_markers
                 WHERE event_id=NEW.id AND kind='event' AND source_identity=NEW.source_id) THEN
            RAISE EXCEPTION 'availability marker required for event' USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END $$""")
    op.execute("""CREATE CONSTRAINT TRIGGER require_event_capture AFTER INSERT ON logs.parsed_logs
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION logs.require_event_capture()""")


def downgrade():
    op.execute("DROP TRIGGER require_event_capture ON logs.parsed_logs")
    op.execute("DROP FUNCTION logs.require_event_capture()")
