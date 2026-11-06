"""Durable detector state, warmup inputs and findings."""
from alembic import op

revision = "0018_detector_core"
down_revision = "0017_capture_writer_guard"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE TABLE logs.detectors (\n\tid UUID NOT NULL, \n\taggregate_id UUID NOT NULL, \n\tname TEXT NOT NULL, \n\tcreated_by UUID NOT NULL, \n\tcreated_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tconfig JSONB NOT NULL, \n\tconfig_hash TEXT NOT NULL, \n\tfeature_keys JSONB NOT NULL, \n\tfeature_names JSONB NOT NULL, \n\trule_hash TEXT NOT NULL, \n\tstate TEXT NOT NULL, \n\treason TEXT, \n\trevision BIGINT NOT NULL, \n\taccelerated BOOLEAN NOT NULL, \n\tstep_seconds INTEGER NOT NULL, \n\taccelerated_used BOOLEAN NOT NULL, \n\tactive_attempt_id UUID, \n\tevaluation_start_index BIGINT, \n\tconsumed_index BIGINT, \n\tevaluated_index BIGINT, \n\tcheckpoint JSONB, \n\tupdated_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tCONSTRAINT pk_detectors PRIMARY KEY (id), \n\tCONSTRAINT ck_detectors_state_valid CHECK (state IN ('warming','waiting','ready','stopped','error')), \n\tCONSTRAINT ck_detectors_revision_valid CHECK (revision >= 0), \n\tCONSTRAINT fk_detectors_aggregate_id_aggregates FOREIGN KEY(aggregate_id) REFERENCES logs.aggregates (id) ON DELETE RESTRICT\n)")
    op.execute('CREATE INDEX ix_detector_aggregate ON logs.detectors (aggregate_id)')
    op.execute("CREATE TABLE logs.detector_warmups (\n\tid UUID NOT NULL, \n\tdetector_id UUID NOT NULL, \n\tcutoff TIMESTAMP WITH TIME ZONE NOT NULL, \n\tplan JSONB NOT NULL, \n\tinputs JSONB NOT NULL, \n\tinputs_hash TEXT NOT NULL, \n\tstatus TEXT NOT NULL, \n\treason TEXT, \n\tcreated_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tCONSTRAINT pk_detector_warmups PRIMARY KEY (id), \n\tCONSTRAINT ck_detector_warmups_inputs_array CHECK (jsonb_typeof(inputs) = 'array'), \n\tCONSTRAINT fk_detector_warmups_detector_id_detectors FOREIGN KEY(detector_id) REFERENCES logs.detectors (id) ON DELETE RESTRICT\n)")
    op.execute('CREATE INDEX ix_logs_detector_warmups_detector_id ON logs.detector_warmups (detector_id)')
    op.execute("CREATE TABLE logs.detector_outcomes (\n\tid UUID NOT NULL, \n\tdetector_id UUID NOT NULL, \n\tworking_point_id UUID NOT NULL, \n\twindow_index BIGINT NOT NULL, \n\trevision BIGINT NOT NULL, \n\tkind TEXT NOT NULL, \n\treason TEXT, \n\tdata JSONB NOT NULL, \n\tCONSTRAINT pk_detector_outcomes PRIMARY KEY (id), \n\tCONSTRAINT ck_detector_outcomes_kind_valid CHECK (kind IN ('evaluated','skipped')), \n\tCONSTRAINT fk_detector_outcomes_detector_id_detectors FOREIGN KEY(detector_id) REFERENCES logs.detectors (id) ON DELETE RESTRICT, \n\tCONSTRAINT fk_detector_outcomes_working_point_id_working_window_points FOREIGN KEY(working_point_id) REFERENCES logs.working_window_points (id) ON DELETE RESTRICT\n)")
    op.execute('CREATE UNIQUE INDEX uq_detector_outcome_input ON logs.detector_outcomes (detector_id, working_point_id)')
    op.execute('CREATE TABLE logs.findings (\n\tid UUID NOT NULL, \n\tdetector_id UUID NOT NULL, \n\tworking_point_id UUID NOT NULL, \n\tcreated_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tlate BOOLEAN DEFAULT false NOT NULL, \n\tdata JSONB NOT NULL, \n\tCONSTRAINT pk_findings PRIMARY KEY (id), \n\tCONSTRAINT fk_findings_detector_id_detectors FOREIGN KEY(detector_id) REFERENCES logs.detectors (id) ON DELETE RESTRICT, \n\tCONSTRAINT fk_findings_working_point_id_working_window_points FOREIGN KEY(working_point_id) REFERENCES logs.working_window_points (id) ON DELETE RESTRICT\n)')
    op.execute('CREATE UNIQUE INDEX uq_finding_input ON logs.findings (detector_id, working_point_id)')

    op.create_foreign_key("fk_detector_active_attempt", "detectors", "detector_warmups",
        ["active_attempt_id"], ["id"], source_schema="logs", referent_schema="logs",
        ondelete="RESTRICT", deferrable=True, initially="DEFERRED")
    op.create_check_constraint("step_valid", "detectors", "step_seconds > 0", schema="logs")
    op.create_check_constraint("start_valid", "detectors", "evaluation_start_index IS NULL OR evaluation_start_index >= 0", schema="logs")
    op.create_check_constraint("status_valid", "detector_warmups", "status IN ('preparing','prepared','waiting','ready')", schema="logs")
    op.execute("""CREATE FUNCTION logs.protect_detector_definition() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (NEW.aggregate_id,NEW.config,NEW.config_hash,NEW.feature_keys,NEW.feature_names,NEW.rule_hash,
            NEW.accelerated,NEW.step_seconds) IS DISTINCT FROM
           (OLD.aggregate_id,OLD.config,OLD.config_hash,OLD.feature_keys,OLD.feature_names,OLD.rule_hash,
            OLD.accelerated,OLD.step_seconds) OR NEW.revision <> OLD.revision + 1 THEN
          RAISE EXCEPTION 'immutable detector configuration or invalid revision' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$""")
    op.execute("CREATE TRIGGER protect_detector_definition BEFORE UPDATE ON logs.detectors FOR EACH ROW EXECUTE FUNCTION logs.protect_detector_definition()")
    op.execute("""CREATE FUNCTION logs.protect_warmup_inputs() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (NEW.detector_id,NEW.cutoff,NEW.plan) IS DISTINCT FROM (OLD.detector_id,OLD.cutoff,OLD.plan)
           OR (OLD.status <> 'preparing' AND
               (NEW.inputs,NEW.inputs_hash) IS DISTINCT FROM (OLD.inputs,OLD.inputs_hash)) THEN
          RAISE EXCEPTION 'immutable warmup inputs' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$""")
    op.execute("CREATE TRIGGER protect_warmup_inputs BEFORE UPDATE ON logs.detector_warmups FOR EACH ROW EXECUTE FUNCTION logs.protect_warmup_inputs()")


def downgrade():
    op.drop_constraint("fk_detector_active_attempt", "detectors", schema="logs", type_="foreignkey")
    op.execute("DROP TRIGGER protect_detector_definition ON logs.detectors")
    op.execute("DROP FUNCTION logs.protect_detector_definition()")
    op.execute("DROP TRIGGER protect_warmup_inputs ON logs.detector_warmups")
    op.execute("DROP FUNCTION logs.protect_warmup_inputs()")
    op.drop_table('findings', schema="logs")
    op.drop_table('detector_outcomes', schema="logs")
    op.drop_table('detector_warmups', schema="logs")
    op.drop_table('detectors', schema="logs")
