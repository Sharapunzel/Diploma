"""Upgrade populated 0020 data and reject every obsolete writer after retirement."""

import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from app.aggregate_composition import build_aggregate_service
from app.core.errors import DomainError
from app.db import Database
from app.ecs import PackagedEcsCatalog
from app.schemas.aggregates import CreateAggregateRequest


@pytest.fixture
def legacy_database():
    base = make_url(os.environ["TEST_DATABASE_URL"])
    assert base.database == "diploma_test" or (base.database or "").startswith("diploma_test_review_")
    name = "diploma_test_retire_" + uuid4().hex[:12]
    url = base.set(database=name).render_as_string(hide_password=False)
    admin_url = base.set(drivername="postgresql", database="postgres").render_as_string(hide_password=False)
    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        engine = create_engine(url)
        try:
            env = {**os.environ, "DATABASE_URL": url}
            env.pop("DATABASE_URL_FILE", None)

            def migrate(*args, check=True):
                return subprocess.run([sys.executable, "-B", "-m", "alembic", *args],
                    env=env, check=check, capture_output=True, text=True)

            migrate("upgrade", "0020_detector_commands")
            yield engine, url, migrate
        finally:
            engine.dispose()
            admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))


def test_empty_downgrade_and_upgrade(legacy_database):
    _, _, migrate = legacy_database
    for args in (("upgrade", "head"), ("check",), ("downgrade", "0020_detector_commands"),
                 ("upgrade", "head"), ("check",)):
        migrate(*args)


@pytest.mark.parametrize("basis", [None, "backend_processed_at_historical"])
@pytest.mark.parametrize("lifecycle", ["active", "archived", "deleted"])
def test_populated_retirement_replay_and_old_writer_fences(legacy_database, basis, lifecycle):
    engine, url, migrate = legacy_database
    meta = MetaData()
    meta.reflect(engine, schema="logs", only=["aggregate_schedules", "aggregates", "working_window_points",
        "detectors", "detector_warmups", "detector_commands", "findings", "detector_outcomes"])
    tables = {name: meta.tables["logs." + name] for name in (
        "aggregate_schedules", "aggregates", "working_window_points", "detectors", "detector_warmups",
        "detector_commands", "findings", "detector_outcomes")}
    actor, source, schedule, aggregate, point, detector, attempt = [uuid4() for _ in range(7)]
    now = datetime.now(UTC)
    anchor = (now - timedelta(days=1)).replace(microsecond=0)
    rule = {"features": [{"key": "n", "name": "N", "op": "count"}]}
    body = CreateAggregateRequest(source_id=source, aggregate_rule=rule,
        schedule={"boundary": anchor, "timezone": "UTC", "duration_seconds": 300},
        window_start_utc=anchor, time_basis=basis, name="Before upgrade", rule_hash="f" * 64)
    document = body.model_dump(mode="json")
    document["time_basis"] = basis or "availability_at"
    for feature in document["aggregate_rule"]["features"]:
        feature.pop("percentile", None)
    fingerprint = hashlib.sha256(json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    checkpoint = {"body": {"private": "preserve byte-for-byte", "exact": 2**60 + 1}, "sha256": "a" * 64}
    inputs = [{"features": [{"key": "n", "value": 2**60 + 1, "reason": None,
        "valid_count": 1, "invalid_count": 0}], "basis": "availability_at"}]
    with engine.begin() as connection:
        connection.execute(tables["aggregate_schedules"].insert(), {"id": schedule, "source_identity": source,
            "anchor_utc": anchor, "duration_seconds": 300, "next_index": 5})
        connection.execute(tables["aggregates"].insert(), {"id": aggregate, "schedule_id": schedule,
            "source_identity": source, "source_name": "Old source", "source_type": "kafka", "name": body.name,
            "rule": rule, "rule_hash": body.rule_hash, "original_boundary": body.schedule.boundary.isoformat(), "timezone": "UTC",
            "created_at": now, "created_by": actor, "state": lifecycle, "request_key": f"{actor}:old", "request_hash": fingerprint})
        connection.execute(tables["working_window_points"].insert(), {"id": point, "schedule_id": schedule,
            "window_index": 0, "start_utc": anchor, "end_utc": anchor + timedelta(seconds=300),
            "scheduled_at": anchor + timedelta(seconds=300), "actual_started_at": now, "actual_completed_at": now,
            "event_count": 1, "features": inputs[0]["features"], "completeness": "unknown", "completeness_reason": "old",
            "evidence_refs": {"old": True}})
        connection.execute(tables["detectors"].insert(), {"id": detector, "aggregate_id": aggregate, "name": "Old detector",
            "created_by": actor, "created_at": now, "config": {"sample_size": 8}, "config_hash": "a" * 64,
            "feature_keys": ["n"], "feature_names": ["N"], "rule_hash": body.rule_hash, "state": "ready", "revision": 5,
            "accelerated": False, "step_seconds": 300, "accelerated_used": False, "evaluation_start_index": 0,
            "consumed_index": 0, "evaluated_index": 0, "checkpoint": checkpoint, "updated_at": now,
            "execution_state": "running", "execution_generation": 7})
        connection.execute(tables["detector_warmups"].insert(), {"id": attempt, "detector_id": detector, "cutoff": now,
            "plan": {"old": True}, "inputs": inputs, "inputs_hash": "b" * 64, "status": "ready", "created_at": now})
        connection.execute(tables["detectors"].update().where(tables["detectors"].c.id == detector).values(
            active_attempt_id=attempt, revision=6))
        connection.execute(tables["detector_commands"].insert(), {"id": uuid4(), "actor_id": actor, "action": "start",
            "key": "old-start", "fingerprint": "c" * 64, "detector_id": detector, "recorded_at": now,
            "execution_generation": 7, "start_request_id": uuid4()})
        connection.execute(tables["detector_outcomes"].insert(), {"id": uuid4(), "detector_id": detector,
            "working_point_id": point, "window_index": 0, "revision": 4, "kind": "evaluated", "data": {"old": True}})
        connection.execute(tables["findings"].insert(), {"id": uuid4(), "detector_id": detector, "working_point_id": point,
            "created_at": now, "late": False, "data": {"actual": [2**60 + 1], "expected": [1], "origin": "working"}})
    migrate("upgrade", "head")
    migrate("check")
    with engine.connect() as connection:
        row = connection.execute(text("SELECT * FROM logs.detectors WHERE id=:id"), {"id": detector}).mappings().one()
        assert (row["checkpoint"], row["active_attempt_id"], row["consumed_index"], row["evaluated_index"]) == (checkpoint, attempt, 0, 0)
        assert (row["execution_state"], row["failure_code"], row["execution_generation"], row["revision"]) == (
                "terminated", "aggregate_time_semantics_retired", 8, 9)
        assert connection.scalar(text("SELECT inputs FROM logs.detector_warmups WHERE id=:id"), {"id": attempt}) == inputs
        finding = connection.scalar(text("SELECT data FROM logs.findings WHERE detector_id=:id"), {"id": detector})
        assert finding["actual"] == [2**60 + 1] and finding["expected"] == [1] and finding["origin"] == "working"
        assert finding["origin_point_id"] == str(point) and finding["time_basis"] == "availability_at"
        assert connection.scalar(text("SELECT state FROM logs.aggregates WHERE id=:id"), {"id": aggregate}) == lifecycle
    old_columns = [c.name for c in tables["detectors"].columns]
    obsolete_create = "INSERT INTO logs.detectors (" + ",".join(old_columns) + ") SELECT " + ",".join(
        "gen_random_uuid()" if name == "id" else name for name in old_columns
    ) + " FROM logs.detectors WHERE id=:detector"
    for statement in (
        obsolete_create,
        "UPDATE logs.aggregate_schedules SET next_index=next_index+1 WHERE id=:schedule",
        "UPDATE logs.detectors SET checkpoint='{}',revision=revision+1 WHERE id=:detector",
        "UPDATE logs.detectors SET execution_state='requested',revision=revision+1 WHERE id=:detector",
        "INSERT INTO logs.working_window_points (id,schedule_id,window_index,start_utc,end_utc,scheduled_at,actual_started_at,actual_completed_at,event_count,event_time_min,event_time_max,features,completeness,completeness_reason,evidence_refs) SELECT gen_random_uuid(),schedule_id,window_index+1,start_utc+interval '300s',end_utc+interval '300s',scheduled_at+interval '300s',actual_started_at,actual_completed_at,event_count,event_time_min,event_time_max,features,completeness,completeness_reason,evidence_refs FROM logs.working_window_points WHERE id=:point",
        "INSERT INTO logs.detector_outcomes SELECT gen_random_uuid(),detector_id,working_point_id,window_index,revision,kind,reason,data FROM logs.detector_outcomes WHERE detector_id=:detector",
        "INSERT INTO logs.findings SELECT gen_random_uuid(),detector_id,working_point_id,created_at,late,data FROM logs.findings WHERE detector_id=:detector",
    ):
        with engine.begin() as connection, pytest.raises(DBAPIError) as error:
            connection.execute(text(statement), {"schedule": schedule, "detector": detector, "point": point})
        assert error.value.orig.sqlstate == "23514" and "aggregate_time_semantics_retired" in str(error.value.orig)
    db = Database(url)
    try:
        with db.session_factory() as session:
            service = build_aggregate_service(session, PackagedEcsCatalog.load())
            card = service.create(body, actor, "old")
            assert card.id == aggregate and card.execution_mode == "retired" and card.state == lifecycle
            assert service.copy(aggregate).aggregate_rule.model_dump(exclude_none=True) == rule
            assert service.point_by_id(aggregate, point).features[0].value == 2**60 + 1
            with pytest.raises(DomainError) as error:
                service.create(body, actor, "new")
            assert error.value.code == "legacy_calculation_removed"
    finally:
        db.dispose()
    refused = migrate("downgrade", "0020_detector_commands", check=False)
    assert refused.returncode != 0 and "explicit data-preserving migration" in refused.stderr
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0024_aggregate_execution_budget"
        assert connection.scalar(text("SELECT checkpoint FROM logs.detectors WHERE id=:id"), {"id": detector}) == checkpoint
