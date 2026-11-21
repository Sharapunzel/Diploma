"""Preserve opaque legacy facts; this fixture is not a live/complete ML proof."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import MetaData, inspect, text
from test_event_time_migration import legacy_database

__all__ = ["legacy_database"]
PREVIOUS = "0023_aggregate_retention"
CURRENT = "0024_aggregate_execution_budget"
ORDERS = {
    "aggregates": "id", "aggregate_schedules": "id", "working_window_points": "id",
    "aggregate_closures": "origin_point_id", "aggregate_memberships": "closure_id,event_id",
    "aggregate_snapshots": "closure_id", "aggregate_retention_state": "aggregate_id",
    "detectors": "id", "detector_warmups": "id", "detector_outcomes": "id", "findings": "id",
}


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def stored_facts(engine, *, remove_new=False):
    facts = {}
    additions = {"aggregate_schedules": ("last_execution",),
        "working_window_points": ("calculation_status", "calculation_reason"),
        "aggregate_closures": ("calculation_status", "calculation_reason", "execution")}
    with engine.connect() as connection:
        for name, order in ORDERS.items():
            rows = connection.scalars(text(f"SELECT to_jsonb(r) FROM logs.{name} r ORDER BY {order}")).all()
            if remove_new:
                for row in rows:
                    for column in additions.get(name, ()):
                        row.pop(column)
            facts[name] = rows
    return facts


def column_contract(engine):
    inspector = inspect(engine)
    return {table: {column["name"]: {key: str(column[key]) if key == "type" else column[key]
            for key in ("type", "nullable", "default")}
        for column in inspector.get_columns(table, schema="logs")}
        for table in ("aggregate_schedules", "aggregate_closures", "working_window_points")}


def seed_opaque_legacy_facts(engine, migrate):
    names = ("aggregate_schedules", "aggregates", "working_window_points", "detectors",
        "detector_warmups", "detector_outcomes", "findings")
    metadata = MetaData()
    metadata.reflect(engine, schema="logs", only=names)
    tables = {name: metadata.tables["logs." + name] for name in names}
    identities = {name: uuid4() for name in ("actor", "source", "schedule", "aggregate",
        "cleared", "present", "detector", "attempt", "outcome", "finding")}
    actor, source, schedule, aggregate = (identities[name] for name in ("actor", "source", "schedule", "aggregate"))
    anchor = (datetime.now(UTC) - timedelta(days=2)).replace(microsecond=0)
    rule = {"features": [{"key": "n", "name": "Legacy number", "op": "count"}]}
    features = [{"key": "n", "value": 2**60 + 1, "reason": None, "valid_count": 1, "invalid_count": 0}]
    inputs = [{"features": features, "basis": "availability_at", "opaque_integer": 2**60 + 1}]
    body = {"legacy_opaque": True, "exact_integer": 2**60 + 1}
    checkpoint = {"body": body, "sha256": fingerprint(body)}
    with engine.begin() as connection:
        connection.execute(tables["aggregate_schedules"].insert(), {"id": schedule, "source_identity": source,
            "anchor_utc": anchor, "duration_seconds": 300, "next_index": 2})
        connection.execute(tables["aggregates"].insert(), {"id": aggregate, "schedule_id": schedule,
            "source_identity": source, "source_name": "Legacy migration source", "source_type": "kafka",
            "name": "Before execution budget", "rule": rule, "rule_hash": fingerprint(rule),
            "original_boundary": anchor.isoformat(), "timezone": "UTC", "created_at": anchor,
            "created_by": actor, "state": "archived", "request_key": uuid4().hex,
            "request_hash": fingerprint({"legacy": True})})
        for index, name in enumerate(("cleared", "present")):
            start, end = anchor + timedelta(seconds=index * 300), anchor + timedelta(seconds=(index + 1) * 300)
            connection.execute(tables["working_window_points"].insert(), {"id": identities[name],
                "schedule_id": schedule, "window_index": index, "start_utc": start, "end_utc": end,
                "scheduled_at": end, "actual_started_at": end, "actual_completed_at": end,
                "event_count": 1, "features": features, "completeness": "unknown",
                "completeness_reason": "legacy migration fixture", "evidence_refs": {
                    "exact_integer": 2**60 + 1, "inputs_hash": fingerprint(inputs)}})
        connection.execute(tables["detectors"].insert(), {"id": identities["detector"], "aggregate_id": aggregate,
            "name": "Opaque legacy detector", "created_by": actor, "created_at": anchor,
            "config": {"sample_size": 8}, "config_hash": "a" * 64, "rule_hash": fingerprint(rule),
            "feature_keys": ["n"], "feature_names": ["Legacy number"], "state": "ready", "revision": 5,
            "accelerated": False, "step_seconds": 300, "accelerated_used": False, "evaluation_start_index": 0,
            "consumed_index": 1, "evaluated_index": 1, "checkpoint": checkpoint, "updated_at": anchor,
            "execution_state": "running", "execution_generation": 7})
        connection.execute(tables["detector_warmups"].insert(), {"id": identities["attempt"],
            "detector_id": identities["detector"], "cutoff": anchor, "plan": {"legacy": True},
            "inputs": inputs, "inputs_hash": fingerprint(inputs), "status": "ready", "created_at": anchor})
        connection.execute(tables["detectors"].update().where(tables["detectors"].c.id == identities["detector"])
            .values(active_attempt_id=identities["attempt"], revision=6))
        connection.execute(tables["detector_outcomes"].insert(), {"id": identities["outcome"],
            "detector_id": identities["detector"], "working_point_id": identities["present"],
            "window_index": 1, "revision": 4, "kind": "evaluated", "data": {"legacy": True,
                "exact_integer": 2**60 + 1, "inputs_hash": fingerprint(inputs)}})
        connection.execute(tables["findings"].insert(), {"id": identities["finding"],
            "detector_id": identities["detector"], "working_point_id": identities["present"],
            "created_at": anchor, "late": False, "data": {"actual": [2**60 + 1], "expected": [1],
                "origin": "working", "inputs_hash": fingerprint(inputs)}})
    migrate("upgrade", "0021_event_time")
    auxiliary = MetaData()
    auxiliary.reflect(engine, schema="logs", only=["aggregate_memberships", "aggregate_snapshots"])
    member = auxiliary.tables["logs.aggregate_memberships"]
    snapshot = auxiliary.tables["logs.aggregate_snapshots"]
    with engine.begin() as connection:
        for name in ("cleared", "present"):
            connection.execute(member.insert(), {"closure_id": identities[name], "event_id": uuid4(),
                "event_time": anchor, "fingerprint": fingerprint({"event": name, "integer": 2**60 + 1})})
            connection.execute(snapshot.insert(), {"closure_id": identities[name], "metadata_json": {
                "legacy": True, "exact_integer": 2**60 + 1, "inputs_hash": fingerprint(inputs)}})
        # Exercise the real old-version manual authority before provenance
        # existed. No trigger/constraint bypass and no invented clear timestamp.
        connection.execute(text("UPDATE logs.aggregate_schedules SET active_cleanup_id=:operation WHERE id=:id"),
            {"operation": uuid4(), "id": schedule})
        connection.execute(snapshot.delete().where(snapshot.c.closure_id == identities["cleared"]))
        connection.execute(tables["working_window_points"].delete().where(
            tables["working_window_points"].c.id == identities["cleared"]))
        connection.execute(text("UPDATE logs.aggregate_closures SET payload_state='cleared' WHERE origin_point_id=:id"),
            {"id": identities["cleared"]})
        connection.execute(member.update().where(member.c.closure_id == identities["cleared"])
            .values(event_time=None, fingerprint=None))
        connection.execute(text("UPDATE logs.aggregate_schedules SET active_cleanup_id=NULL WHERE id=:id"), {"id": schedule})
    migrate("upgrade", PREVIOUS)
    return identities


def test_0023_facts_survive_budget_upgrade_and_refused_nonempty_downgrade(legacy_database):
    engine, _, migrate = legacy_database
    identities = seed_opaque_legacy_facts(engine, migrate)
    before = stored_facts(engine)
    assert len(before["aggregate_closures"]) == len(before["aggregate_memberships"]) == 2
    assert {row["payload_state"] for row in before["aggregate_closures"]} == {"present", "cleared"}
    assert before["working_window_points"][0]["features"][0]["value"] == 2**60 + 1
    assert before["working_window_points"][0]["completeness"] == "unknown"
    assert before["detectors"][0]["checkpoint"]["body"]["exact_integer"] == 2**60 + 1
    assert before["detectors"][0]["active_attempt_id"] == str(identities["attempt"])
    assert before["detector_warmups"][0]["inputs_hash"] == fingerprint(before["detector_warmups"][0]["inputs"])
    assert before["aggregate_snapshots"][0]["metadata_json"]["exact_integer"] == 2**60 + 1
    migrate("upgrade", CURRENT)
    assert stored_facts(engine, remove_new=True) == before
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == CURRENT
        assert connection.scalar(text("SELECT count(*) FROM logs.aggregate_window_attempts")) == 0
        assert connection.scalar(text("SELECT last_execution FROM logs.aggregate_schedules")) is None
        assert connection.execute(text("SELECT calculation_status,calculation_reason FROM logs.working_window_points")).one() == ("computed", None)
        assert connection.execute(text("SELECT DISTINCT calculation_status,calculation_reason,execution FROM logs.aggregate_closures")).one() == ("computed", None, None)
    upgraded = stored_facts(engine)
    refused = migrate("downgrade", PREVIOUS, check=False)
    assert refused.returncode != 0 and "nonempty aggregate execution requires explicit data-preserving migration" in refused.stderr
    assert stored_facts(engine) == upgraded
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == CURRENT
        assert connection.scalar(text("SELECT count(*) FROM logs.aggregate_window_attempts")) == 0
    migrate("upgrade", "head")
    migrate("check")


def test_empty_0023_0024_schema_roundtrip_restores_columns_and_guards(legacy_database):
    engine, _, migrate = legacy_database
    migrate("upgrade", PREVIOUS)
    before = column_contract(engine)
    with engine.connect() as connection:
        previous_guard = connection.scalar(text("SELECT prosrc FROM pg_proc WHERE oid='logs.guard_aggregate_closure()'::regprocedure"))
    migrate("upgrade", CURRENT)
    upgraded = column_contract(engine)
    assert upgraded["aggregate_closures"]["calculation_status"]["nullable"] is False
    assert upgraded["working_window_points"]["calculation_status"]["nullable"] is False
    assert upgraded["aggregate_closures"]["snapshot_reference_at"]["nullable"] is True
    for column in ("event_count", "completeness", "completeness_reason", "evidence_refs"):
        assert upgraded["working_window_points"][column]["nullable"] is True
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT to_regclass('logs.aggregate_window_attempts')")) is not None
        assert connection.scalar(text("SELECT count(*) FROM pg_trigger WHERE tgrelid='logs.aggregate_window_attempts'::regclass AND tgname='guard_window_attempt' AND NOT tgisinternal")) == 1
    migrate("downgrade", PREVIOUS)
    assert column_contract(engine) == before
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == PREVIOUS
        assert connection.scalar(text("SELECT to_regclass('logs.aggregate_window_attempts')")) is None
        assert connection.scalar(text("SELECT to_regprocedure('logs.guard_window_attempt()')")) is None
        restored_guard = connection.scalar(text("SELECT prosrc FROM pg_proc WHERE oid='logs.guard_aggregate_closure()'::regprocedure"))
        assert "uncomputed" not in restored_guard and "automatic" in restored_guard
        # The old authorization clauses remain; whitespace in CREATE OR
        # REPLACE is irrelevant to the restored behavior.
        assert "authorization_txid" in previous_guard
        assert " ".join(restored_guard.split()) == " ".join(previous_guard.split())
    migrate("upgrade", CURRENT)
    migrate("upgrade", "head")
    migrate("check")
    assert column_contract(engine) == upgraded
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM logs.aggregate_window_attempts")) == 0
