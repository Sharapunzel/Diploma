"""Migration initializes policy state without deleting or inventing old facts."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from test_event_time_migration import legacy_database

from app.aggregate_composition import build_aggregate_service
from app.core.errors import DomainError
from app.db import Database
from app.ecs import PackagedEcsCatalog

__all__ = ["legacy_database"]


def test_existing_cleared_metadata_stays_unknown_and_present_payload_survives(legacy_database):
    engine, url, migrate = legacy_database
    metadata = MetaData()
    metadata.reflect(engine, schema="logs", only=["aggregate_schedules", "aggregates", "working_window_points"])
    schedule = metadata.tables["logs.aggregate_schedules"]
    aggregate = metadata.tables["logs.aggregates"]
    point = metadata.tables["logs.working_window_points"]
    actor, source, schedule_id, aggregate_id, first, second = [uuid4() for _ in range(6)]
    anchor = datetime.now(UTC) - timedelta(days=2)
    rule = {"features": [{"key": "n", "name": "Count", "op": "count"}]}
    with engine.begin() as connection:
        connection.execute(schedule.insert(), {"id": schedule_id, "source_identity": source,
            "anchor_utc": anchor, "duration_seconds": 300, "next_index": 2})
        connection.execute(aggregate.insert(), {"id": aggregate_id, "schedule_id": schedule_id,
            "source_identity": source, "source_name": "Legacy", "source_type": "kafka", "name": "Before retention",
            "rule": rule, "rule_hash": "f" * 64, "original_boundary": anchor.isoformat(), "timezone": "UTC",
            "created_at": anchor, "created_by": actor, "state": "archived", "request_key": uuid4().hex,
            "request_hash": "f" * 64})
        for index, identity in enumerate((first, second)):
            start, end = anchor + timedelta(seconds=300 * index), anchor + timedelta(seconds=300 * (index + 1))
            connection.execute(point.insert(), {"id": identity, "schedule_id": schedule_id,
                "window_index": index, "start_utc": start, "end_utc": end, "scheduled_at": end,
                "actual_started_at": end, "actual_completed_at": end, "event_count": 1,
                "features": [{"key": "n", "value": 2**60 + 1, "reason": None, "valid_count": 1, "invalid_count": 0}],
                "completeness": "unknown", "completeness_reason": "legacy", "evidence_refs": {}})
    migrate("upgrade", "0021_event_time")
    with engine.begin() as connection:
        # This is the actual old-version cleanup authority, before clearing
        # provenance existed. The new migration must keep that absence honest.
        connection.execute(text("UPDATE logs.aggregate_schedules SET active_cleanup_id=:operation WHERE id=:id"),
            {"operation": uuid4(), "id": schedule_id})
        connection.execute(text("DELETE FROM logs.working_window_points WHERE id=:id"), {"id": first})
        connection.execute(text("UPDATE logs.aggregate_closures SET payload_state='cleared' WHERE origin_point_id=:id"), {"id": first})
        connection.execute(text("UPDATE logs.aggregate_schedules SET active_cleanup_id=NULL WHERE id=:id"), {"id": schedule_id})
    migrate("upgrade", "head")
    migrate("check")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM pg_statistic_ext WHERE stxnamespace='logs'::regnamespace AND stxname='st_outcome_retention_compacted'")) == 1
        row = connection.execute(text("SELECT payload_state,cleared_at,clear_reason,clear_policy_version FROM logs.aggregate_closures WHERE origin_point_id=:id"),
            {"id": first}).one()
        assert row == ("cleared", None, None, None)
        assert connection.scalar(text("SELECT count(*) FROM logs.working_window_points WHERE id=:id"), {"id": second}) == 1
        state = connection.execute(text("SELECT policy_version,status,reserve_windows,last_checked_at,auto_cleared_total,history_revision,last_cleared_at FROM logs.aggregate_retention_state WHERE aggregate_id=:id"),
            {"id": aggregate_id}).one()
        assert state == ("model_history_v1", "scheduled", None, None, 0, 0, None)
    database = Database(url)
    try:
        with database.session_factory() as session:
            api = build_aggregate_service(session, PackagedEcsCatalog.load())
            card = api.card(aggregate_id)
            assert card.execution_mode == "retired" and card.auto_cleanup.history_revision == 0
            assert api.point_by_id(aggregate_id, second).features[0].value == 2**60 + 1
            with pytest.raises(DomainError) as error:
                api.point_by_id(aggregate_id, first)
            assert error.value.status_code == 410
            assert error.value.details["clear_reason"] is error.value.details["cleared_at"] is None
            assert api.maintain(aggregate_id)["status"] == "inactive"
            assert api.card(aggregate_id).auto_cleanup.history_revision == 0
    finally:
        database.dispose()
