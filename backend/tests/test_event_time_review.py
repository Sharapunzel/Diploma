"""Permanent DB regressions for snapshot evidence, date range and preflight."""

import os
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from app.aggregate_composition import build_aggregate_service
from app.config import Settings
from app.core.errors import DomainError
from app.core.security import hash_password
from app.detection.contracts import DetectionError, feature_vector
from app.detector_composition import build_detector_service
from app.main import create_app
from app.models import (
    Aggregate,
    AggregateClosure,
    AggregateSchedule,
    KafkaOperationalEvent,
    User,
    WorkingWindowPoint,
)
from app.repositories.sqlalchemy.detectors import SqlAlchemyDetectorRepository
from fastapi.testclient import TestClient
from sqlalchemy import Text, delete, func, select
from test_event_time_pipeline import context, create, migrated, turns, write_event

__all__ = ["context", "migrated"]


@pytest.mark.parametrize("index", [1, 2])
def test_archive_gap_survives_empty_and_nonempty_out_of_order_snapshot(context, index):
    db, catalog, source, connection = context
    anchor = (datetime.now(UTC) - timedelta(days=3)).replace(microsecond=0)
    for window, offset in [(0, 10), (2, 0), (3, 5)]:
        write_event(context, (anchor + timedelta(seconds=300 * window)).isoformat(), offset=offset)
    gap = uuid4()
    try:
        with db.session_factory.begin() as session:
            session.add(KafkaOperationalEvent(id=gap, kind="retention_gap", reason_code="retention_gap",
                source_id=source, source_identity=source, source_name="test", connection_id=connection,
                connection_identity=connection, connection_name="test", cluster_identity="event-time-test",
                topic_name=str(source), old_topic_identity="event-time-generation", kafka_partition=0,
                offset_start=1, offset_end=5, detected_at=datetime.now(UTC)))
        card, request = create(context, anchor)
        preview = request.model_copy(update={"window_start_utc": anchor + timedelta(seconds=300 * index)})
        with db.session_factory() as session:
            result = build_aggregate_service(session, catalog).preview(preview)
            assert result.completeness == "unknown"
            assert str(gap) in result.snapshot_metadata["facts"]["gaps"]
        turns(context, card.id, 24)
        with db.session_factory() as session:
            api = build_aggregate_service(session, catalog)
            point = api.point_at(card.id, index)
            assert point.completeness == "unknown"
            assert str(gap) in point.snapshot_metadata["facts"]["gaps"]
            assert point.event_count == (0 if index == 1 else 1)
            with pytest.raises(ValueError):
                feature_vector(point.features, ["n", "bytes"], point.completeness)
        # Another generation at a disjoint event-time span is not affected.
        write_event(context, (anchor + timedelta(seconds=1500)).isoformat(), offset=20,
            generation="other-generation", partition=1)
        with db.session_factory() as session:
            clean = build_aggregate_service(session, catalog).preview(
                request.model_copy(update={"window_start_utc": anchor + timedelta(seconds=1500)}))
            assert clean.completeness == "complete"
            assert clean.snapshot_metadata["facts"]["gaps"] == []
    finally:
        with db.session_factory.begin() as session:
            session.execute(delete(KafkaOperationalEvent).where(KafkaOperationalEvent.id == gap))


def test_http_rejects_unrepresentable_online_deadline_before_create(context):
    db, catalog, source, _ = context
    ordinary, _ = create(context, datetime.now(UTC) - timedelta(days=1))
    username = uuid4().hex
    with db.session_factory.begin() as session:
        session.add(User(username=username, display_name=username, password_hash=hash_password("test-password"),
            role_id=UUID("00000000-0000-4000-8000-000000000001")))
    app = create_app(Settings(environment="test", database_url=os.environ["TEST_DATABASE_URL"]),
        database=db, ecs_catalog=catalog, start_consumers=False)
    with TestClient(app) as client:
        assert client.post("/api/v1/auth/local/login", json={"username": username, "password": "test-password"}).status_code == 200
        headers = {"X-CSRF-Token": client.get("/api/v1/auth/session").json()["csrf_token"], "Idempotency-Key": uuid4().hex}
        body = {"source_id": str(source), "aggregate_rule": {"features": [{"key": "n", "name": "Count", "op": "count"}]},
            "schedule": {"boundary": "9999-12-31T23:50:00Z", "timezone": "UTC", "duration_seconds": 300},
            "window_start_utc": "2026-01-01T00:00:00Z", "time_basis": "event_time"}
        checked = client.post("/api/v1/aggregates/validate", headers=headers,
            json={k: body[k] for k in ("source_id", "aggregate_rule", "schedule")})
        assert checked.status_code == 200, checked.text
        assert client.post("/api/v1/aggregates/preview", headers=headers, json=body).status_code == 200
        with db.session_factory() as session:
            before = session.scalar(select(func.count()).select_from(AggregateSchedule))
        body.update(name="Range test", rule_hash=checked.json()["rule_hash"])
        for _ in range(2):
            result = client.post("/api/v1/aggregates", json=body, headers=headers)
            assert result.status_code == 422, result.text
            assert result.json()["code"] == "schedule_datetime_range"
        with db.session_factory() as session:
            assert session.scalar(select(func.count()).select_from(AggregateSchedule)) == before
            assert session.scalar(select(func.count()).select_from(Aggregate).where(Aggregate.name == "Range test")) == 0
        assert client.get("/api/v1/aggregates").status_code == 200
        assert client.get(f"/api/v1/aggregates/{ordinary.id}").status_code == 200
        body["schedule"]["boundary"] = "9999-12-31T23:45:00Z"
        accepted = client.post("/api/v1/aggregates", json=body, headers=headers)
        assert accepted.status_code == 201, accepted.text
        assert client.post("/api/v1/aggregates", json=body, headers=headers).json() == accepted.json()


@pytest.mark.parametrize("large", [False, True])
def test_full_evidence_preflight_before_fetch_and_cleanup(context, monkeypatch, large):
    db, catalog, source, _ = context
    anchor = datetime.now(UTC) - timedelta(days=1)
    card, _ = create(context, anchor, 10)
    starts = [anchor + timedelta(seconds=10 * i) for i in range(4)]
    # A bounded fixture represents individually valid metadata; no JVM or
    # hundreds of thousands of source events are needed to test SQL preflight.
    evidence = {"coordinates": [{"connection": str(uuid4()), "topic": "x" * (18 * 1024**2 if large else 10),
        "partition": 0, "first_offset": 0, "last_offset": 1}]}
    with db.session_factory.begin() as session:
        for index, start in enumerate(starts):
            end, identity = start + timedelta(seconds=10), uuid4()
            session.add(AggregateClosure(origin_point_id=identity, aggregate_id=card.id, schedule_id=card.schedule_id,
                source_identity=source, window_index=index, start_utc=start, end_utc=end, time_basis="event_time",
                origin="initial_history", snapshot_reference_at=end, closed_at=end, close_not_before_at=end,
                payload_state="present", point_kind="working"))
            session.flush()
            session.add(WorkingWindowPoint(id=identity, schedule_id=card.schedule_id, window_index=index,
                start_utc=start, end_utc=end, scheduled_at=end, actual_started_at=end, actual_completed_at=end,
                event_count=1, completeness="complete", completeness_reason="received_event_snapshot",
                evidence_refs=evidence, features=[{"key": "n", "value": 1, "reason": None,
                    "valid_count": 1, "invalid_count": 0}], time_basis="event_time"))
    with db.session_factory() as session:
        repo = SqlAlchemyDetectorRepository(session)
        if large:
            feature_size, evidence_size = session.execute(select(
                func.sum(func.octet_length(WorkingWindowPoint.features.cast(Text))),
                func.sum(func.octet_length(WorkingWindowPoint.evidence_refs.cast(Text)))).where(
                    WorkingWindowPoint.schedule_id == card.schedule_id)).one()
            assert feature_size < 64 * 1024**2 < evidence_size
            def forbidden(*args, **kwargs):
                pytest.fail("Full payload fetch reached before preflight refusal")
            monkeypatch.setattr(repo, "_points", forbidden)
            with pytest.raises(DetectionError, match="resource_budget:warmup_input_bytes"):
                repo.working_inputs(card.schedule_id, starts)
        else:
            assert len(repo.working_inputs(card.schedule_id, starts)) == 4
    with db.session_factory() as session:
        detector = build_detector_service(session, catalog).create(card.id, UUID(int=1), "Protected",
            config={"sample_size": 2, "output_after": 2, "trees": 20})
        build_detector_service(session, catalog).stop(detector)
        api = build_aggregate_service(session, catalog)
        if large:
            for action in (lambda: api.cleanup_preview(card.id, 0),
                lambda: api.aggregate_command(card.id, UUID(int=1), uuid4().hex, "cleanup", 0)):
                with pytest.raises(DomainError) as error:
                    action()
                assert error.value.code == "cleanup_protection_unavailable"
            assert session.scalar(select(func.count()).select_from(WorkingWindowPoint).where(
                WorkingWindowPoint.schedule_id == card.schedule_id)) == 4
            assert session.get(AggregateSchedule, card.schedule_id).active_cleanup_id is None
        else:
            assert api.cleanup_preview(card.id, 0).minimum_keep == 4
