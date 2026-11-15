"""Real PostgreSQL event snapshots, finality, late scans and physical cleanup."""

import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from app.aggregate_composition import build_aggregate_service
from app.core.errors import DomainError
from app.db import Database
from app.ecs import PackagedEcsCatalog
from app.models import (
    Aggregate,
    AggregateClosure,
    AggregateLateEvent,
    AggregateMembership,
    AggregateSnapshot,
    AvailabilityMarker,
    KafkaConnection,
    ParsedLog,
    ProcessedKafkaRecord,
    Source,
    SourceInterruption,
)
from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
from app.schemas.aggregates import AggregatePreviewRequest, CreateAggregateRequest
from event_time_fixtures import delete_aggregate
from sqlalchemy import delete, func, select


@pytest.fixture(scope="module", autouse=True)
def migrated():
    env = {**os.environ, "DATABASE_URL": os.environ["TEST_DATABASE_URL"]}
    env.pop("DATABASE_URL_FILE", None)
    subprocess.run([sys.executable, "-B", "-m", "alembic", "upgrade", "head"], env=env, check=True)


@pytest.fixture
def context():
    database = Database(os.environ["TEST_DATABASE_URL"])
    assert database.engine.url.database != "diploma_db"
    source_id, connection_id = uuid4(), uuid4()
    with database.session_factory.begin() as session:
        session.add(KafkaConnection(id=connection_id, name=str(connection_id), bootstrap_servers=["kafka:9092"], cluster_identity="event-time-test"))
        session.flush()
        session.add(Source(id=source_id, connection_id=connection_id, name=str(source_id), topic_name=str(source_id), is_enabled=True))
    catalog = PackagedEcsCatalog.load()
    try:
        yield database, catalog, source_id, connection_id
    finally:
        with database.session_factory.begin() as session:
            aggregates = list(session.scalars(select(Aggregate).where(Aggregate.source_identity == source_id)))
            for aggregate in aggregates:
                delete_aggregate(session, aggregate.id)
            session.execute(delete(AvailabilityMarker).where(AvailabilityMarker.source_identity == source_id))
            session.execute(delete(ParsedLog).where(ParsedLog.source_id == source_id))
            session.execute(delete(ProcessedKafkaRecord).where(ProcessedKafkaRecord.source_id == source_id))
            session.execute(delete(SourceInterruption).where(SourceInterruption.source_identity == source_id))
            session.execute(delete(Source).where(Source.id == source_id))
            session.execute(delete(KafkaConnection).where(KafkaConnection.id == connection_id))
        database.dispose()


def write_event(context, timestamp, *, offset=0, diagnostics=None, value=2**60 + 1, event_id=None,
                generation="event-time-generation", partition=0):
    database, _, source_id, connection_id = context
    now = datetime.now(UTC)
    event_id = event_id or uuid4()
    with database.session_factory.begin() as session:
        record = ProcessedKafkaRecord(id=uuid4(), connection_id=connection_id,
            connection_identity=connection_id, kafka_topic_identity=generation,
            source_id=source_id, normalizer_id=None, kafka_topic=str(source_id), kafka_partition=partition,
            kafka_offset=offset, result_status="complete", backend_received_at=now, backend_processed_at=now)
        event = ParsedLog(id=event_id, source_id=source_id, connection_id=connection_id, normalizer_id=None,
            normalizer_version=1, normalizer_name="test", source_name="test", connection_name="test",
            kafka_topic=str(source_id), kafka_partition=partition, kafka_offset=offset,
            deduplication_key=str(event_id), fluent_bit_collected_at=now, backend_received_at=now,
            backend_processed_at=now, raw="synthetic", ecs_data={"@timestamp": timestamp, "network": {"bytes": value}},
            normalization_status="complete", normalization_diagnostics=diagnostics or [])
        assert SqlAlchemyProcessingRepository(session).store_event(record, event)
    return event_id


def create(context, anchor, duration=300):
    database, catalog, source_id, _ = context
    body = AggregatePreviewRequest(source_id=source_id,
        aggregate_rule={"features": [{"key": "n", "name": "Count", "op": "count"},
            {"key": "bytes", "name": "Bytes", "op": "sum", "field": "network.bytes"}]},
        schedule={"boundary": anchor, "timezone": "UTC", "duration_seconds": duration},
        window_start_utc=anchor, time_basis="event_time")
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        checked = api.validate(body)
        request = CreateAggregateRequest(**body.model_dump(), name="Event time", rule_hash=checked.rule_hash)
        return api.create(request, UUID(int=1), str(uuid4())), request


def turns(context, aggregate_id, count=15):
    database, catalog, _, _ = context
    for _ in range(count):
        with database.session_factory() as session:
            build_aggregate_service(session, catalog).advance(aggregate_id)


def archive(context, anchor):
    events = []
    for index in range(16):
        for second in range(1 + index % 3):
            at = anchor + timedelta(seconds=index * 10 + second)
            write_event(context, at.isoformat(), offset=len(events))
            events.append(at)
    card, _ = create(context, anchor, 10)
    turns(context, card.id, 60)
    return card, events


def test_old_archive_last_window_exact_membership_and_late_smaller_uuid(context):
    database, catalog, _, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=3)).replace(microsecond=0)
    first = write_event(context, (anchor + timedelta(seconds=1)).isoformat(), event_id=UUID(int=100))
    write_event(context, (anchor + timedelta(seconds=601)).isoformat(), offset=1)
    card, _ = create(context, anchor)
    turns(context, card.id)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        history = [p for p in api.points(card.id, 50, 0)["items"] if p.origin == "initial_history"]
        assert [p.window_index for p in reversed(history)] == [0, 1, 2]
        assert history[-1].features[1].value == 2**60 + 1
        assert history[1].event_count == 0 and history[1].features[1].value is None
        before = history[-1].model_dump()
        closure_id = history[-1].id
        assert session.scalar(select(func.count()).select_from(AggregateMembership).where(AggregateMembership.closure_id == closure_id)) == 1
    late = write_event(context, (anchor + timedelta(seconds=2)).isoformat(), offset=2, event_id=UUID(int=1))
    turns(context, card.id, 12)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        assert api.point_at(card.id, 0).model_dump() == before
        ledger = list(session.scalars(select(AggregateLateEvent).where(AggregateLateEvent.closure_id == closure_id)))
        assert [r.event_id for r in ledger] == [late]
        assert first != late


@pytest.mark.parametrize("zone", ["UTC", "Asia/Yekaterinburg", "America/New_York"])
def test_canonical_snapshot_and_membership_ignore_database_timezone(context, zone):
    database, catalog, _, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=2)).replace(microsecond=0)
    event = write_event(context, (anchor + timedelta(seconds=1)).isoformat())
    with database.session_factory() as session:
        session.execute(select(func.set_config("TimeZone", zone, False)))
        session.commit()
    card, _ = create(context, anchor)
    turns(context, card.id, 6)
    with database.session_factory() as session:
        point = build_aggregate_service(session, catalog).point_at(card.id, 0)
        assert point.start_utc == anchor and point.end_utc == anchor + timedelta(seconds=300)
        assert point.event_count == 1 and point.features[1].value == 2**60 + 1
        assert session.scalar(select(AggregateMembership.event_id).where(AggregateMembership.closure_id == point.id)) == event


def test_pause_idempotent_replay_cleanup_and_no_recreation(context):
    database, catalog, _, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=2)).replace(microsecond=0)
    write_event(context, (anchor + timedelta(seconds=1)).isoformat())
    card, _ = create(context, anchor)
    turns(context, card.id)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        paused = api.aggregate_command(card.id, UUID(int=1), "pause", "pause")
        cursor = api.card(card.id).next_index
        api.aggregate_command(card.id, UUID(int=1), "unpause", "unpause")
        assert api.aggregate_command(card.id, UUID(int=1), "pause", "pause") == paused
        assert not api.card(card.id).paused and api.card(card.id).next_index == cursor
        plan = api.cleanup_preview(card.id, 0)
        assert plan.minimum_keep == 0 and plan.can_cleanup
        receipt = api.aggregate_command(card.id, UUID(int=1), "cleanup", "cleanup", 0)
        assert receipt.cleanup_operation_id
        assert api.aggregate_command(card.id, UUID(int=1), "cleanup", "cleanup", 0) == receipt
    turns(context, card.id, 12)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        assert api.cleanup_operation(card.id, receipt.cleanup_operation_id).status == "succeeded"
        with pytest.raises(DomainError) as error:
            api.point_at(card.id, 0)
        assert error.value.code == "aggregate_point_cleared"
        assert session.scalar(select(func.count()).select_from(AggregateMembership)) >= 1
        assert session.scalar(select(func.count()).select_from(AggregateSnapshot).where(AggregateSnapshot.closure_id.in_(
            select(AggregateClosure.origin_point_id).where(AggregateClosure.aggregate_id == card.id)))) == 0
    turns(context, card.id, 12)
    with database.session_factory() as session:
        assert not session.scalars(select(AggregateLateEvent).join(AggregateClosure,
            AggregateClosure.origin_point_id == AggregateLateEvent.closure_id).where(AggregateClosure.aggregate_id == card.id)).all()


def test_pause_fences_snapshot_computed_before_command(context, monkeypatch):
    from app.services.implementations.aggregate_snapshot import AggregateSnapshotService

    database, catalog, _, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=2)).replace(microsecond=0)
    write_event(context, (anchor + timedelta(seconds=1)).isoformat())
    card, _ = create(context, anchor)
    original = AggregateSnapshotService.build

    def paused_after_snapshot(*args, **kwargs):
        candidate = original(*args, **kwargs)
        with database.session_factory() as session:
            build_aggregate_service(session, catalog).aggregate_command(card.id, UUID(int=1), "race-pause", "pause")
        return candidate

    with monkeypatch.context() as patch:
        patch.setattr(AggregateSnapshotService, "build", paused_after_snapshot)
        turns(context, card.id, 2)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        assert api.card(card.id).paused
        assert api.repository.closure_at(card.schedule_id, 0) is None
        assert api.points(card.id, 50, 0)["total"] == 0
        api.aggregate_command(card.id, UUID(int=1), "race-unpause", "unpause")
    turns(context, card.id, 6)
    with database.session_factory() as session:
        assert build_aggregate_service(session, catalog).point_at(card.id, 0).event_count == 1


def test_unknown_closure_commit_keeps_one_payload_manifest_and_position(context):
    database, catalog, _, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=2)).replace(microsecond=0)
    write_event(context, (anchor + timedelta(seconds=1)).isoformat())
    card, _ = create(context, anchor)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        commit, add = api.uow.commit, api.repository.add_event_point
        candidate = []

        def added(*args):
            add(*args)
            candidate.append(args[0].id)

        def unknown_commit():
            commit()
            if candidate:
                raise RuntimeError("synthetic acknowledgement loss")

        api.repository.add_event_point, api.uow.commit = added, unknown_commit
        api.advance(card.id)  # discovery precedes this history turn
        with pytest.raises(RuntimeError, match="synthetic acknowledgement loss"):
            api.advance(card.id)
    turns(context, card.id, 12)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        points = api.points(card.id, 50, 0)
        assert points["total"] == 1 and points["items"][0].id == candidate[0]
        assert api.card(card.id).next_index == card.next_index
        assert session.scalar(select(func.count()).select_from(AggregateMembership).where(
            AggregateMembership.closure_id == candidate[0])) == 1


def test_failed_cleanup_holds_fence_and_retry_reuses_boundary(context, monkeypatch):
    from app.repositories.sqlalchemy.aggregate_cleanup import SqlAlchemyAggregateCleanupQueries

    database, catalog, _, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=2)).replace(microsecond=0)
    write_event(context, (anchor + timedelta(seconds=1)).isoformat())
    card, _ = create(context, anchor)
    turns(context, card.id)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        accepted = api.aggregate_command(card.id, UUID(int=1), "failed-cleanup", "cleanup", 0)
        before = api.cleanup_operation(card.id, accepted.cleanup_operation_id)

    def failure(*args):
        raise RuntimeError("synthetic physical delete failure")

    with monkeypatch.context() as patch:
        patch.setattr(SqlAlchemyAggregateCleanupQueries, "compact_point", failure)
        with pytest.raises(RuntimeError, match="synthetic physical delete failure"):
            turns(context, card.id, 1)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        failed = api.cleanup_operation(card.id, accepted.cleanup_operation_id)
        assert failed.status == "failed"
        with pytest.raises(DomainError) as error:
            api.aggregate_command(card.id, UUID(int=1), "blocked-pause", "pause")
        assert error.value.code == "cleanup_in_progress"
        assert api.aggregate_command(card.id, UUID(int=1), "failed-cleanup", "cleanup", 0) == accepted
        retry = api.aggregate_command(card.id, UUID(int=1), "retry-cleanup", "cleanup_retry",
            operation_id=accepted.cleanup_operation_id)
        assert retry.cleanup_operation_id == accepted.cleanup_operation_id
        assert api.cleanup_operation(card.id, retry.cleanup_operation_id).reference_tail == before.reference_tail
    turns(context, card.id, 8)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        assert api.cleanup_operation(card.id, retry.cleanup_operation_id).status == "succeeded"
        assert api.card(card.id).active_cleanup_id is None


@pytest.mark.parametrize("action", ["pause", "cleanup"])
def test_unknown_command_commit_replays_confirmed_receipt_without_new_action(context, action):
    from psycopg import OperationalError
    from sqlalchemy.exc import DBAPIError

    database, catalog, _, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=2)).replace(microsecond=0)
    write_event(context, (anchor + timedelta(seconds=1)).isoformat())
    card, _ = create(context, anchor)
    turns(context, card.id)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        commit = api.uow.commit

        def unknown_commit():
            commit()
            raise DBAPIError(None, None, OperationalError("synthetic acknowledgement loss"))

        api.uow.commit = unknown_commit
        with pytest.raises(DomainError) as error:
            api.aggregate_command(card.id, UUID(int=1), "unknown", action, 0 if action == "cleanup" else None)
        assert error.value.code == "aggregate_command_unconfirmed" and error.value.status_code == 503
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        receipt = api.aggregate_command(card.id, UUID(int=1), "unknown", action, 0 if action == "cleanup" else None)
        if action == "pause":
            api.aggregate_command(card.id, UUID(int=1), "opposite", "unpause")
        else:
            turns(context, card.id, 8)
            assert api.cleanup_operation(card.id, receipt.cleanup_operation_id).status == "succeeded"
        assert api.aggregate_command(card.id, UUID(int=1), "unknown", action, 0 if action == "cleanup" else None) == receipt
        assert not api.card(card.id).paused and api.card(card.id).active_cleanup_id is None


def test_paused_prepared_start_survives_takeover_without_reauthorizing_stopped_model(context):
    from app.adapters.detector_lock import DetectorSessionLock
    from app.detector_composition import build_detector_service

    database, catalog, _, _ = context
    card, _ = archive(context, (datetime.now(UTC) - timedelta(days=1)).replace(microsecond=0))
    lock = DetectorSessionLock(os.environ["TEST_DATABASE_URL"])
    try:
        pid = lock.acquire()
        assert pid is not None
        with database.session_factory() as session:
            owner = build_detector_service(session, catalog).acquire_owner(uuid4(), pid)
            service = build_detector_service(session, catalog, owner)
            identity = service.create(card.id, UUID(int=1), "Pending warmup",
                config={"sample_size": 8, "output_after": 8, "trees": 20})
            stopped = service.create(card.id, UUID(int=1), "Stopped neighbour")
            service.stop(stopped)
            service.start(identity)
            service.begin_start(identity)
            assert service.prepare_warmup(identity)["state"] == "prepared"
            before = service.state(identity)["detector"]
            aggregates = build_aggregate_service(session, catalog)
            aggregates.aggregate_command(card.id, UUID(int=1), "takeover-pause", "pause")
            successor = service.acquire_owner(uuid4(), pid)
            resumed = build_detector_service(session, catalog, successor)
            resumed.recover(identity)
            after = resumed.state(identity)["detector"]
            assert after == before and after["execution_state"] == "starting"
            future = datetime.now(UTC) + timedelta(seconds=60)
            aggregates.repository.now = lambda: future
            aggregates.aggregate_command(card.id, UUID(int=1), "takeover-unpause", "unpause")
            assert any(r["reason"] == "aggregate_resume" for r in resumed.state(identity)["recovery"])
            assert resumed.state(stopped)["detector"]["execution_state"] == "stopped"
            assert resumed.prepare_warmup(identity)["attempt_id"] == before["active_attempt_id"]
    finally:
        lock.close()


def test_http_commands_permissions_strict_keep_and_same_key_replay(context):
    from app.config import Settings
    from app.core.security import hash_password
    from app.main import create_app
    from app.models import User
    from fastapi.testclient import TestClient

    database, catalog, _, _ = context
    card, _ = create(context, datetime.now(UTC) - timedelta(days=1))
    names = [uuid4().hex, uuid4().hex]
    with database.session_factory.begin() as session:
        for username, role in zip(names, (1, 2)):
            session.add(User(username=username, display_name=username, password_hash=hash_password("synthetic-password"),
                role_id=UUID(f"00000000-0000-4000-8000-{role:012d}")))
    app = create_app(Settings(environment="test", database_url=os.environ["TEST_DATABASE_URL"]),
        database=database, ecs_catalog=catalog, start_consumers=False)
    base = f"/api/v1/aggregates/{card.id}"
    with TestClient(app) as client:
        assert client.get(base + "/points/cleanup-preview?keep_last=0").status_code == 401
        for username in names:
            assert client.post("/api/v1/auth/local/login", json={"username": username, "password": "synthetic-password"}).status_code == 200
            csrf = {"X-CSRF-Token": client.get("/api/v1/auth/session").json()["csrf_token"], "Idempotency-Key": uuid4().hex}
            if username == names[1]:
                for route in ("/points/cleanup-preview?keep_last=0", f"/points/{uuid4()}", f"/cleanup-operations/{uuid4()}"):
                    assert client.get(base + route).status_code == 403
                for route, body in (("/pause", None), ("/unpause", None), ("/points/cleanup", {"keep_last": 0}),
                                    (f"/cleanup-operations/{uuid4()}/retry", None)):
                    assert client.post(base + route, json=body, headers=csrf).status_code == 403
            else:
                assert client.post(base + "/pause", headers={"Idempotency-Key": "no-csrf"}).status_code == 403
                for invalid in ("-1", "1.0", "true", "9223372036854775808", "1e2"):
                    assert client.get(base + "/points/cleanup-preview", params={"keep_last": invalid}).status_code == 422
                for invalid in (True, False, -1, 1.0, "1", None, 9223372036854775808):
                    assert client.post(base + "/points/cleanup", json={"keep_last": invalid}, headers=csrf).status_code == 422
                assert client.get(base + "/points/cleanup-preview?keep_last=000").json()["minimum_keep"] == 0
                paused = client.post(base + "/pause", headers=csrf)
                assert paused.status_code == 200
                assert client.post(base + "/unpause", headers=csrf).status_code == 200
                assert client.post(base + "/pause", headers=csrf).json() == paused.json()
                assert client.get(base).json()["paused"] is False
                accepted = client.post(base + "/points/cleanup", json={"keep_last": 9223372036854775807}, headers=csrf)
                assert accepted.status_code == 202 and accepted.json()["cleanup_operation_id"]
                assert client.post(base + "/points/cleanup", json={"keep_last": 9223372036854775807}, headers=csrf).json() == accepted.json()
                operation_id = accepted.json()["cleanup_operation_id"]
                assert client.get(base + f"/cleanup-operations/{operation_id}").json()["planned_delete"] == 0
                assert client.post(base + "/points/cleanup", json={"keep_last": 0}, headers=csrf).status_code == 409
            assert client.post("/api/v1/auth/logout", headers=csrf).status_code == 200


@pytest.mark.parametrize("accelerated", [False, True])
def test_both_warmups_use_canonical_history_and_preserve_prepared_inputs(context, accelerated):
    from app.adapters.detector_lock import DetectorSessionLock
    from app.detector_composition import build_detector_service

    database, catalog, _, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=1)).replace(microsecond=0)
    card, events = archive(context, anchor)
    # This retained raw row must never appear in overlap or ordinary training.
    write_event(context, (anchor + timedelta(seconds=101)).isoformat(), offset=len(events), value=10**15)
    lock = DetectorSessionLock(os.environ["TEST_DATABASE_URL"])
    try:
        pid = lock.acquire()
        assert pid is not None
        with database.session_factory() as session:
            initial = build_detector_service(session, catalog)
            owner = initial.acquire_owner(uuid4(), pid)
            service = build_detector_service(session, catalog, owner)
            detector = service.create(card.id, UUID(int=1), "Canonical",
                config={"sample_size": 8, "output_after": 8, "trees": 20},
                accelerated=accelerated, step=5 if accelerated else None)
            service.stop(detector)
            pending = build_aggregate_service(session, catalog).cleanup_preview(card.id, 0)
            assert pending.minimum_keep > 0 and not pending.can_cleanup
            assert pending.blockers[0].reason == "planned_warmup"
            service.start(detector)
            service.begin_start(detector)
            result = service.prepare_warmup(detector)
            assert result["state"] == "prepared", result
            attempt = service.repository.attempt(result["attempt_id"])
            assert len(attempt["inputs"]) == 16
            assert all(item["basis"] == "event_time" for item in attempt["inputs"])
            assert attempt["plan"]["evaluation_start_index"] == card.online_start_index
            before = attempt["inputs_hash"]
            service.stop(detector)
            api = build_aggregate_service(session, catalog)
            assert api.cleanup_preview(card.id, 0).minimum_keep == 0
            api.aggregate_command(card.id, UUID(int=1), "prepared-input-pause", "pause")
            accepted = api.aggregate_command(card.id, UUID(int=1), "prepared-input-cleanup", "cleanup", 0)
            turns(context, card.id, 40)
            assert api.cleanup_operation(card.id, accepted.cleanup_operation_id).status == "succeeded"
            assert api.points(card.id, 50, 0)["total"] == 0
            api.aggregate_command(card.id, UUID(int=1), "prepared-input-unpause", "unpause")
            service.start(detector)
            service.begin_start(detector)
            assert service.prepare_warmup(detector)["attempt_id"] == attempt["id"]
            assert service.repository.attempt(attempt["id"])["inputs_hash"] == before
    finally:
        lock.close()


@pytest.mark.parametrize("mutation", ["delete_before", "ecs_before", "diagnostics_before", "delete_during", "add_during"])
def test_overlap_verifies_exact_membership_and_reads_one_snapshot(context, mutation):
    from app.adapters.detector_lock import DetectorSessionLock
    from app.detection.history import plan_windows
    from app.detector_composition import build_detector_service

    database, catalog, source_id, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=1)).replace(microsecond=0)
    card, events = archive(context, anchor)
    if mutation.endswith("before"):
        with database.session_factory.begin() as session:
            row = session.scalar(select(ParsedLog).where(ParsedLog.source_id == source_id)
                .order_by(ParsedLog.kafka_offset.desc()).limit(1))
            if mutation == "delete_before":
                session.delete(row)
            elif mutation == "ecs_before":
                row.ecs_data = {**row.ecs_data, "network": {"bytes": 2**60 + 2}}
            else:
                row.normalization_diagnostics = [{"code": "synthetic_changed_diagnostic"}]
    lock = DetectorSessionLock(os.environ["TEST_DATABASE_URL"])
    try:
        pid = lock.acquire()
        assert pid is not None
        with database.session_factory() as session:
            owner = build_detector_service(session, catalog).acquire_owner(uuid4(), pid)
            service = build_detector_service(session, catalog, owner)
            detector = service.create(card.id, UUID(int=1), "Exact overlap",
                config={"sample_size": 8, "output_after": 8, "trees": 20}, accelerated=True, step=5)
            original = service.history.aggregates.overlap_statistics
            changed = []

            def mutate_after_first_read(*args):
                result = original(*args)
                if not changed:
                    if mutation == "delete_during":
                        with database.session_factory.begin() as other:
                            other.execute(delete(ParsedLog).where(ParsedLog.source_id == source_id))
                    elif mutation == "add_during":
                        write_event(context, (anchor + timedelta(seconds=101)).isoformat(), offset=len(events))
                    changed.append(True)
                return result

            service.history.aggregates.overlap_statistics = mutate_after_first_read
            service.start(detector)
            service.begin_start(detector)
            result = service.prepare_warmup(detector)
            attempt = service.repository.attempt(result["attempt_id"])
            if mutation.endswith("before"):
                assert result["state"] == "waiting" and result["reason"] == "warmup_membership_source_unavailable"
                assert attempt["inputs"] == [] and service.state(detector)["detector"]["checkpoint"] is None
            else:
                assert result["state"] == "prepared", result
                counts = [sum(left <= at < right for at in events) for left, right in plan_windows(attempt["plan"])]
                assert [i["event_count"] for i in attempt["inputs"]] == counts
                assert [i["features"][0]["value"] for i in attempt["inputs"]] == counts
                assert [i["features"][1]["value"] for i in attempt["inputs"]] == [n*(2**60+1) for n in counts]
    finally:
        lock.close()
