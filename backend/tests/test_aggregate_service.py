"""Database/API regressions for immutable aggregates, snapshots and lifecycle."""

import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from threading import Event, Thread
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from event_time_fixtures import delete_aggregate
from fastapi import Depends
from fastapi.testclient import TestClient
from psycopg.errors import SerializationFailure
from sqlalchemy import delete, func, select
from sqlalchemy.exc import DBAPIError

from app.aggregate_composition import build_aggregate_service
from app.aggregation.worker_admission import FOREGROUND_BURST
from app.config import Settings
from app.core.errors import DomainError
from app.core.security import hash_password
from app.db import Database, get_session
from app.dependencies import get_aggregate_service
from app.ecs import PackagedEcsCatalog
from app.main import create_app
from app.models import (
    Aggregate,
    AggregateSchedule,
    AvailabilityMarker,
    HistoricalAggregatePoint,
    KafkaConnection,
    ParsedLog,
    ProcessedKafkaRecord,
    Source,
    User,
    WorkingWindowPoint,
)
from app.schemas.aggregates import (
    AggregatePreviewRequest,
    CreateAggregateRequest,
    HistoricalRequest,
)
from app.services.implementations.aggregate_worker import AggregateWorker


@pytest.fixture(scope="module")
def database():
    url = os.environ["TEST_DATABASE_URL"]
    assert "/diploma_db" not in url
    env = {**os.environ, "DATABASE_URL": url}
    env.pop("DATABASE_URL_FILE", None)
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], env=env, check=True)
    db = Database(url)
    try:
        yield db
    finally:
        db.dispose()


@pytest.fixture(scope="module")
def catalog():
    return PackagedEcsCatalog.load()


@pytest.fixture
def source(database):
    connection_id, source_id = uuid4(), uuid4()
    with database.session_factory.begin() as session:
        session.add(KafkaConnection(id=connection_id, name=str(connection_id),
            bootstrap_servers=["kafka:9092"], cluster_identity="synthetic"))
        session.flush()
        session.add(Source(id=source_id, name="Authentication " + str(source_id),
            connection_id=connection_id, topic_name=str(source_id), is_enabled=True))
    try:
        yield source_id
    finally:
        with database.session_factory.begin() as session:
            aggregates = list(session.scalars(select(Aggregate).where(Aggregate.source_identity == source_id)))
            for aggregate in aggregates:
                delete_aggregate(session, aggregate.id)
            session.execute(delete(AvailabilityMarker).where(AvailabilityMarker.source_identity == source_id))
            session.execute(delete(ParsedLog).where(ParsedLog.source_id == source_id))
            session.execute(delete(ProcessedKafkaRecord).where(ProcessedKafkaRecord.source_id == source_id))
            session.execute(delete(Source).where(Source.id == source_id))
            session.execute(delete(KafkaConnection).where(KafkaConnection.id == connection_id))


def service(session, catalog):
    result = build_aggregate_service(session, catalog)
    result.capture = SimpleNamespace(status=lambda: SimpleNamespace(ready=True, reason="ready"))
    return result


def request(source_id, now=None, duration=1):
    now = now or datetime.now(UTC)
    anchor = (now - timedelta(seconds=max(20, duration + 1))).replace(microsecond=0)
    return AggregatePreviewRequest(source_id=source_id,
        aggregate_rule={"features": [{"key": "n", "name": "Count", "op": "count"}]},
        schedule={"boundary": anchor, "timezone": "UTC", "duration_seconds": duration},
        window_start_utc=anchor, time_basis="event_time")


@pytest.mark.parametrize("duration", [1, 60])
def test_request_fixture_uses_a_completed_preview_window(duration):
    now = datetime(2026, 1, 1, tzinfo=UTC)
    body = request(uuid4(), now=now, duration=duration)
    assert body.window_start_utc + timedelta(seconds=duration) <= now


def create(database, catalog, source_id, key=None, duration=1, clock_shift=0):
    body = request(source_id, duration=duration)
    with database.session_factory() as session:
        api = service(session, catalog)
        if clock_shift:
            clock = api.repository.now
            api.repository.now = lambda: clock() - timedelta(seconds=clock_shift)
        validated = api.validate(body)
        body = CreateAggregateRequest(**body.model_dump(), name="Authentication count", rule_hash=validated.rule_hash)
        return api.create(body, UUID(int=1), key or str(uuid4())), body


def close_one(api, aggregate_id):
    for _ in range(6):
        result = api.advance(aggregate_id)
        if result["state"] == "closed":
            return api.repository.session.get(WorkingWindowPoint, result["point_id"])
    pytest.fail("No due event-time window was closed")


def test_preview_readonly_hash_and_creation_idempotency(database, catalog, source):
    body = request(source)
    with database.session_factory() as session:
        api = service(session, catalog)
        checked, preview = api.validate(body), api.preview(body)
        assert checked.rule_hash == preview.rule_hash
        assert preview.completeness == "complete" and preview.event_count == 0
        assert session.scalar(select(func.count()).select_from(Aggregate).where(Aggregate.source_identity == source)) == 0
        assert session.scalar(select(func.count()).select_from(AvailabilityMarker).where(AvailabilityMarker.source_identity == source)) == 0
    card, body = create(database, catalog, source, key="repeat")
    with database.session_factory() as session:
        api = service(session, catalog)
        assert api.create(body, UUID(int=1), "repeat").id == card.id
        changed = body.model_copy(update={"name": "different"})
        with pytest.raises(DomainError, match="Ключ"):
            api.create(changed, UUID(int=1), "repeat")
        stored = api.repository.schedule(card.schedule_id)
        assert stored.anchor_utc + timedelta(seconds=stored.next_index) <= card.created_at < stored.anchor_utc + timedelta(seconds=stored.next_index + 1)
        assert api.copy(card.id).aggregate_rule == body.aggregate_rule


def test_concurrent_create_returns_one_uuid(database, catalog, source):
    base = request(source)
    with database.session_factory() as session:
        digest = service(session, catalog).validate(base).rule_hash
    body = CreateAggregateRequest(**base.model_dump(), name="Concurrent", rule_hash=digest)
    def run():
        with database.session_factory() as session:
            return service(session, catalog).create(body, UUID(int=1), "concurrent").id
    with ThreadPoolExecutor(max_workers=2) as pool:
        ids = list(pool.map(lambda _: run(), range(2)))
    assert ids[0] == ids[1]


def test_worker_closes_due_snapshot_without_wal_barrier(database, catalog, source):
    card, _ = create(database, catalog, source, clock_shift=3)
    with database.session_factory() as session:
        api = service(session, catalog)
        point = close_one(api, card.id)
        assert point.features[0]["value"] == 0 and point.completeness == "complete"
        assert api.repository.schedule(card.schedule_id).next_index == card.next_index + 1
        assert api.points(card.id, 50, 0)["total"] == 1
        assert session.scalar(select(func.count()).select_from(AvailabilityMarker).where(
            AvailabilityMarker.source_identity == source)) == 0


def test_archive_and_delete_block_closure_and_resume_retains_intentional_gap(database, catalog, source):
    card, _ = create(database, catalog, source, clock_shift=3)
    with database.session_factory() as session:
        api = service(session, catalog)
        api.lifecycle(card.id, "archive", UUID(int=1))
        for _ in range(3):
            api.advance(card.id)
        assert api.points(card.id, 50, 0)["total"] == 0
        assert api.card(card.id).next_index == card.next_index
        resumed = api.lifecycle(card.id, "resume", UUID(int=1))
        assert resumed.skipped_ranges and resumed.next_index > card.next_index
        deleted = api.lifecycle(card.id, "delete", UUID(int=1))
        assert deleted.state == "deleted"
        assert api.lifecycle(card.id, "delete", UUID(int=1)).deleted_at == deleted.deleted_at
        for _ in range(3):
            api.advance(card.id)
        with pytest.raises(DomainError):
            api.copy(card.id)
        assert api.points(card.id, 50, 0)["total"] == 0


def test_old_calculation_is_rejected_and_missing_canonical_point_is_not_ready(database, catalog, source):
    card, body = create(database, catalog, source)
    with database.session_factory() as session:
        api = service(session, catalog)
        with pytest.raises(DomainError) as error:
            api.historical(card.id, HistoricalRequest(window_start_utc=body.window_start_utc))
        assert error.value.code == "canonical_point_not_ready"
        for basis in (None, "availability_at", "backend_processed_at_historical"):
            with pytest.raises(DomainError) as error:
                api.preview(body.model_copy(update={"time_basis": basis}))
            assert error.value.code == "legacy_calculation_removed"
        assert api.preview(body).completeness == "complete"


def test_api_permissions_csrf_and_typed_contract(database, catalog, source):
    config = Settings(environment="test", database_url=os.environ["TEST_DATABASE_URL"])
    app = create_app(config, database=database, ecs_catalog=catalog, start_consumers=False)
    def dependency(session=Depends(get_session)):
        return service(session, catalog)
    app.dependency_overrides[get_aggregate_service] = dependency
    names = ["aggregate-guest-" + uuid4().hex, "aggregate-admin-" + uuid4().hex]
    with database.session_factory.begin() as session:
        for name, role in zip(names, ["00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000001"]):
            session.add(User(username=name, password_hash=hash_password("synthetic-password"), display_name=name, role_id=UUID(role)))
    with TestClient(app) as client:
        body = request(source).model_dump(mode="json")
        for name in names:
            assert client.post("/api/v1/auth/local/login", json={"username": name, "password": "synthetic-password"}).status_code == 200
            csrf = {"X-CSRF-Token": client.get("/api/v1/auth/session").json()["csrf_token"]}
            expected = 403 if name == names[0] else 200
            assert client.get("/api/v1/aggregates/operators").status_code == expected
            assert client.get(f"/api/v1/aggregates/sources/{source}/fields").status_code == expected
            validation_body = {k: v for k, v in body.items() if k in {"source_id", "aggregate_rule", "schedule"}}
            assert client.post("/api/v1/aggregates/validate", json=validation_body, headers=csrf).status_code == expected
            assert client.post("/api/v1/aggregates/preview", json=body, headers=csrf).status_code == expected
            assert client.post("/api/v1/aggregates/preview", json=body).status_code == 403
            if name == names[0]:
                aggregate_id = uuid4()
                for route in ["", f"/{aggregate_id}", f"/{aggregate_id}/points", f"/{aggregate_id}/copy"]:
                    assert client.get("/api/v1/aggregates" + route).status_code == 403
                for action in ["archive", "resume", "historical-points"]:
                    response = client.post(f"/api/v1/aggregates/{aggregate_id}/{action}",
                        headers=csrf, json={"window_start_utc": body["window_start_utc"]} if action == "historical-points" else None)
                    assert response.status_code == 403
                assert client.delete(f"/api/v1/aggregates/{aggregate_id}", headers=csrf).status_code == 403
            assert client.post("/api/v1/auth/logout", headers=csrf).status_code == 200


def test_catalog_search_is_server_side_literal_scoped_and_backward_compatible(database, catalog, source):
    prefix = "Каталог-" + uuid4().hex
    at = datetime.now(UTC).replace(microsecond=0)
    identities = [uuid4() for _ in range(60)]
    active = sorted(identities[:57], reverse=True)
    target, special = active[-1], active[-2]
    names = {identity: f"{prefix} запись {index}" for index, identity in enumerate(identities)}
    names[target] = prefix + " Нужная ЦЕЛЬ"
    names[special] = prefix + " буквальные %_\\' символы"
    names[active[0]] = prefix + " одинаковое имя"
    names[active[1]] = names[active[0]]
    with database.session_factory.begin() as session:
        for index, identity in enumerate(identities):
            schedule = uuid4()
            session.add(AggregateSchedule(id=schedule, source_identity=source, anchor_utc=at - timedelta(seconds=600), duration_seconds=2, next_index=300, semantic_basis="event_time", bootstrap_at=at, online_start_index=300))
            session.flush()
            session.add(Aggregate(id=identity, schedule_id=schedule, name=names[identity], source_identity=source,
                source_name="Источник поиска", source_type="kafka", rule={"features": [{"key":"n","name":"Count","op":"count"}]},
                rule_hash="f" * 64, original_boundary=at.isoformat(), timezone="UTC", created_at=at,
                created_by=uuid4(), state="active" if index < 57 else "archived" if index < 59 else "deleted",
                request_key=uuid4().hex, request_hash="e" * 64, semantic_basis="event_time", execution_mode="current"))
    app = create_app(Settings(environment="test", database_url=os.environ["TEST_DATABASE_URL"]), database=database, ecs_catalog=catalog, start_consumers=False)
    names_user = ["search-admin-" + uuid4().hex, "search-guest-" + uuid4().hex]
    with database.session_factory.begin() as session:
        for name, role in zip(names_user, [1, 2]):
            session.add(User(username=name, display_name=name, password_hash=hash_password("synthetic-password"), role_id=UUID(f"00000000-0000-4000-8000-{role:012d}")))
    with TestClient(app) as client:
        assert client.post("/api/v1/auth/local/login", json={"username":names_user[0],"password":"synthetic-password"}).status_code == 200
        def listing(**params):
            response = client.get("/api/v1/aggregates", params={"q":prefix, **params})
            assert response.status_code == 200, response.text
            return response.json()
        page = listing(state="active", limit=10)
        assert page["total"] == 57 and [UUID(item["id"]) for item in page["items"]] == active[:10]
        assert str(target) not in [item["id"] for item in page["items"]]
        found = listing(q="  " + prefix.upper() + " нужная цель  ", state="active", limit=10)
        assert found["total"] == 1 and found["items"][0]["id"] == str(target)
        assert listing(q="%_\\'", state="active")["items"][0]["id"] == str(special)
        assert listing(q="%_\\'", state="active")["total"] == 1
        assert listing(q=prefix + " одинаковое имя", state="active")["total"] == 2
        assert listing(state="archived")["total"] == 2
        assert listing()["total"] == 59 and len(listing()["items"]) == 50
        assert len(listing(offset=50)["items"]) == 9
        assert listing(q="несуществующее имя")["total"] == 0
        old = client.get("/api/v1/aggregates", params={"limit":50,"offset":0})
        assert old.status_code == 200 and len(old.json()["items"]) == 50
        assert client.get("/api/v1/aggregates", params={"q":" "}).json()["total"] == old.json()["total"]
        for params in ({"q":"x" * 201}, {"state":"deleted"}, {"state":"invalid"}, {"limit":0}, {"limit":101}, {"offset":-1}, {"offset":100001}):
            assert client.get("/api/v1/aggregates", params=params).status_code == 422
        csrf = client.get("/api/v1/auth/session").json()["csrf_token"]
        assert client.post("/api/v1/auth/logout", headers={"X-CSRF-Token":csrf}).status_code == 200
        assert client.post("/api/v1/auth/local/login", json={"username":names_user[1],"password":"synthetic-password"}).status_code == 200
        assert client.get("/api/v1/aggregates", params={"q":prefix,"state":"active","limit":10}).status_code == 403
    with database.session_factory() as session:
        result = service(session, catalog).list(10, 0, "  " + prefix + "  ", "active")
        assert [item.id for item in result["items"]] == active[:10] and result["total"] == 57


def test_worker_error_isolation_and_interruptible_wait(database, catalog, source):
    stop = Event()
    worker = AggregateWorker(database.session_factory, lambda session, cat: service(session, cat), catalog, stop)
    thread = Thread(target=worker.run)
    thread.start()
    stop.set()
    thread.join(1)
    assert not thread.is_alive()


def test_worker_due_excludes_admitted_targets_and_preserves_success_rotation(database, catalog, source):
    identities = {create(database, catalog, source, key=str(uuid4()))[0].id for _ in range(3)}
    with database.session_factory() as session:
        api = service(session, catalog)
        original = [identity for identity in api.due(1000) if identity in identities]
        assert len(original) == 3
        admitted = original[:2]
        remaining = api.due(1000, exclude_ids=admitted)
        assert not set(admitted) & set(remaining)
        assert [identity for identity in remaining if identity in identities] == original[2:]
        api.record_worker_error(original[0], None)
        rotated = [identity for identity in api.due(1000) if identity in identities]
        assert rotated == [*original[1:], original[0]]


def test_idle_scan_and_history_turns_report_real_online_backlog_without_new_wait(database, catalog, source):
    card, _ = create(database, catalog, source, clock_shift=3)
    with database.session_factory() as session:
        api = service(session, catalog)
        # Exercise the forced background phases with genuinely overdue online
        # work; ordinary active phases now close that work first.
        session.get(AggregateSchedule, card.schedule_id).turn = FOREGROUND_BURST
        session.commit()
        scan = api.advance(card.id)
        session.get(AggregateSchedule, card.schedule_id).turn = 2 * (FOREGROUND_BURST + 1) - 1
        session.commit()
        history = api.advance(card.id)
        assert scan == {"state": "scanned", "records": 0, "work_available": True}
        assert history == {"state": "history_waiting", "work_available": True}
        assert api.advance(card.id)["state"] == "closed"

    waiting, _ = create(database, catalog, source, duration=60)
    with database.session_factory() as session:
        api = service(session, catalog)
        assert api.advance(waiting.id) == {"state": "scanned", "records": 0, "work_available": False}
        for _ in range(FOREGROUND_BURST + 1):
            assert api.advance(waiting.id) == {"state": "history_waiting", "work_available": False}
        assert api.advance(waiting.id) == {"state": "scanned", "records": 0, "work_available": False}
        assert api.card(waiting.id).next_index == waiting.next_index
        assert api.points(waiting.id, 50, 0)["total"] == 0


def test_failed_point_write_rolls_back_manifest_closure_payload_and_cursor(database, catalog, source, monkeypatch):
    card, _ = create(database, catalog, source, clock_shift=3)
    with database.session_factory() as session:
        api = service(session, catalog)
        original = api.repository.add_event_point
        def failing(*args):
            original(*args)
            raise RuntimeError("simulated crash before commit")
        monkeypatch.setattr(api.repository, "add_event_point", failing)
        with pytest.raises(RuntimeError, match="simulated crash"):
            close_one(api, card.id)
    with database.session_factory() as session:
        api = service(session, catalog)
        assert api.points(card.id, 50, 0)["total"] == 0
        assert api.repository.closure_at(card.schedule_id, card.next_index) is None
        assert api.repository.schedule(card.schedule_id).next_index == card.next_index
        close_one(api, card.id)
        assert api.points(card.id, 50, 0)["total"] == 1


def test_unproven_raw_rows_do_not_create_history_or_mutate_saved_snapshot(database, catalog, source):
    card, body = create(database, catalog, source, clock_shift=3)
    with database.session_factory.begin() as session:
        origin = session.get(Source, source)
        session.add(ParsedLog(source_id=source, connection_id=origin.connection_id,
            normalizer_version=1, normalizer_name="legacy", source_name=origin.name,
            connection_name="legacy", kafka_topic=origin.topic_name, kafka_partition=0,
            kafka_offset=0, deduplication_key=str(uuid4()), fluent_bit_collected_at=body.window_start_utc,
            backend_received_at=body.window_start_utc, backend_processed_at=body.window_start_utc,
            raw="synthetic legacy", ecs_data={"@timestamp": body.window_start_utc.isoformat()}))
    with database.session_factory() as session:
        api = service(session, catalog)
        point = close_one(api, card.id)
        saved_id, saved_features = point.id, point.features
        assert point.event_count == 0
        with pytest.raises(DomainError) as error:
            api.historical(card.id, HistoricalRequest(window_start_utc=body.window_start_utc))
        assert error.value.code == "canonical_point_not_ready"
    with database.session_factory.begin() as session:
        session.execute(delete(ParsedLog).where(ParsedLog.source_id == source))
        session.execute(delete(Source).where(Source.id == source))
    def read():
        with database.session_factory() as session:
            return service(session, catalog).point_by_id(card.id, saved_id).model_dump()
    with ThreadPoolExecutor(max_workers=2) as pool:
        points = list(pool.map(lambda _: read(), range(2)))
    assert points[0] == points[1]
    assert points[0]["features"] == saved_features


def test_worker_one_error_does_not_block_other_aggregate(database, catalog):
    ids = [uuid4(), uuid4()]
    seen = []
    def build(session, _):
        api = service(session, catalog)
        api.repository.due = lambda limit, exclude_ids=(): [identity for identity in ids if identity not in exclude_ids][:limit]
        api.maintenance_due = lambda *_, **__: []
        api.repository.record_error = lambda *_: None
        def advance(aggregate_id, *, execution_started):
            assert execution_started is not None
            seen.append(aggregate_id)
            if aggregate_id == ids[0]:
                raise DomainError("aggregate_timeout", "synthetic", 503)
        api.advance = advance
        return api
    worker = AggregateWorker(database.session_factory, build, catalog, Event())
    worker.tick()
    assert seen == ids
    worker.tick()
    assert seen == [*ids, ids[1]]


def test_worker_uses_only_declared_service_scenarios(database, catalog):
    ids, seen, errors = [uuid4(), uuid4()], [], []

    def advance(aggregate_id, *, execution_started):
        assert execution_started is not None
        seen.append(aggregate_id)
        if aggregate_id == ids[0]:
            raise DomainError("aggregate_timeout", "synthetic", 503)

    # No repository or unit of work is exposed by this implementation.
    api = SimpleNamespace(due=lambda limit, exclude_ids=(): [identity for identity in ids if identity not in exclude_ids][:limit],
                          maintenance_due=lambda *_, **__: [], advance=advance,
                          record_worker_error=lambda *args: errors.append(args))
    worker = AggregateWorker(database.session_factory, lambda *_: api, catalog, Event())
    worker.tick()
    assert seen == ids
    assert errors == [(ids[0], "aggregate_timeout"), (ids[1], None)]


@pytest.mark.parametrize("result,idle", [
    ({"state": "waiting_close_deadline"}, True), ({"state": "history_waiting"}, True),
    ({"state": "scanned", "records": 499}, True), ({"state": "scanned", "records": 500}, False),
    ({"state": "history_waiting", "work_available": True}, False),
    ({"state": "scanned", "records": 0, "work_available": True}, False),
    ({"state": "closed"}, False),
])
def test_worker_backs_off_idle_turns_but_keeps_full_pages_and_closes_ready(database, catalog, monkeypatch, result, idle):
    from app.services.implementations import aggregate_worker as module
    identity, calls, clock = uuid4(), [], [100.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])

    def advance(target, *, execution_started):
        assert execution_started == clock[0]
        calls.append(target)
        return result

    api = SimpleNamespace(due=lambda _, exclude_ids=(): [] if identity in exclude_ids else [identity],
        maintenance_due=lambda *_, **__: [], record_worker_error=lambda *_: None,
        advance=advance)
    worker = AggregateWorker(database.session_factory, lambda *_: api, catalog, Event())
    worker.tick()
    worker.tick()
    assert len(calls) == (1 if idle else 2)
    clock[0] += .25
    worker.tick()
    assert len(calls) == (2 if idle else 3)


def test_worker_slow_aggregate_does_not_hold_unrelated_one(database, catalog):
    ids, release, second = [uuid4(), uuid4()], Event(), Event()
    def build(session, _):
        api = service(session, catalog)
        api.repository.due = lambda limit, exclude_ids=(): [identity for identity in ids if identity not in exclude_ids][:limit]
        api.maintenance_due = lambda *_, **__: []
        api.repository.record_error = lambda *_: None
        def advance(aggregate_id, *, execution_started):
            assert execution_started is not None
            if aggregate_id == ids[0]:
                assert release.wait(3)
            else:
                second.set()
        api.advance = advance
        return api
    worker = AggregateWorker(database.session_factory, build, catalog, Event())
    with ThreadPoolExecutor(max_workers=4) as executor:
        try:
            worker.tick(executor)
            assert second.wait(1), "unrelated aggregate was delayed by the first calculation"
        finally:
            release.set()


def test_definition_and_schedule_are_immutable_but_copy_creates_new_uuid(database, catalog, source):
    card, body = create(database, catalog, source)
    with database.session_factory() as session:
        session.get(Aggregate, card.id).rule = {"features": []}
        with pytest.raises(DBAPIError):
            session.commit()
        session.rollback()
        session.get(AggregateSchedule, card.schedule_id).duration_seconds = 2
        with pytest.raises(DBAPIError):
            session.commit()
        session.rollback()
        api = service(session, catalog)
        copied = api.copy(card.id)
        copy_body = CreateAggregateRequest(**copied.model_dump(exclude_none=True),
            window_start_utc=body.window_start_utc, time_basis=body.time_basis, name="Copy", rule_hash=body.rule_hash)
        new = api.create(copy_body, UUID(int=1), "copy")
        assert new.id != card.id and new.rule_hash == card.rule_hash


def test_preview_budget_refuses_before_calculation(database, catalog, source):
    with database.session_factory() as session:
        api = service(session, catalog)
        api.repository.snapshot_events = lambda *_, **__: ([], [], 100001, 256)
        api.repository.event_source_rows = lambda *_: 100001
        api.repository.event_statistics = lambda *_: pytest.fail("over-budget preview was calculated")
        with pytest.raises(DomainError) as error:
            api.preview(request(source))
        assert error.value.code == "resource_budget_exceeded"
        assert error.value.details["path"] == "preview_rows"


def test_runtime_timeout_rolls_back_point_and_cursor(database, catalog, source):
    card, _ = create(database, catalog, source, clock_shift=3)
    with database.session_factory() as session:
        api = service(session, catalog)
        def slow(*_):
            api.repository.read_budget(20)
            session.execute(select(func.pg_sleep(.1)))
            pytest.fail("statement_timeout did not interrupt the calculation")
        api.repository.event_statistics = slow
        with pytest.raises(DomainError) as error:
            close_one(api, card.id)
        assert error.value.code == "aggregate_timeout"
        assert api.repository.schedule(card.schedule_id).next_index == card.next_index
        assert api.points(card.id, 50, 0)["total"] == 0


def test_serialization_conflict_requires_new_turn_without_restarting_whole_budget(database, catalog, source):
    card, _ = create(database, catalog, source, clock_shift=3)
    with database.session_factory() as session:
        api = service(session, catalog)
        original, calls = api.repository.event_statistics, []
        def conflict(*args):
            values = original(*args)
            calls.append(True)
            if len(calls) == 1:
                raise DBAPIError(None, None, SerializationFailure("synthetic serialization conflict"))
            return values
        api.repository.event_statistics = conflict
        with pytest.raises(DomainError) as error:
            close_one(api, card.id)
        assert error.value.code == "aggregate_retry_required" and len(calls) == 1
        assert api.repository.schedule(card.schedule_id).next_index == card.next_index
        close_one(api, card.id)
        assert len(calls) == 2
        assert api.points(card.id, 50, 0)["total"] == 1

@pytest.mark.parametrize('op', ['median', 'percentile', 'mode_count', 'mode_share'])
def test_exact_materialization_budget_refuses_before_query(database, catalog, source, op):
    from app.aggregation.rules import AggregateRule
    body = request(source)
    body.aggregate_rule = AggregateRule.model_validate({'features': [{
        'key': 'n', 'name': 'N', 'op': op, 'field': 'network.bytes',
        **({'percentile': .5} if op == 'percentile' else {})}]})
    with database.session_factory() as session:
        api = service(session, catalog)
        api.repository.snapshot_events = lambda *_, **__: ([], [], 10000, 10000)
        api.repository.event_source_rows = lambda *_: 10000
        api.repository.event_statistics = lambda *_: pytest.fail('over-budget exact materialization reached SQL')
        with pytest.raises(DomainError) as error:
            api.preview(body)
        assert error.value.code == 'resource_budget_exceeded'
        assert error.value.details['path'] == 'total_memory'


def test_legacy_create_digest_survives_optional_operator_parameters(database, catalog, source):
    import hashlib
    import json
    key = 'legacy-digest'
    card, body = create(database, catalog, source, key=key)
    legacy = body.model_dump(mode='json')
    legacy['aggregate_rule']['features'] = [
        {name: getattr(f, name) for name in ('key', 'name', 'op', 'field', 'where', 'expr')}
        for f in body.aggregate_rule.features]
    expected = hashlib.sha256(json.dumps(legacy, ensure_ascii=False,
        sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    with database.session_factory() as session:
        assert session.get(Aggregate, card.id).request_hash == expected
        assert service(session, catalog).create(body, UUID(int=1), key).id == card.id


def test_validate_accepts_normalizer_second_offset_date(database, catalog, source):
    from app.aggregation.rules import AggregateRule
    from app.normalization.convert import convert_value
    normalized = convert_value(catalog.get('event.created'), '1900-01-01T00:00:00+05:21:10')
    body = request(source)
    body.aggregate_rule = AggregateRule.model_validate({'where': {
        'op': 'eq', 'field': 'event.created', 'value': normalized},
        'features': [{'key': 'n', 'name': 'N', 'op': 'count'}]})
    with database.session_factory() as session:
        api = service(session, catalog)
        checked = api.validate(body)
        assert api.preview(body).rule_hash == checked.rule_hash


@pytest.mark.parametrize('op', ['has_all', 'has_none', 'has_any'])
def test_array_literal_budget_refuses_preview_and_atomic_point(database, catalog, source, op):
    from app.aggregation.rules import AggregateRule
    body = request(source)
    body.aggregate_rule = AggregateRule.model_validate({'where': {
        'op': op, 'field': 'host.ip', 'value': [f'2001:db8::{i:x}' for i in range(256)]},
        'features': [{'key': 'n', 'name': 'N', 'op': 'count'}]})
    with database.session_factory() as session:
        api = service(session, catalog)
        clock = api.repository.now
        api.repository.now = lambda: clock() - timedelta(seconds=3)
        checked = api.validate(body)
        card = api.create(CreateAggregateRequest(**body.model_dump(), name='Array budget',
            rule_hash=checked.rule_hash), UUID(int=1), uuid4().hex)
    with database.session_factory() as session:
        api = service(session, catalog)
        api.budget_limits = replace(api.budget_limits, cost=1000)
        api.repository.snapshot_events = lambda *_, **__: ([], [], 1, 22000)
        api.repository.event_source_rows = lambda *_: 1
        api.repository.event_statistics = lambda *_: pytest.fail('array budget did not stop calculation')
        for action in [lambda: api.preview(body), lambda: close_one(api, card.id)]:
            with pytest.raises(DomainError) as error:
                action()
            assert error.value.code == 'resource_budget_exceeded'
            assert error.value.details['path'] == 'operator_cost'
            assert api.points(card.id, 50, 0)['total'] == 0
            assert api.repository.schedule(card.schedule_id).next_index == card.next_index


@pytest.mark.parametrize('value', ['0001-01-01T00:00:00+01:00', '9999-12-31T23:59:59-01:00'])
def test_validate_and_preview_reject_utc_overflow_as_rule_error(database, catalog, source, value):
    from app.aggregation.rules import AggregateRule
    from app.normalization.convert import convert_value
    normalized = convert_value(catalog.get('event.created'), value)
    body = request(source)
    body.aggregate_rule = AggregateRule.model_validate({'where': {
        'op': 'eq', 'field': 'event.created', 'value': normalized},
        'features': [{'key': 'n', 'name': 'N', 'op': 'count'}]})
    with database.session_factory() as session:
        api = service(session, catalog)
        for action in [lambda: api.validate(body), lambda: api.preview(body)]:
            with pytest.raises(DomainError) as error:
                action()
            assert error.value.status_code == 422
            assert error.value.code == 'invalid_literal_type'
            assert error.value.details['path'] == 'where.value'
        assert session.scalar(select(func.count()).select_from(Aggregate).where(
            Aggregate.source_identity == source)) == 0


@pytest.mark.parametrize('value', ['0001-01-01T00:00:00+01:00', '9999-12-31T23:59:59-01:00'])
@pytest.mark.parametrize('occurred', ['2000-01-01T00:00:00Z',
    '0001-01-01T00:00:00Z', '9999-12-31T23:59:59.999999Z'])
def test_bad_date_commits_quality_point_and_cursor_without_retry(database, catalog, source, value, occurred):
    from app.aggregation.rules import AggregateRule
    from app.services.implementations.window_schedule import WindowSchedule
    body = request(source)
    body.aggregate_rule = AggregateRule.model_validate({'features': [
        {'key': 'n', 'name': 'N', 'op': 'count'},
        {'key': 'mode', 'name': 'Mode', 'op': 'mode_count', 'field': 'event.created'}]})
    with database.session_factory() as session:
        api = service(session, catalog)
        clock = api.repository.now
        api.repository.now = lambda: clock() - timedelta(seconds=3)
        checked = api.validate(body)
        card = api.create(CreateAggregateRequest(**body.model_dump(), name='Date boundary',
            rule_hash=checked.rule_hash), UUID(int=1), uuid4().hex)
    with database.session_factory.begin() as session:
        origin = session.get(Source, source)
        stored = session.get(AggregateSchedule, card.schedule_id)
        window = WindowSchedule(stored.anchor_utc, stored.duration_seconds).window(card.next_index)
        at = window.start + timedelta(microseconds=500000)
        event = ParsedLog(id=uuid4(), source_id=source, connection_id=origin.connection_id,
            normalizer_version=1, normalizer_name='synthetic', source_name=origin.name,
            connection_name='synthetic', kafka_topic=origin.topic_name, kafka_partition=0,
            kafka_offset=0, deduplication_key=str(uuid4()), fluent_bit_collected_at=at,
            backend_received_at=at, backend_processed_at=at, raw='synthetic date',
            ecs_data={'@timestamp': occurred, 'event': {'created': value}})
        from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
        record = ProcessedKafkaRecord(id=uuid4(), connection_id=origin.connection_id,
            connection_identity=origin.connection_id, kafka_topic_identity="synthetic",
            source_id=source, kafka_topic=origin.topic_name, kafka_partition=0, kafka_offset=0,
            result_status="complete", backend_received_at=at, backend_processed_at=at)
        event.ecs_data = {"@timestamp": at.isoformat(), "event": {"created": value, "original": occurred}}
        assert SqlAlchemyProcessingRepository(session).store_event(record, event)
        event_id = event.id
    with database.session_factory() as session:
        api = service(session, catalog)
        point = close_one(api, card.id)
        assert point.event_count == 1 and point.features[0]['value'] == 1
        assert point.features[1] == {'key': 'mode', 'value': None, 'reason': 'invalid_input_type',
                                     'valid_count': 0, 'invalid_count': 1}
        assert api.repository.schedule(card.schedule_id).next_index == card.next_index + 1
        saved_id, saved_features = point.id, point.features
        assert api.point_by_id(card.id, point.id).id == point.id
        assert api.repository.schedule(card.schedule_id).next_index == card.next_index + 1
    with database.session_factory.begin() as session:
        session.get(ParsedLog, event_id).ecs_data = {'@timestamp': at.isoformat(),
                                                    'event': {'created': '2000-01-01T00:00:00Z'}}
    with database.session_factory() as session:
        for zone in ['UTC', 'America/New_York', 'Pacific/Kiritimati']:
            session.execute(select(func.set_config('TimeZone', zone, True)))
            saved = service(session, catalog).points(card.id, 50, 0)['items'][0]
            assert saved.id == saved_id and [item.model_dump() for item in saved.features] == saved_features
            assert saved.event_time_min == saved.event_time_max == at


def test_saved_historical_utc_bounds_are_read_without_timezone_overflow(database, catalog, source):
    card, body = create(database, catalog, source)
    with database.session_factory.begin() as session:
        point = HistoricalAggregatePoint(aggregate_id=card.id, start_utc=body.window_start_utc,
            end_utc=body.window_start_utc + timedelta(seconds=1), actual_started_at=card.created_at,
            actual_completed_at=card.created_at, event_count=2,
            event_time_min=datetime.min.replace(tzinfo=UTC), event_time_max=datetime.max.replace(tzinfo=UTC),
            features=[{'key': 'n', 'value': 2, 'reason': None, 'valid_count': 2, 'invalid_count': 0}],
            time_basis='backend_processed_at_historical', completeness='unknown',
            completeness_reason='legacy_coverage_unproven', evidence_refs={})
        session.add(point)
    for zone in ['UTC', 'America/New_York', 'Pacific/Kiritimati']:
        with database.session_factory() as session:
            session.execute(select(func.set_config('TimeZone', zone, True)))
            api = service(session, catalog)
            for saved in [api.points(card.id, 50, 0)['items'][0],
                          api.repository.historical(card.id, body.window_start_utc)]:
                assert saved.event_time_min == datetime.min.replace(tzinfo=UTC)
                assert saved.event_time_max == datetime.max.replace(tzinfo=UTC)
            assert not session.dirty
