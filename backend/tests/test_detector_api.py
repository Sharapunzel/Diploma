"""Real app/session/DB contracts for detector commands and bounded public reads."""
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from fastapi import Depends
from fastapi.testclient import TestClient
from psycopg.errors import UniqueViolation
from sqlalchemy import event, func, select, text

from app.adapters.detector_lock import DetectorSessionLock
from app.aggregate_composition import build_aggregate_service
from app.config import Settings
from app.core.errors import DomainError
from app.core.security import hash_password
from app.db import Database, get_session
from app.dependencies import get_detector_service
from app.detector_composition import build_detector_service
from app.ecs import PackagedEcsCatalog
from app.main import create_app
from app.models import (
    Aggregate,
    AggregateSchedule,
    AuthSession,
    Detector,
    DetectorCommand,
    DetectorOutcome,
    DetectorRecovery,
    DetectorWarmup,
    Role,
    User,
    WorkingWindowPoint,
)

ADMIN = UUID("00000000-0000-4000-8000-000000000001")
GUEST = UUID("00000000-0000-4000-8000-000000000002")


@pytest.fixture(scope="module")
def database():
    url = os.environ["TEST_DATABASE_URL"]
    assert "/diploma_test" in url and "/diploma_db" not in url
    env = {**os.environ, "DATABASE_URL": url}
    env.pop("DATABASE_URL_FILE", None)
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], env=env, check=True)
    db = Database(url)
    yield db
    db.dispose()


@pytest.fixture(scope="module")
def catalog():
    return PackagedEcsCatalog.load()


@pytest.fixture
def aggregate(database):
    identity, schedule_id, source_id = uuid4(), uuid4(), uuid4()
    now = datetime.now(UTC)
    with database.session_factory.begin() as session:
        session.add(AggregateSchedule(id=schedule_id, source_identity=source_id,
            anchor_utc=now - timedelta(seconds=600), duration_seconds=10, next_index=60))
        session.flush()
        session.add(Aggregate(id=identity, schedule_id=schedule_id, name="Aggregate", source_identity=source_id,
            source_name="Synthetic source", source_type="kafka", rule={"features": [{"key": "n", "name": "Count", "op": "count"}]},
            rule_hash="f" * 64, original_boundary=now.isoformat(), timezone="UTC", created_at=now,
            created_by=uuid4(), state="active", request_key=uuid4().hex, request_hash="e" * 64))
    return identity


def body(aggregate, **changes):
    return {"aggregate_id": str(aggregate), "name": "Detector", **changes}


def command(database, catalog, actor, action, key, target=None, payload=None):
    with database.session_factory() as session:
        return build_detector_service(session, catalog).command(action, actor, key, target, payload)


def test_replay_survives_opposite_commands_and_new_service(database, catalog, aggregate):
    actor = uuid4()
    created = command(database, catalog, actor, "create", "create", payload=body(aggregate, name="  Detector  "))
    identity = created["detector_id"]
    assert command(database, catalog, actor, "create", "create", payload=body(aggregate,
        config={"trees": 100, "sample_size": 256, "output_after": 256, "seed": 42},
        accelerated=False, step_seconds=None)) == created
    started = command(database, catalog, actor, "start", "start", identity)
    stopped = command(database, catalog, actor, "stop", "stop", identity)
    assert command(database, catalog, actor, "start", "start", identity) == started
    with database.session_factory() as session:
        assert session.get(Detector, identity).execution_state == "stopped"
    restarted = command(database, catalog, actor, "start", "new-start", identity)
    assert restarted["start_request_id"] != started["start_request_id"]
    assert command(database, catalog, actor, "stop", "stop", identity) == stopped
    with database.session_factory() as session:
        assert session.get(Detector, identity).execution_state == "requested"
        revision = session.get(Detector, identity).revision
    command(database, catalog, actor, "start", "another-start", identity)
    with database.session_factory() as session:
        assert session.get(Detector, identity).revision == revision
    with pytest.raises(DomainError) as error:
        command(database, catalog, actor, "create", "create", payload=body(aggregate, name="Other"))
    assert error.value.code == "idempotency_conflict"
    with pytest.raises(DomainError) as error:
        command(database, catalog, actor, "start", "start", uuid4())
    assert error.value.code == "idempotency_conflict"  # Key lookup precedes current target validation.
    # Different actor/action scopes do not collide.
    assert command(database, catalog, uuid4(), "create", "create", payload=body(aggregate))["detector_id"] != identity
    command(database, catalog, actor, "stop", "create", identity)


def test_concurrent_independent_sessions(database, catalog, aggregate):
    actor = uuid4()
    def create(_):
        return command(database, catalog, actor, "create", "parallel", payload=body(aggregate))
    with ThreadPoolExecutor(max_workers=4) as executor:
        receipts = list(executor.map(create, range(4)))
    assert all(item == receipts[0] for item in receipts)
    with database.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Detector).where(Detector.aggregate_id == aggregate)) == 1
        assert session.scalar(select(func.count()).select_from(DetectorCommand).where(DetectorCommand.actor_id == actor)) == 1
    identity = receipts[0]["detector_id"]
    with ThreadPoolExecutor(max_workers=4) as executor:
        receipts = list(executor.map(lambda _: command(database, catalog, actor, "start", "same", identity), range(4)))
    assert all(item == receipts[0] for item in receipts)
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(lambda i: command(database, catalog, actor, "stop" if i % 2 else "start", str(i), identity), range(12)))
    with database.session_factory() as session:
        latest = session.scalar(select(DetectorCommand).where(DetectorCommand.detector_id == identity)
            .order_by(DetectorCommand.recorded_at.desc()).limit(1))
        assert session.get(Detector, identity).execution_generation == latest.execution_generation


def test_atomic_rollback_and_unknown_commit(database, catalog, aggregate):
    actor = uuid4()
    with database.session_factory() as session:
        api = build_detector_service(session, catalog)
        original = api.repository.receipt
        def fail(*args):
            original(*args)
            raise RuntimeError("synthetic before commit")
        api.repository.receipt = fail
        with pytest.raises(RuntimeError):
            api.command("create", actor, "before", payload=body(aggregate))
    with database.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Detector).where(Detector.aggregate_id == aggregate)) == 0
        assert session.scalar(select(func.count()).select_from(DetectorCommand).where(DetectorCommand.actor_id == actor)) == 0
    for committed in (False, True):
        key = str(committed)
        with database.session_factory() as session:
            api = build_detector_service(session, catalog)
            commit = api.uow.commit
            def uncertain(committed=committed, commit=commit):
                if committed:
                    commit()
                raise RuntimeError("lost acknowledgement")
            api.uow.commit = uncertain
            with pytest.raises(DomainError) as error:
                api.command("create", actor, key, payload=body(aggregate))
            assert error.value.details["outcome"] == "unknown"
        replay = command(database, catalog, actor, "create", key, payload=body(aggregate))
        assert command(database, catalog, actor, "create", key, payload=body(aggregate)) == replay
    with database.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Detector).where(Detector.aggregate_id == aggregate)) == 2


@pytest.fixture
def client(database, catalog):
    app = create_app(Settings(environment="test", database_url=os.environ["TEST_DATABASE_URL"]),
        database=database, ecs_catalog=catalog, start_consumers=False)
    with TestClient(app) as value:
        yield value


def login(database, client, role=ADMIN):
    name = "detector-" + uuid4().hex
    identity = uuid4()
    with database.session_factory.begin() as session:
        session.add(User(id=identity, username=name, password_hash=hash_password("synthetic-password"),
            display_name=name, role_id=role))
    assert client.post("/api/v1/auth/local/login", json={"username": name, "password": "synthetic-password"}).status_code == 200
    return identity, {"X-CSRF-Token": client.get("/api/v1/auth/session").json()["csrf_token"], "Idempotency-Key": uuid4().hex}


def test_real_http_auth_csrf_permissions_and_openapi(database, catalog, aggregate, client):
    url = "/api/v1/detectors"
    assert client.get(url).status_code == 401
    assert client.post(url, json=body(aggregate)).status_code == 401
    actor, headers = login(database, client)
    assert client.post(url, json=body(aggregate), headers={"Idempotency-Key": "no-csrf"}).status_code == 403
    assert client.post(url, json=body(aggregate), headers={**headers, "X-CSRF-Token": "wrong"}).status_code == 403
    response = client.post(url, json=body(aggregate), headers=headers)
    assert response.status_code == 201, response.text
    created = response.json()
    identity = created["detector_id"]
    assert client.post(url, json=body(aggregate), headers=headers).json() == created
    card = client.get(f"{url}/{identity}").json()
    assert card["execution_state"] == "created" and card["model_state"] == "waiting"
    assert card["backlog"] is None and card["lag_seconds"] is None and card["warmup"] is None
    assert card["feature_keys"] == ["n"] and card["config"]["dimensions"] == 1
    assert client.post(f"{url}/{identity}/start", headers=headers).status_code == 202
    assert client.post(f"{url}/{identity}/stop", headers=headers, json={}).status_code == 200
    assert client.post(f"{url}/{identity}/stop", headers=headers, json={"config": {}}).status_code == 422
    for path, method, status in [(url, "post", "201"), (url, "get", "200"),
            (url + "/{detector_id}", "get", "200"), (url + "/{detector_id}/start", "post", "202"),
            (url + "/{detector_id}/stop", "post", "200")]:
        assert client.app.openapi()["paths"][path][method]["responses"][status]["content"]["application/json"]["schema"]["$ref"]
    # Authorization is checked before replay, including on a previously authorized session.
    with database.session_factory.begin() as session:
        session.get(User, actor).role_id = GUEST
    assert client.post(url, json=body(aggregate), headers=headers).status_code == 403
    for path in (url, f"{url}/{identity}"):
        assert client.get(path).status_code == 403
    for path in (url, f"{url}/{identity}/start", f"{url}/{identity}/stop"):
        assert client.post(path, json={} if path != url else body(aggregate), headers=headers).status_code == 403
    assert client.post("/api/v1/auth/logout", headers=headers).status_code == 200
    login(database, client, GUEST)
    assert client.get(url).status_code == 403


@pytest.mark.parametrize("changes", [
    {"name": " "}, {"name": "n" * 201}, {"accelerated": 1}, {"step_seconds": True},
    {"step_seconds": 1}, {"accelerated": True}, {"accelerated": True, "step_seconds": 11},
    {"accelerated": True, "step_seconds": 1.5}, {"accelerated": True, "step_seconds": "1"},
    {"config": {"trees": True}}, {"config": {"sample_size": 1}}, {"config": {"trees": 0}},
    {"config": {"seed": 2**63}}, {"config": {"trees": 2**40}}, {"config": {"dimensions": 1}},
    {"owner": "x"}, {"dimensions": 1}, {"config": {"output_after": 5000}},
])
def test_strict_http_creation(database, aggregate, client, changes):
    _, headers = login(database, client)
    response = client.post("/api/v1/detectors", json=body(aggregate, **changes), headers=headers)
    assert response.status_code == 422, response.text


def test_http_filters_errors_keys_and_safe_envelope(database, aggregate, client):
    _, headers = login(database, client)
    url = "/api/v1/detectors"
    for key in ("", "   ", "x" * 201):
        assert client.post(url, json=body(aggregate), headers={**headers, "Idempotency-Key": key}).status_code == 422
    assert client.post(url, json=body(aggregate), headers={"X-CSRF-Token": headers["X-CSRF-Token"]}).status_code == 422
    identities = []
    for step in (None, 10, 5):
        response = client.post(url, headers={**headers, "Idempotency-Key": uuid4().hex},
            json=body(aggregate, **({"accelerated": True, "step_seconds": step} if step else {})))
        assert response.status_code == 201, response.text
        identities.append(response.json()["detector_id"])
    for query in ("limit=101", "offset=100001", "execution_state=bogus", "aggregate_id=bad"):
        assert client.get(f"{url}?{query}").status_code == 422
    page = client.get(url, params={"aggregate_id": str(aggregate), "execution_state": "created", "limit": 1}).json()
    assert page["total"] == 3 and len(page["items"]) == 1 and page["items"][0]["id"] == identities[-1]
    second = client.get(url, params={"aggregate_id": str(aggregate), "offset": 1, "limit": 1}).json()
    assert second["items"][0]["id"] == identities[-2]
    for response in (client.get(f"{url}/{uuid4()}"), client.post(f"{url}/{uuid4()}/start", headers=headers)):
        assert response.status_code == 404
        assert set(response.json()) == {"code", "message", "request_id", "details"}
        assert response.headers["X-Request-ID"] == response.json()["request_id"]


def test_failed_worker_attempt_and_replay_do_not_restart(database, catalog, aggregate, client, monkeypatch):
    _, headers = login(database, client)
    url = "/api/v1/detectors"
    identity = UUID(client.post(url, json=body(aggregate), headers=headers).json()["detector_id"])
    start = client.post(f"{url}/{identity}/start", headers=headers).json()
    # Genuine coordinator/control/history scenario. No JVM is needed to prove insufficient history.
    lock = DetectorSessionLock(os.environ["TEST_DATABASE_URL"])
    try:
        pid = lock.acquire()
        assert pid is not None
        with database.session_factory() as session:
            api = build_detector_service(session, catalog)
            api.owner = api.acquire_owner(uuid4(), pid)
            monkeypatch.setattr(api.models, "warmup", lambda *_: pytest.fail("HTTP/history started JVM"))
            api.begin_start(identity)
            assert client.get(f"{url}/{identity}").json()["execution_state"] == "starting"
            api.prepare_warmup(identity)
        card = client.get(f"{url}/{identity}").json()
        assert card["execution_state"] == "failed" and card["reason"]["code"] == "history_insufficient"
        assert client.post(f"{url}/{identity}/start", headers=headers).json() == start
        assert client.get(f"{url}/{identity}").json() == card
        with database.session_factory.begin() as session:
            row = session.get(Aggregate, aggregate)
            row.state = "deleted"
        assert client.post(url, json=body(aggregate), headers=headers).status_code == 201
        assert client.post(f"{url}/{identity}/start", headers={**headers, "Idempotency-Key": "new"}).status_code == 409
        assert client.get(f"{url}/{identity}").status_code == 200
    finally:
        lock.close()


def test_read_projection_excludes_vectors_and_preserves_revision(database, catalog, aggregate):
    identity = command(database, catalog, uuid4(), "create", "projection", payload=body(aggregate))["detector_id"]
    with database.session_factory.begin() as session:
        row = session.get(Detector, identity)
        row.checkpoint = {"secret": "never-public", "padding": "x" * 1000000}
        row.revision += 1
    statements = []
    def observe(connection, cursor, statement, parameters, context, many):
        statements.append(statement)
    event.listen(database.engine, "before_cursor_execute", observe)
    try:
        with database.session_factory() as session:
            api = build_detector_service(session, catalog)
            card = api.public_card(identity)
            assert api.public_card(identity) == card
            assert api.public_list(10, 0, aggregate).items[0].id == identity
    finally:
        event.remove(database.engine, "before_cursor_execute", observe)
    assert "never-public" not in card.model_dump_json()
    assert all("checkpoint" not in sql and "features" not in sql for sql in statements)
    assert card.revision == 1


def test_command_migration_preserves_old_data_and_roles(database):
    url = os.environ["TEST_DATABASE_URL"]
    env = {**os.environ, "DATABASE_URL": url}
    env.pop("DATABASE_URL_FILE", None)
    with database.session_factory() as session:
        before = session.execute(text("SELECT id,checkpoint,execution_state,execution_generation FROM logs.detectors ORDER BY id")).all()
        roles = {row.id: list(row.permissions) for row in session.scalars(select(Role))}
    for args in (["downgrade", "0019_detector_worker"], ["upgrade", "head"], ["check"]):
        subprocess.run([sys.executable, "-m", "alembic", *args], env=env, check=True)
    with database.session_factory() as session:
        assert session.execute(text("SELECT id,checkpoint,execution_state,execution_generation FROM logs.detectors ORDER BY id")).all() == before
        for row in session.scalars(select(Role)):
            assert set(row.permissions) == set(roles[row.id])
        assert {"detectors.read", "detectors.write"} <= set(session.get(Role, ADMIN).permissions)
        assert not {"detectors.read", "detectors.write"} & set(session.get(Role, GUEST).permissions)


def test_diagnostics_warmup_windows_recovery_and_timezone(database, catalog, aggregate):
    identity = command(database, catalog, uuid4(), "create", "diagnostics", payload=body(
        aggregate, accelerated=True, step_seconds=1))["detector_id"]
    with database.session_factory.begin() as session:
        d = session.get(Detector, identity)
        a = session.get(Aggregate, aggregate)
        s = session.get(AggregateSchedule, a.schedule_id)
        anchor, schedule_id = s.anchor_utc, s.id
        at = anchor + timedelta(seconds=300)
        inputs = [{"start": (anchor + timedelta(seconds=i)).isoformat(),
            "end": (anchor + timedelta(seconds=i + 10)).isoformat(),
            "completeness": "complete", "ml_input": ["private-vector" * 10000], "exact": [123]} for i in range(8)]
        attempt_id = uuid4()
        session.add(DetectorWarmup(id=attempt_id, detector_id=identity, cutoff=at,
            plan={"count": 8}, inputs=inputs, inputs_hash="f" * 64,
            status="prepared", reason=None, created_at=at))
        session.flush()
        d.active_attempt_id, d.state, d.execution_state = attempt_id, "warming", "starting"
        d.revision += 1
        d.last_failure = {"code": "resource_limit", "at": at.isoformat(), "start_request_id": None,
            "details": {"factor": "model_seconds", "secret": "never-public"}}
    with database.session_factory() as session:
        api = build_detector_service(session, catalog)
        warming = api.public_card(identity)
        assert warming.warmup.materialized_windows == warming.warmup.eligible_windows == 8
        assert warming.warmup.materialized_coverage_seconds == warming.warmup.eligible_coverage_seconds == 17
        assert warming.warmup.readiness == "warming" and warming.warmup.required_windows == 512
        assert not warming.accelerated_used and warming.backlog is None
    with database.session_factory.begin() as session:
        d = session.get(Detector, identity)
        d.state, d.execution_state, d.reason = "ready", "running", None
        d.evaluation_start_index, d.consumed_index, d.evaluated_index = 0, 2, 1
        d.accelerated_used = True
        d.revision += 1
        for index, kind in [(1, "evaluated"), (2, "skipped")]:
            left = anchor + timedelta(seconds=index * 10)
            right = left + timedelta(seconds=10)
            point_id = uuid4()
            session.add(WorkingWindowPoint(id=point_id, schedule_id=schedule_id, window_index=index,
                start_utc=left, end_utc=right, scheduled_at=right, actual_started_at=right,
                actual_completed_at=right + timedelta(seconds=1), event_count=0, completeness="unknown",
                completeness_reason="unknown", evidence_refs={}, features=[{"private-vector": 1}]))
            session.flush()
            session.add(DetectorOutcome(id=uuid4(), detector_id=identity, working_point_id=point_id,
                window_index=index, revision=2, kind=kind, reason="coverage:unknown" if kind == "skipped" else None,
                data={"actual_started_at": at.isoformat(), "actual_completed_at": at.isoformat(),
                    "late": True, "result": {"secret": "never-public"}}))
        for index in range(30):
            session.add(DetectorRecovery(id=uuid4(), detector_id=identity, first_index=index,
                last_index=index, started_at=at + timedelta(seconds=index), coordinator_generation=0,
                reason="manual_resume" if index == 29 else "crash_recovery"))
        a = session.get(Aggregate, aggregate)
        a.skipped_ranges = [{"reason": "intentional_archive", "start_utc": (anchor + timedelta(seconds=i * 10)).isoformat(),
            "end_utc": (anchor + timedelta(seconds=j * 10)).isoformat()} for i, j in [(3, 5), (5, 7)]]
    cards = []
    for zone in ("UTC", "Asia/Yekaterinburg", "America/New_York"):
        with database.session_factory() as session:
            session.execute(select(func.set_config("TimeZone", zone, False)))
            session.commit()
            api = build_detector_service(session, catalog)
            read = api.repository.public_context
            def fixed(*args, read=read):
                context = read(*args)
                context["now"] = at
                return context
            api.repository.public_context = fixed
            cards.append(api.public_card(identity))
    assert cards[0] == cards[1] == cards[2]
    card = cards[0]
    assert card.last_consumed.index == 2 and card.last_consumed.kind == "skipped"
    assert card.last_consumed.evaluation_completed_at is None
    assert card.last_evaluated.index == 1 and card.last_evaluated.evaluation_completed_at is not None
    assert card.next_scheduled_at == anchor + timedelta(seconds=80)
    assert card.backlog == 23 and card.lag_seconds == 220
    assert card.recovery.first_index == 29 and card.recovery.reason == "manual_resume"
    assert card.reason is None or card.reason.code == "working_point_pending"
    assert card.last_failure.reason.factor == "model_seconds"
    assert "private-vector" not in card.model_dump_json() and "never-public" not in card.model_dump_json()


def test_list_all_states_and_archived_deleted_history(database, catalog, aggregate):
    states = ["created", "requested", "starting", "running", "stopped", "failed", "terminated"]
    for state in states:
        identity = command(database, catalog, uuid4(), "create", state, payload=body(aggregate))["detector_id"]
        with database.session_factory.begin() as session:
            d = session.get(Detector, identity)
            d.execution_state = state
            d.revision += 1
    with database.session_factory() as session:
        api = build_detector_service(session, catalog)
        page = api.public_list(100, 0, aggregate)
        assert page.total == 7 and {item.execution_state for item in page.items} == set(states)
        for state in states:
            assert api.public_list(100, 0, aggregate, state).total == 1
        identity = page.items[0].id
    for state in ("archived", "deleted"):
        with database.session_factory() as session:
            build_aggregate_service(session, catalog).lifecycle(aggregate, "archive" if state == "archived" else "delete", uuid4())
        with database.session_factory() as session:
            assert build_detector_service(session, catalog).public_card(identity).aggregate_state == state
        with pytest.raises(DomainError) as error:
            command(database, catalog, uuid4(), "start", "inactive", identity)
        assert error.value.status_code == 409


def test_delete_competes_with_command_and_replay(database, catalog, aggregate):
    actor = uuid4()
    created = command(database, catalog, actor, "create", "create", payload=body(aggregate))
    identity = created["detector_id"]
    def remove():
        with database.session_factory() as session:
            build_aggregate_service(session, catalog).lifecycle(aggregate, "delete", actor)
    def start():
        try:
            return command(database, catalog, actor, "start", "start", identity)
        except DomainError as error:
            assert error.status_code == 409
            return None
    with ThreadPoolExecutor(max_workers=2) as executor:
        deletion, starting = executor.submit(remove), executor.submit(start)
        deletion.result(timeout=10)
        receipt = starting.result(timeout=10)
    with database.session_factory() as session:
        d = session.get(Detector, identity)
        assert d.execution_state == "terminated"
        generation = d.execution_generation
    assert command(database, catalog, actor, "create", "create", payload=body(aggregate)) == created
    if receipt:
        assert command(database, catalog, actor, "start", "start", identity) == receipt
    command(database, catalog, actor, "stop", "after-delete", identity)
    with database.session_factory() as session:
        assert session.get(Detector, identity).execution_generation == generation


def test_http_does_not_invoke_model_and_expired_session_is_unauthorized(database, aggregate, client, monkeypatch):
    from app.detection import model_process
    def forbidden(*args, **kwargs):
        pytest.fail("HTTP attempted a model subprocess")
    monkeypatch.setattr(model_process, "invoke", forbidden)
    monkeypatch.setattr(model_process.BoundedModelFactory, "restore", forbidden)
    monkeypatch.setattr(model_process.BoundedModelFactory, "warmup", forbidden)
    actor, headers = login(database, client)
    url = "/api/v1/detectors"
    response = client.post(url, json=body(aggregate), headers=headers)
    assert response.status_code == 201
    identity = response.json()["detector_id"]
    assert client.post(f"{url}/{identity}/start", headers=headers).status_code == 202
    assert client.get(f"{url}/{identity}").json()["execution_state"] == "requested"
    assert client.get(url).status_code == 200
    assert client.post(f"{url}/{identity}/stop", headers=headers).status_code == 200
    with database.session_factory.begin() as session:
        for row in session.scalars(select(AuthSession).where(AuthSession.user_id == actor)):
            past = datetime.now(UTC) - timedelta(hours=1)
            row.created_at = row.last_seen_at = past
            row.expires_at = past + timedelta(seconds=1)
    assert client.get(url).status_code == 401
    assert client.post(f"{url}/{identity}/start", headers=headers).status_code == 401


def test_sql_unique_scope_and_different_payload_race(database, catalog, aggregate):
    from sqlalchemy.exc import IntegrityError
    actor = uuid4()
    def create(name):
        try:
            return command(database, catalog, actor, "create", "conflicting", payload=body(aggregate, name=name))
        except DomainError as error:
            assert error.code == "idempotency_conflict"
            return None
    with ThreadPoolExecutor(max_workers=2) as executor:
        receipts = list(executor.map(create, ["A", "B"]))
    assert sum(item is not None for item in receipts) == 1
    with database.session_factory() as session:
        receipt = session.scalar(select(DetectorCommand).where(DetectorCommand.actor_id == actor))
        session.add(DetectorCommand(id=uuid4(), actor_id=actor, action="create", key="conflicting",
            fingerprint="a" * 64, detector_id=receipt.detector_id, recorded_at=datetime.now(UTC),
            execution_generation=0))
        with pytest.raises(IntegrityError) as error:
            session.flush()
        assert isinstance(error.value.orig, UniqueViolation)
        session.rollback()


def test_http_unknown_acknowledgement_resolves_same_key(database, catalog, aggregate, client):
    actor, headers = login(database, client)
    def uncertain_service(session=Depends(get_session)):
        api = build_detector_service(session, catalog)
        commit = api.uow.commit
        def uncertain():
            commit()
            raise RuntimeError("synthetic lost acknowledgement; never-public")
        api.uow.commit = uncertain
        return api
    client.app.dependency_overrides[get_detector_service] = uncertain_service
    response = client.post("/api/v1/detectors", json=body(aggregate), headers=headers)
    assert response.status_code == 503
    assert response.json()["details"]["outcome"] == "unknown"
    assert "never-public" not in response.text
    client.app.dependency_overrides.clear()
    receipt = client.post("/api/v1/detectors", json=body(aggregate), headers=headers)
    assert receipt.status_code == 201
    with database.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(DetectorCommand).where(DetectorCommand.actor_id == actor)) == 1
        assert session.scalar(select(func.count()).select_from(Detector).where(Detector.aggregate_id == aggregate)) == 1


def test_command_key_timeout_is_safe_and_rolls_back(database, catalog, aggregate):
    actor = uuid4()
    with database.session_factory() as holder:
        build_detector_service(holder, catalog).repository.command(actor, "create", "locked")
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(command, database, catalog, actor, "create", "locked", None, body(aggregate))
            with pytest.raises(DomainError) as error:
                future.result(timeout=10)
            assert error.value.code == "detector_unavailable" and error.value.status_code == 503
            assert "SQL" not in error.value.message
        holder.rollback()
    with database.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Detector).where(Detector.aggregate_id == aggregate)) == 0
        assert session.scalar(select(func.count()).select_from(DetectorCommand).where(DetectorCommand.actor_id == actor)) == 0
    assert command(database, catalog, actor, "create", "locked", payload=body(aggregate))["detector_id"]


def test_empty_warmup_attempt_does_not_invent_progress(database, catalog, aggregate):
    identity = command(database, catalog, uuid4(), "create", "empty", payload=body(aggregate))["detector_id"]
    with database.session_factory.begin() as session:
        attempt_id = uuid4()
        at = datetime.now(UTC)
        session.add(DetectorWarmup(id=attempt_id, detector_id=identity, cutoff=at, plan={"count": 512},
            inputs=[], inputs_hash="f" * 64, status="preparing", reason=None, created_at=at))
        session.flush()
        d = session.get(Detector, identity)
        d.active_attempt_id, d.execution_state, d.state = attempt_id, "starting", "warming"
        d.revision += 1
    with database.session_factory() as session:
        card = build_detector_service(session, catalog).public_card(identity)
    assert card.warmup is None and card.backlog is None and card.lag_seconds is None
