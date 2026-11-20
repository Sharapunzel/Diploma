"""Permanent DB boundary proofs for maintenance admission, fences and identity."""

import importlib.util
import os
import platform
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
import sqlalchemy
from checkpoint_fixtures import checkpoint_body
from fastapi.testclient import TestClient
from sqlalchemy import JSON, Text, delete, event, func, null, select, text, update
from sqlalchemy.exc import DBAPIError
from test_aggregate_retention import history, maintain, read_state, settle
from test_aggregate_retention_races import storage_attempt
from test_event_time_pipeline import context, migrated

from app.aggregate_composition import build_aggregate_service
from app.aggregation.event_time import EventTimeBudgetError, OperationBudget
from app.aggregation.retention import protection_plan
from app.config import Settings
from app.core.security import hash_password
from app.detection.contracts import DetectionError, ModelConfig, digest
from app.detection.numeric import Affine
from app.detector_composition import build_detector_service
from app.main import create_app
from app.models import (
    AggregateClosure,
    AggregateRetentionState,
    AggregateSchedule,
    AggregateSnapshot,
    AuthSession,
    Detector,
    DetectorWarmup,
    Source,
    User,
    WorkingWindowPoint,
)
from app.repositories.sqlalchemy import aggregate_cleanup as cleanup_queries
from app.repositories.sqlalchemy import aggregate_retention as retention_queries
from app.repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository

__all__ = ["context", "migrated"]


@pytest.mark.parametrize("milliseconds,lock_ms", [(987, 50), (25, 25)], ids=["987-50", "25-25"])
def test_maintenance_budget_sets_both_local_timeouts_before_pending_orm_flush(
        context, capsys, milliseconds, lock_ms):
    database, catalog, _, connection_id = context
    observed, calls = [], []
    pending_id = uuid4()

    def current_settings(session):
        # Connection-level SHOW cannot recursively trigger Session autoflush.
        connection = session.connection()
        return tuple(connection.exec_driver_sql("SHOW " + setting).scalar_one()
            for setting in ("statement_timeout", "lock_timeout"))

    def before_flush(session, flush_context, instances):
        observed.append(current_settings(session))

    def count_settings(connection, cursor, statement, parameters, execution, many):
        if "set_config(" in statement:
            calls.append(statement.count("set_config("))

    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        original = current_settings(session)
        connection = session.connection()
        connection.exec_driver_sql("SET LOCAL statement_timeout = '1733ms'")
        connection.exec_driver_sql("SET LOCAL lock_timeout = '1777ms'")
        assert current_settings(session) == ("1733ms", "1777ms")
        pending = Source(id=pending_id, connection_id=connection_id, name=str(pending_id),
            topic_name=str(pending_id), is_enabled=False)
        session.add(pending)
        event.listen(session, "before_flush", before_flush)
        event.listen(database.engine, "before_cursor_execute", count_settings)
        try:
            # A fixed clock isolates GUC ordering from elapsed wall time. The
            # real PostgreSQL INSERT below still executes under both actual
            # LOCAL values, including the 25ms statement/lock case.
            budget = OperationBudget(milliseconds / 1000, clock=lambda: 0)
            api.repository.maintenance_budget(budget)
            after_setting = current_settings(session)
            with capsys.disabled():
                print("Maintenance-budget pending ORM order:", {
                    "system": platform.system(), "python": platform.python_version(),
                    "sqlalchemy": sqlalchemy.__version__, "sqlalchemy_file": sqlalchemy.__file__,
                    "old_local": ("1733ms", "1777ms"), "requested_ms": milliseconds,
                    "lock_ms": lock_ms, "before_flush": observed,
                    "after_setting": after_setting, "still_pending": pending in session.new,
                    "setting_statement_calls": calls}, flush=True)
            # Keep this first: RED must name premature pending ORM flush,
            # rather than a secondary setting format/statement-count mismatch.
            assert observed == [], "maintenance_budget must set both GUCs before pending ORM writes"
            assert pending in session.new
            assert calls == [2]
            assert after_setting == (f"{milliseconds}ms", f"{lock_ms}ms")
            session.flush()
            assert observed == [(f"{milliseconds}ms", f"{lock_ms}ms")]
            assert session.connection().exec_driver_sql(
                "SELECT count(*) FROM app.sources WHERE id = %s", (pending_id,)).scalar_one() == 1
        finally:
            event.remove(database.engine, "before_cursor_execute", count_settings)
            event.remove(session, "before_flush", before_flush)
            session.rollback()
            assert current_settings(session) == original
            assert session.connection().exec_driver_sql(
                "SELECT count(*) FROM app.sources WHERE id = %s", (pending_id,)).scalar_one() == 0
            session.rollback()


@pytest.fixture
def http_lookup_client(context):
    db, catalog, _, _ = context
    username, actor = uuid4().hex, uuid4()
    with db.session_factory.begin() as session:
        session.add(User(id=actor, username=username, display_name=username,
            password_hash=hash_password("lookup-test-password"),
            role_id=UUID("00000000-0000-4000-8000-000000000001")))
    app = create_app(Settings(environment="test", database_url=os.environ["TEST_DATABASE_URL"]),
        database=db, ecs_catalog=catalog, start_consumers=False)
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.post("/api/v1/auth/local/login", json={"username": username, "password": "lookup-test-password"})
            assert response.status_code == 200, response.text
            yield client
    finally:
        with db.session_factory.begin() as session:
            session.execute(delete(AuthSession).where(AuthSession.user_id == actor))
            session.execute(delete(User).where(User.id == actor))


def assert_http_cleared(response, point_id):
    assert response.status_code == 410, response.text
    body = response.json()
    assert body["code"] == "aggregate_point_cleared"
    assert UUID(body["request_id"]) == UUID(response.headers["X-Request-ID"])
    assert body["details"]["origin_point_id"] == str(point_id)
    assert body["details"]["window_index"] == 0
    assert body["details"]["time_basis"] == "event_time"
    assert body["details"]["origin"] == "initial_history"
    assert body["details"]["clear_reason"] == "automatic"
    assert body["details"]["clear_policy_version"] == "model_history_v1"
    assert datetime.fromisoformat(body["details"]["cleared_at"]).utcoffset() == timedelta(0)


def test_http_automatic_clear_returns_410_with_metadata_and_keeps_other_lookup_statuses(context, http_lookup_client):
    client = http_lookup_client
    card, ids = history(context)
    base = f"/api/v1/aggregates/{card.id}/points"
    old = client.get(f"{base}/{ids[0]}")
    assert old.status_code == 200, old.text
    assert old.json()["payload_state"] == "present" and old.json()["completeness"] == "unknown"
    state = settle(context, card.id, 8)
    assert state["reserve_windows"] == 512 and state["history_revision"] == state["auto_cleared_total"] == 8
    for path in (f"{base}/{ids[0]}", f"{base}/by-index/0"):
        assert_http_cleared(client.get(path), ids[0])
    remaining = client.get(base).json()
    assert remaining["total"] == 512
    not_ready = client.get(f"{base}/by-index/520")
    assert not_ready.status_code == 409 and not_ready.json()["code"] == "canonical_point_not_ready"
    unknown = client.get(f"{base}/{uuid4()}")
    assert unknown.status_code == 404 and unknown.json()["code"] == "aggregate_point_not_found"
    other, other_ids = history(context, count=1)
    assert other.id != card.id
    foreign = client.get(f"{base}/{other_ids[0]}")
    assert foreign.status_code == 404 and foreign.json()["code"] == "aggregate_point_not_found"


@pytest.mark.parametrize("lookup", ["id", "index"])
def test_http_lookup_during_automatic_clear_never_combines_present_closure_with_missing_payload(context, http_lookup_client, monkeypatch, lookup):
    db, _, _, _ = context
    client = http_lookup_client
    card, ids = history(context)
    base = f"/api/v1/aggregates/{card.id}/points"
    path = f"{base}/{ids[0]}" if lookup == "id" else f"{base}/by-index/0"
    before = client.get(path)
    assert before.status_code == 200, before.text
    crossed = []
    original = SqlAlchemyAggregateRepository.payload

    def gc_between_closure_and_payload(repository, closure):
        if closure.origin_point_id == ids[0] and not crossed:
            assert closure.payload_state == "present"
            crossed.append(closure.origin_point_id)
            # This synchronous barrier runs real GC in another DB session after
            # HTTP read the closure and before it reads payload or metadata.
            result = maintain(context, card.id)
            assert result["status"] != "error", result
            with db.session_factory() as observer:
                assert observer.get(AggregateClosure, ids[0]).payload_state == "cleared"
                assert observer.get(WorkingWindowPoint, ids[0]) is None
                assert observer.get(AggregateSnapshot, ids[0]) is None
                state = observer.get(AggregateRetentionState, card.id)
                assert state.reserve_windows == 512 and state.history_revision == state.auto_cleared_total == 1
        return original(repository, closure)

    monkeypatch.setattr(SqlAlchemyAggregateRepository, "payload", gc_between_closure_and_payload)
    raced = client.get(path)
    assert crossed == [ids[0]]
    assert raced.status_code in {200, 410}, raced.text
    if raced.status_code == 200:
        assert raced.json() == before.json()
    else:
        assert_http_cleared(raced, ids[0])
    assert_http_cleared(client.get(f"{base}/{ids[0]}"), ids[0])
    assert_http_cleared(client.get(f"{base}/by-index/0"), ids[0])


def test_browser_auto_facts_preserves_outer_rr_and_timeout_during_automatic_clear(context, http_lookup_client, monkeypatch):
    db, _, _, _ = context
    card, ids = history(context, count=513)
    base = f"/api/v1/aggregates/{card.id}/points"
    before = http_lookup_client.get(f"{base}/{ids[0]}")
    assert before.status_code == 200, before.text
    assert before.json()["completeness"] == "unknown"  # Storage consistency, not live quality proof.
    spec = importlib.util.spec_from_file_location("event_time_runtime_retention_test",
        Path(__file__).parent / "ml" / "event_time_runtime.py")
    assert spec is not None and spec.loader is not None
    runtime = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runtime)
    crossed, settings = [], []
    original_get = SqlAlchemyAggregateRepository.get

    def gc_after_facts_before_aggregate(repository, identity):
        if identity == card.id and not crossed:
            crossed.append(identity)
            # SHOW utilities inspect the already-owned transaction without a
            # SELECT that could pin a replacement RR snapshot and mask a reset.
            settings.append(tuple(repository.session.scalar(text("SHOW " + key)) for key in (
                "transaction_isolation", "transaction_read_only", "statement_timeout")))
            result = maintain(context, card.id)
            assert result["status"] != "error", result
            with db.session_factory() as observer:
                assert observer.get(AggregateClosure, ids[0]).payload_state == "cleared"
                assert observer.get(WorkingWindowPoint, ids[0]) is None
                assert observer.get(AggregateSnapshot, ids[0]) is None
                state = observer.get(AggregateRetentionState, card.id)
                assert state.reserve_windows == 512 and state.history_revision == state.auto_cleared_total == 1
        return original_get(repository, identity)

    monkeypatch.setattr(SqlAlchemyAggregateRepository, "get", gc_after_facts_before_aggregate)
    facts = runtime.browser_auto_facts(db, {"aggregate": str(card.id)})
    assert crossed == [card.id]
    assert settings == [("repeatable read", "on", "3s")]
    assert facts["ready"] and facts["old_point_id"] == str(ids[0])
    assert facts["canonical_history_count"] == 513
    assert facts["payload_state"] == "present" and facts["old_payloads"] == facts["old_snapshots"] == 1
    assert facts["old_point"] == before.json()
    assert runtime.browser_auto_facts(db, {"aggregate": str(card.id), "readiness_only": True}) == {
        "canonical_history_count": 513}
    assert_http_cleared(http_lookup_client.get(f"{base}/{ids[0]}"), ids[0])
    assert_http_cleared(http_lookup_client.get(f"{base}/by-index/0"), ids[0])


def claim_only(context, card, monkeypatch):
    db, catalog, _, _ = context
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        commit, first = api.uow.commit, [True]

        def lost_ack():
            commit()
            if first[0]:
                first[0] = False
                raise DBAPIError(None, None, RuntimeError("lost acknowledgement"))

        monkeypatch.setattr(api.uow, "commit", lost_ack)
        assert api.maintain(card.id)["pending"]


def test_transport_preflight_fails_closed_before_any_dependency_headers(context, monkeypatch):
    db, catalog, _, _ = context
    card, _ = history(context)
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)

        def refuse(_identity, _budget):
            assert session.scalar(text("SHOW transaction_read_only")) == "on"
            raise EventTimeBudgetError("protection_bytes")

        def forbidden(*_args, **_kwargs):
            pytest.fail("dependency headers loaded before transport preflight")

        monkeypatch.setattr(api.repository, "retention_dependencies", refuse)
        monkeypatch.setattr(api.repository, "retention_headers", forbidden)
        result = api.maintain(card.id)
        assert result["status"] == "protected" and result["reason"] == "protection_unavailable"
        state = session.get(AggregateRetentionState, card.id)
        assert state.history_revision == state.auto_cleared_total == 0
        assert state.reserve_windows is state.protected_total is None


def transport_models(context, kinds, *, count=16, retained_bytes=128):
    """Persist parser-contract dependencies, without claiming Java forest restore.

    Source closures remain explicitly unknown-quality storage fixtures. The
    prepared batch and checkpoint exercise the neutral protection parser only;
    genuine numerical restore is covered by the existing native ML regressions.
    """
    db, catalog, _, _ = context
    card, points = history(context, count=count)
    models, saved = {}, {}
    with db.session_factory() as session:
        for kind in kinds:
            identity = build_detector_service(session, catalog).create(card.id, UUID(int=1), kind,
                config={"trees": 20, "sample_size": 8, "output_after": 8})
            model = session.get(Detector, identity)
            config = ModelConfig(**model.config)
            attempt_id = uuid4()
            cutoff = card.anchor_utc + timedelta(seconds=160)
            plan = {"first_start": card.anchor_utc.isoformat(), "cutoff": cutoff.isoformat(),
                "count": 16, "step": 10, "duration": 10,
                "evaluation_start_index": max(card.online_start_index, 16)}
            status = ("prepared" if kind in {"prepared", "corrupt", "corrupt_prepared"}
                else "preparing" if kind in {"frozen", "sql_null", "json_null"} else "ready")
            inputs = []
            if status == "prepared":
                rows = [[i + 1, 2 * (i + 1)] for i in range(16)]
                transform = Affine.fit(rows)
                for index, values in enumerate(rows):
                    start = card.anchor_utc + timedelta(seconds=index * 10)
                    inputs.append({"basis": "event_time", "start": start.isoformat(),
                        "end": (start + timedelta(seconds=10)).isoformat(), "completeness": "complete",
                        "features": [{"key": key, "value": value, "reason": None, "invalid_count": 0}
                            for key, value in zip(model.feature_keys, values, strict=True)],
                        "exact": values, "ml_input": list(transform.convert(values))})
                inputs[0]["affine"] = {"algorithm": "min_range_decimal_float32_v1",
                    "center": [str(value) for value in transform.center],
                    "scale": [str(value) for value in transform.scale]}
            elif status == "ready":
                # Real saved transport bytes; readiness consumes the checkpoint,
                # and must not materialize this historical input document.
                inputs = [{"saved_input": index, "retained": "x" * retained_bytes}
                    for index in range(64)]
            row = DetectorWarmup(id=attempt_id, detector_id=identity, cutoff=cutoff, plan=plan,
                inputs=inputs,
                inputs_hash="0" * 64 if kind == "corrupt_prepared" else digest(inputs),
                status=status, reason=None, created_at=card.created_at)
            session.add(row)
            session.flush()
            model.active_attempt_id, model.revision = attempt_id, model.revision + 1
            if kind in {"checkpoint", "corrupt"}:
                body = checkpoint_body({"config": model.config, "feature_keys": model.feature_keys,
                    "rule_hash": model.rule_hash}, config)
                model.checkpoint = {"body": body, "sha256": digest(body)} if kind == "checkpoint" else {}
                model.state, model.evaluation_start_index, model.consumed_index = "ready", 0, 15
            elif kind in {"sql_null", "json_null"}:
                model.checkpoint = null() if kind == "sql_null" else JSON.NULL
                model.state = "waiting"
            else:
                model.state = "warming"
            models[kind], saved[kind] = identity, {column.key: deepcopy(getattr(row, column.key))
                for column in DetectorWarmup.__table__.columns}
        session.commit()
    return card, points, models, saved


def dependency_transport(session, repository, aggregate_id, monkeypatch):
    """Observe real ORM hydration and the actual document deepcopy separately."""
    loaded, copied, statements = [], [], []
    original = retention_queries.document

    def hydrated(_session, row):
        if isinstance(row, DetectorWarmup):
            loaded.append(row.id)

    def copy_document(row):
        copied.append(row.id)
        return original(row)

    def sql(_connection, _cursor, statement, _parameters, _execution, _many):
        statements.append(statement)

    event.listen(session, "loaded_as_persistent", hydrated)
    event.listen(session.connection(), "before_cursor_execute", sql)
    monkeypatch.setattr(retention_queries, "document", copy_document)
    try:
        detectors, attempts = repository.retention_dependencies(aggregate_id, OperationBudget(1))
    finally:
        event.remove(session, "loaded_as_persistent", hydrated)
        event.remove(session.connection(), "before_cursor_execute", sql)
    return detectors, attempts, {"hydrated": set(loaded), "copied": set(copied)}, statements


@pytest.mark.parametrize("kind", ["checkpoint", "corrupt"])
def test_present_checkpoint_avoids_full_ready_attempt_transport(context, monkeypatch, kind):
    db, _, _, _ = context
    card, _, models, saved = transport_models(context, [kind])
    attempt_id = saved[kind]["id"]
    with db.session_factory() as session:
        repo = SqlAlchemyAggregateRepository(session)
        with session.begin():
            session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            detectors, attempts, transport, statements = dependency_transport(session, repo, card.id, monkeypatch)
            assert detectors[0]["id"] == models[kind] and detectors[0]["checkpoint"] is not None
            sizing = [sql for sql in statements if "sum(" in sql and "detector_warmups" in sql]
            assert len(sizing) == 1
            assert "CASE WHEN" in sizing[0] and "checkpoint IS NULL" in sizing[0]
            assert "checkpoint =" in sizing[0] and "JSONB" in sizing[0]
            # Only the guarded aggregate preflight remains. There must not be a
            # second full warmup serialization selected by this unused ID.
            assert sum("to_jsonb(" in sql and "detector_warmups" in sql for sql in statements) == 1
            assert not any("FROM logs.detector_warmups" in sql for sql in statements)
            headers = repo.retention_headers(card.id, OperationBudget(1))
            assert headers[1][0]["id"] == attempt_id
            assert (transport, set(attempts)) == ({"hydrated": set(), "copied": set()}, set()), (
                "present checkpoint must not hydrate/deepcopy unused saved inputs", transport, set(attempts))


def test_mixed_checkpoint_and_null_dependencies_materialize_only_required_attempts(context, monkeypatch):
    db, _, _, _ = context
    card, _, models, saved = transport_models(context, ["checkpoint", "prepared", "frozen"])
    required = {saved[kind]["id"] for kind in ("prepared", "frozen")}
    with db.session_factory() as session:
        repo = SqlAlchemyAggregateRepository(session)
        detectors, attempts, transport, statements = dependency_transport(session, repo, card.id, monkeypatch)
        # Content and frozen plan must be byte-for-byte equivalent to persisted
        # dependencies, regardless of the omitted ready-attempt document.
        assert {key: attempts[key] for key in required} == {
            saved[kind]["id"]: saved[kind] for kind in ("prepared", "frozen")}
        result = protection_plan({"anchor_utc": card.anchor_utc, "duration_seconds": 10,
            "online_start_index": card.online_start_index}, detectors, attempts, 15, 15, lambda: None)
        reasons = {row["detector_id"]: row["reason"] for row in result["protected_by"]}
        assert reasons == {str(models[kind]): reason for kind, reason in (
            ("checkpoint", "unconsumed_points"), ("prepared", "prepared_warmup_continuation"), ("frozen", "frozen_warmup"))}
        assert sum("to_jsonb(" in sql and "detector_warmups" in sql for sql in statements) == 1
        headers = repo.retention_headers(card.id, OperationBudget(1))
        assert {row["id"] for row in headers[1]} == {row["id"] for row in saved.values()}
        assert (transport, set(attempts)) == ({"hydrated": required, "copied": required}, required), (
            "only NULL checkpoint dependencies need full attempts", transport, set(attempts))


@pytest.mark.parametrize("kind", ["sql_null", "json_null"])
def test_null_checkpoint_transport_keeps_frozen_attempt_and_rollback(context, monkeypatch, kind):
    db, _, _, _ = context
    card, _, models, saved = transport_models(context, [kind])
    identity, attempt_id = models[kind], saved[kind]["id"]
    with db.session_factory() as session:
        sql_null = session.scalar(select(Detector.checkpoint.is_(None)).where(Detector.id == identity))
        assert sql_null is (kind == "sql_null")
        detectors, attempts, transport, _ = dependency_transport(
            session, SqlAlchemyAggregateRepository(session), card.id, monkeypatch)
        assert detectors[0]["checkpoint"] is None
        assert transport == {"hydrated": {attempt_id}, "copied": {attempt_id}}
        assert attempts == {attempt_id: saved[kind]}
        result = protection_plan({"anchor_utc": card.anchor_utc, "duration_seconds": 10,
            "online_start_index": card.online_start_index}, detectors, attempts, 15, 15, lambda: None)
        assert result["protected_from_index"] == 0 and result["protected_by"][0]["reason"] == "frozen_warmup"
        session.rollback()
        assert session.get(DetectorWarmup, attempt_id).plan == saved[kind]["plan"]


def test_corrupt_present_checkpoint_does_not_fallback_to_valid_prepared_inputs(context):
    db, _, _, _ = context
    card, points, _, _ = transport_models(context, ["corrupt"], count=520)
    result = maintain(context, card.id)
    assert result["status"] == "protected" and result["reason"] == "protection_unavailable"
    state = read_state(context, card.id)
    assert state["history_revision"] == state["auto_cleared_total"] == 0
    assert state["protected_total"] is state["keep_from_index"] is None
    with db.session_factory() as session:
        assert session.get(AggregateClosure, points[0]).payload_state == "present"
        assert session.get(WorkingWindowPoint, points[0]) is not None


def test_checkpoint_attempt_headers_still_fence_revision_change_after_rr(context, monkeypatch):
    db, catalog, _, _ = context
    card, points, models, saved = transport_models(context, ["checkpoint"], count=520)
    identity, attempt_id = models["checkpoint"], saved["checkpoint"]["id"]
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        original, crossed = api.repository.retention_headers, []

        def change_after_rr(aggregate_id, budget, lock=False):
            headers = original(aggregate_id, budget, lock)
            assert headers[1][0]["id"] == attempt_id
            if not lock:
                assert session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
                with db.session_factory.begin() as writer:
                    writer.execute(update(Detector).where(Detector.id == identity).values(
                        revision=Detector.revision + 1))
                crossed.append(identity)
            return headers

        monkeypatch.setattr(api.repository, "retention_headers", change_after_rr)
        result = api.maintain(card.id)
        assert crossed == [identity]
        assert result["status"] == "deferred" and result["reason"] == "dependencies_changed"
    state = read_state(context, card.id)
    assert state["history_revision"] == state["auto_cleared_total"] == 0
    with db.session_factory() as session:
        assert session.get(AggregateClosure, points[0]).payload_state == "present"
        assert session.get(DetectorWarmup, attempt_id).inputs == saved["checkpoint"]["inputs"]


@pytest.mark.parametrize("field", ["cleared_at", "clear_reason", "clear_policy_version"])
def test_cleared_provenance_cannot_be_rewritten_even_with_fresh_authority(context, monkeypatch, field):
    db, _, _, _ = context
    card, ids = history(context)
    claim_only(context, card, monkeypatch)
    with db.session_factory() as session:
        repo = SqlAlchemyAggregateRepository(session)
        before = session.get(AggregateClosure, ids[0])
        original = before.cleared_at, before.clear_reason, before.clear_policy_version
        state = repo.retention_state(card.id, lock=True)
        repo.retention_authorize(state, "claim", OperationBudget(1))
        value = original[0] + timedelta(seconds=1) if field == "cleared_at" else "manual" if field == "clear_reason" else "other_policy"
        with pytest.raises(DBAPIError) as error:
            session.execute(update(AggregateClosure).where(AggregateClosure.origin_point_id == ids[0]).values(**{field: value}))
        assert error.value.orig.sqlstate == "23514"
        session.rollback()
        after = session.get(AggregateClosure, ids[0])
        assert (after.cleared_at, after.clear_reason, after.clear_policy_version) == original
        assert session.get(AggregateRetentionState, card.id).history_revision == 1


def test_active_attempt_delete_is_immediate_and_unlink_reinsert_cannot_hide_aba(context):
    db, _, _, _ = context
    card, identity, attempt_id = storage_attempt(context)
    with db.session_factory() as session:
        repo = SqlAlchemyAggregateRepository(session)
        before = repo.retention_headers(card.id, OperationBudget(1))
        saved = session.get(DetectorWarmup, attempt_id)
        values = {column.key: getattr(saved, column.key) for column in DetectorWarmup.__table__.columns}
        # RESTRICT is immediate even when this FK is DEFERRABLE and all
        # constraints are explicitly deferred: DELETE+INSERT cannot recycle an
        # active identity invisibly to a concurrent retention proof.
        session.execute(text("SET CONSTRAINTS ALL DEFERRED"))
        with pytest.raises(DBAPIError) as error:
            session.execute(delete(DetectorWarmup).where(DetectorWarmup.id == attempt_id))
        assert error.value.orig.sqlstate == "23001"
        session.rollback()
        assert session.get(DetectorWarmup, attempt_id).plan == values["plan"]
        with pytest.raises(DBAPIError) as error:
            session.execute(update(Detector).where(Detector.id == identity).values(active_attempt_id=None))
        assert error.value.orig.sqlstate == "23514"
        session.rollback()
        # A legal unlink/reinsert/relink is visible through the mandatory
        # monotonic revision, even when all compared attempt headers are equal.
        session.execute(update(Detector).where(Detector.id == identity).values(
            active_attempt_id=None, revision=Detector.revision + 1))
        session.execute(delete(DetectorWarmup).where(DetectorWarmup.id == attempt_id))
        values["plan"] = {**values["plan"], "count": 17}
        session.add(DetectorWarmup(**values))
        session.flush()
        session.execute(update(Detector).where(Detector.id == identity).values(
            active_attempt_id=attempt_id, revision=Detector.revision + 1))
        session.commit()
        after = repo.retention_headers(card.id, OperationBudget(1))
        assert after[1] == before[1]
        assert after[0][0]["revision"] == before[0][0]["revision"] + 2
        assert after != before


@pytest.mark.parametrize("fail_then_retry", [False, True])
def test_manual_fence_release_wakes_pending_auto_gc_atomically(context, monkeypatch, fail_then_retry):
    db, catalog, _, _ = context
    card, ids = history(context, members=1)
    claim_only(context, card, monkeypatch)
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        receipt = api.aggregate_command(card.id, UUID(int=1), uuid4().hex, "cleanup", 518 if fail_then_retry else 519)
        operation_id = receipt.cleanup_operation_id
    result = maintain(context, card.id)
    assert result["status"] == "deferred" and result["reason"] == "cleanup_in_progress"
    blocked = read_state(context, card.id)
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        assert card.id not in api.repository.maintenance_due()
        if fail_then_retry:
            compact = api.repository.compact_point

            def fail(*_args, **_kwargs):
                raise RuntimeError("injected manual failure")

            monkeypatch.setattr(api.repository, "compact_point", fail)
            with pytest.raises(RuntimeError, match="injected manual failure"):
                api.advance(card.id)
            assert api.cleanup_operation(card.id, operation_id).status == "failed"
            assert read_state(context, card.id)["next_check_at"] == blocked["next_check_at"]
            api.aggregate_command(card.id, UUID(int=1), uuid4().hex, "cleanup_retry", operation_id=operation_id)
            assert read_state(context, card.id)["next_check_at"] == blocked["next_check_at"]
            assert session.get(AggregateSchedule, card.schedule_id).active_cleanup_id == operation_id
            monkeypatch.setattr(api.repository, "compact_point", compact)
        for _ in range(3):
            api.advance(card.id)
            if api.cleanup_operation(card.id, operation_id).status == "succeeded":
                break
        operation = api.cleanup_operation(card.id, operation_id)
        assert operation.status == "succeeded"
    with db.session_factory() as observer:
        state = observer.get(AggregateRetentionState, card.id)
        assert observer.get(AggregateSchedule, card.schedule_id).active_cleanup_id is None
        assert state.pending_closure_id == ids[0] and state.next_check_at == operation.completed_at
        assert card.id in SqlAlchemyAggregateRepository(observer).maintenance_due()
        assert state.auto_cleared_total == 1
        assert list(observer.scalars(select(Detector).where(Detector.aggregate_id == card.id))) == []



def detector_json_transport_bytes(session, aggregate_id):
    """Server-side size only; no checkpoint/config JSON crosses the driver here."""
    size = sum(
        func.coalesce(func.octet_length(getattr(Detector, key).cast(Text)), 0)
        for key in ("checkpoint", "config", "feature_keys")
    )
    return session.scalar(
        select(func.sum(size + 8192)).where(Detector.aggregate_id == aggregate_id)
    )


def test_checkpoint_omits_unused_warmup_sql_size_but_keeps_detector_and_manual_caps(
    context, monkeypatch
):
    db, _, _, _ = context
    card, _, _, saved = transport_models(context, ["checkpoint"], retained_bytes=8192)
    attempt_id = saved["checkpoint"]["id"]
    with db.session_factory() as session:
        session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        needed_bytes = detector_json_transport_bytes(session, card.id)
        inputs_bytes = session.scalar(
            select(func.octet_length(DetectorWarmup.inputs.cast(Text))).where(
                DetectorWarmup.id == attempt_id
            )
        )
        assert needed_bytes < inputs_bytes < 64 * 1024**2
        assert (
            retention_queries.MAX_SNAPSHOT_BYTES
            == cleanup_queries.MAX_SNAPSHOT_BYTES
            == 64 * 1024**2
        )
        # Lower a test-local guard to the measured required transport. The real
        # unused row is larger, so accidentally serializing/counting it fails.
        monkeypatch.setattr(retention_queries, "MAX_SNAPSHOT_BYTES", needed_bytes)
        repository = SqlAlchemyAggregateRepository(session)
        detectors, attempts, transport, _ = dependency_transport(
            session, repository, card.id, monkeypatch
        )
        assert len(detectors) == 1 and attempts == {}
        assert transport == {"hydrated": set(), "copied": set()}
        monkeypatch.setattr(cleanup_queries, "MAX_SNAPSHOT_BYTES", needed_bytes)
        with pytest.raises(EventTimeBudgetError, match="protection_bytes"):
            repository.prepared_attempt(attempt_id, OperationBudget(0.85))
        monkeypatch.setattr(retention_queries, "MAX_SNAPSHOT_BYTES", needed_bytes - 1)

        def forbidden(*_args, **_kwargs):
            pytest.fail("detector JSON loaded before the required-transport cap")

        monkeypatch.setattr(repository, "protection_detectors", forbidden)
        with pytest.raises(EventTimeBudgetError, match="protection_bytes"):
            repository.retention_dependencies(card.id, OperationBudget(0.85))
        session.rollback()


@pytest.mark.parametrize("kind", ["prepared", "frozen", "sql_null", "json_null"])
def test_required_full_attempt_is_sized_before_any_dependency_json(context, monkeypatch, kind):
    db, _, _, _ = context
    card, _, _, saved = transport_models(context, [kind])
    attempt_id = saved[kind]["id"]
    with db.session_factory() as session:
        session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        detector_bytes = detector_json_transport_bytes(session, card.id)
        attempt_bytes = session.scalar(
            select(
                func.octet_length(func.to_jsonb(DetectorWarmup.__table__.table_valued()).cast(Text))
            ).where(DetectorWarmup.id == attempt_id)
        )
        assert detector_bytes and attempt_bytes
        monkeypatch.setattr(
            retention_queries, "MAX_SNAPSHOT_BYTES", detector_bytes + attempt_bytes - 1
        )
        repository = SqlAlchemyAggregateRepository(session)

        def forbidden(*_args, **_kwargs):
            pytest.fail("required warmup row was not covered by the pre-load aggregate size")

        monkeypatch.setattr(repository, "protection_detectors", forbidden)
        with pytest.raises(EventTimeBudgetError, match="protection_bytes"):
            repository.retention_dependencies(card.id, OperationBudget(0.85))
        session.rollback()


@pytest.mark.parametrize(
    "amount,allowed",
    [(256 * 1024**2 // 8, True), (256 * 1024**2 // 8 + 1, False), (64 * 1024**2 + 1, False)],
)
def test_dependency_transport_memory_arithmetic_stays_bounded_before_hydration(
    monkeypatch, amount, allowed
):
    """Injected scalar checks arithmetic only; it is not the real DB cost proof."""
    repository = SqlAlchemyAggregateRepository(SimpleNamespace(scalar=lambda _query: amount))
    observed = []
    monkeypatch.setattr(repository, "maintenance_budget", lambda _budget: None)

    def projected(*_args, **_kwargs):
        observed.append("loaded")
        return []

    monkeypatch.setattr(repository, "protection_detectors", projected)
    if allowed:
        assert repository.retention_dependencies(uuid4(), OperationBudget(0.85)) == ([], {})
        assert observed == ["loaded"]
    else:
        with pytest.raises(EventTimeBudgetError, match="protection_bytes"):
            repository.retention_dependencies(uuid4(), OperationBudget(0.85))
        assert observed == []


def test_missing_required_attempt_read_remains_fail_closed_and_keeps_identity_pin(
    context, monkeypatch
):
    """A missed read is defensive fault injection, not a committed broken FK."""
    db, _, _, _ = context
    card, _, _, saved = transport_models(context, ["frozen"])
    attempt_id = saved["frozen"]["id"]
    with db.session_factory() as session:
        repository = SqlAlchemyAggregateRepository(session)
        before = repository.retention_headers(card.id, OperationBudget(0.85))
        get = session.get

        def missed(model, identity, **kwargs):
            if model is DetectorWarmup and identity == attempt_id:
                return None
            return get(model, identity, **kwargs)

        monkeypatch.setattr(session, "get", missed)
        detectors, attempts = repository.retention_dependencies(card.id, OperationBudget(0.85))
        assert detectors[0]["active_attempt_id"] == attempt_id and attempts == {}
        with pytest.raises(ValueError, match="warmup_attempt_missing"):
            protection_plan(
                {
                    "anchor_utc": card.anchor_utc,
                    "duration_seconds": 10,
                    "online_start_index": card.online_start_index,
                },
                detectors,
                attempts,
                15,
                15,
                lambda: None,
            )
        assert repository.retention_headers(card.id, OperationBudget(0.85)) == before
        assert get(DetectorWarmup, attempt_id).plan == saved["frozen"]["plan"]
        session.rollback()


def test_corrupt_required_prepared_inputs_are_loaded_validated_and_never_release_source(
    context, monkeypatch
):
    db, _, _, _ = context
    card, points, _, saved = transport_models(context, ["corrupt_prepared"])
    attempt_id = saved["corrupt_prepared"]["id"]
    with db.session_factory() as session:
        repository = SqlAlchemyAggregateRepository(session)
        detectors, attempts, transport, _ = dependency_transport(
            session, repository, card.id, monkeypatch
        )
        assert attempts[attempt_id] == saved["corrupt_prepared"]
        assert transport == {"hydrated": {attempt_id}, "copied": {attempt_id}}
        with pytest.raises(DetectionError, match="prepared_input_hash_mismatch"):
            protection_plan(
                {
                    "anchor_utc": card.anchor_utc,
                    "duration_seconds": 10,
                    "online_start_index": card.online_start_index,
                },
                detectors,
                attempts,
                15,
                15,
                lambda: None,
            )
        session.rollback()
    result = maintain(context, card.id)
    assert result["status"] == "protected" and result["reason"] == "protection_unavailable"
    state = read_state(context, card.id)
    assert state["history_revision"] == state["auto_cleared_total"] == 0
    with db.session_factory() as session:
        assert session.get(AggregateClosure, points[0]).payload_state == "present"
        assert session.get(WorkingWindowPoint, points[0]) is not None
