"""Real PostgreSQL claims, protection races, authority and restartable GC.

The large canonical rows are storage fixtures with unknown quality, not forged
RCF training evidence. Real-model/runtime acceptance remains in the ML/live gate.
"""

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event
from uuid import UUID, uuid4

import pytest
from retention_assertions import (
    assert_maintenance_result,
    maintenance_trace,
)
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.exc import DBAPIError
from test_event_time_pipeline import context, create, migrated

from app.aggregate_composition import build_aggregate_service
from app.aggregation.event_time import OperationBudget
from app.core.errors import DomainError
from app.detector_composition import build_detector_service
from app.models import (
    AggregateClosure,
    AggregateMembership,
    AggregateRetentionState,
    AggregateSnapshot,
    WorkingWindowPoint,
)
from app.repositories.sqlalchemy import SqlAlchemyUnitOfWork
from app.repositories.sqlalchemy.aggregates import SqlAlchemyAggregateRepository
from app.repositories.sqlalchemy.detectors import SqlAlchemyDetectorRepository
from app.services.implementations.detector_history import DetectorHistoryService

__all__ = ["context", "migrated"]


def history(context, count=520, members=0, omit_indices=(), first_index=0):
    db, _, source, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=2)).replace(microsecond=0)
    card, _ = create(context, anchor, 10)
    ids = [uuid4() for _ in range(count)]
    with db.session_factory.begin() as session:
        for index, identity in enumerate(ids):
            index += first_index
            if index in omit_indices:
                continue
            start = anchor + timedelta(seconds=10 * index)
            end = start + timedelta(seconds=10)
            session.add(AggregateClosure(origin_point_id=identity, aggregate_id=card.id,
                schedule_id=card.schedule_id, source_identity=source, window_index=index,
                start_utc=start, end_utc=end, time_basis="event_time", origin="initial_history",
                snapshot_reference_at=end, closed_at=end, close_not_before_at=end,
                payload_state="present", point_kind="working"))
        session.flush()
        for index, identity in enumerate(ids):
            index += first_index
            if index in omit_indices:
                continue
            start = anchor + timedelta(seconds=10 * index)
            end = start + timedelta(seconds=10)
            session.add(WorkingWindowPoint(id=identity, schedule_id=card.schedule_id, window_index=index,
                start_utc=start, end_utc=end, scheduled_at=end, actual_started_at=end, actual_completed_at=end,
                event_count=0, features=[{"key": "n", "value": 0, "reason": None, "valid_count": 0, "invalid_count": 0},
                    {"key": "bytes", "value": None, "reason": "no_values", "valid_count": 0, "invalid_count": 0}],
                completeness="unknown", completeness_reason="storage_fixture", evidence_refs={}, time_basis="event_time"))
            session.add(AggregateSnapshot(closure_id=identity, metadata_json={"storage_fixture": True}))
        for _ in range(members):
            session.add(AggregateMembership(closure_id=ids[0], event_id=uuid4(),
                event_time=anchor + timedelta(seconds=1), fingerprint="f" * 64))
    return card, ids


def maintain(context, identity):
    db, catalog, _, _ = context
    with db.session_factory() as session:
        return build_aggregate_service(session, catalog).maintain(identity)


def read_state(context, identity):
    db, _, _, _ = context
    with db.session_factory() as session:
        row = session.get(AggregateRetentionState, identity)
        return {column.key: getattr(row, column.key) for column in row.__table__.columns}


def settle(context, identity, expected, turns=40):
    for _ in range(turns):
        result = maintain(context, identity)
        assert result["status"] != "error", result
        row = read_state(context, identity)
        if row["auto_cleared_total"] == expected and row["pending_closure_id"] is None:
            return row
    pytest.fail("bounded maintenance did not finish its fixed fixture")


def test_auto_reserve_includes_unknown_grid_and_supports_both_future_plans(context):
    db, catalog, _, _ = context
    card, ids = history(context)
    original = card.next_index, card.execution_generation, card.paused
    row = settle(context, card.id, 8)
    assert row["reserve_windows"] == 512 and row["keep_from_index"] == 8
    assert row["history_revision"] == row["auto_cleared_total"] == 8
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        current = api.card(card.id)
        assert (current.next_index, current.execution_generation, current.paused) == original
        assert current.active_cleanup_id is None and current.last_error == card.last_error
        assert api.points(card.id, 100, 0)["total"] == 512
        assert (current.auto_cleanup.retained_from_index, current.auto_cleanup.retained_to_index) == (8, 519)
        for identity in ids[:8]:
            assert session.get(WorkingWindowPoint, identity) is None
            assert session.get(AggregateSnapshot, identity) is None
            closure = session.get(AggregateClosure, identity)
            assert (closure.payload_state, closure.clear_reason, closure.clear_policy_version) == (
                "cleared", "automatic", "model_history_v1")
            with pytest.raises(DomainError) as error:
                api.point_by_id(card.id, identity)
            assert error.value.status_code == 410 and error.value.details["cleared_at"] is not None
        model_api = build_detector_service(session, catalog)
        models = [model_api.create(card.id, UUID(int=1), "After GC", config={"sample_size": 8, "output_after": 8},
            accelerated=fast, step=5 if fast else None) for fast in (False, True)]
        repository = SqlAlchemyDetectorRepository(session)
        planner = DetectorHistoryService(repository, api.repository, None, api.uow, catalog)
        for identity in models:
            saved = planner.plan(repository.read_context(identity), time.monotonic() + 30)
            assert saved["count"] == 16
            assert saved["base_closures"][0]["window_index"] >= 8


def test_claim_commit_is_atomic_before_large_membership_gc_and_unknown_ack(context, monkeypatch):
    db, catalog, _, _ = context
    card, ids = history(context, members=5001)
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        commit, commits, observations = api.uow.commit, [0], []
        def lost_first_ack():
            commit()
            commits[0] += 1
            if commits[0] == 1:
                with db.session_factory() as observer:
                    closure = observer.get(AggregateClosure, ids[0])
                    observations.append((closure.payload_state,
                        observer.get(WorkingWindowPoint, ids[0]), observer.get(AggregateSnapshot, ids[0]),
                        observer.scalar(select(func.count()).select_from(AggregateMembership).where(
                            AggregateMembership.closure_id == ids[0], AggregateMembership.fingerprint.is_not(None)))))
                raise DBAPIError(None, None, RuntimeError("lost acknowledgement"))
        monkeypatch.setattr(api.uow, "commit", lost_first_ack)
        result = api.maintain(card.id)
        assert result["pending"]
        assert observations == [("cleared", None, None, 5001)]
    state = read_state(context, card.id)
    assert state["auto_cleared_total"] == state["history_revision"] == 1
    assert state["pending_closure_id"] == ids[0]
    # A fresh service/session resumes the one durable point before claiming any
    # other point. Membership identities survive each <=5000-row portion.
    with maintenance_trace(db) as trace:
        result = maintain(context, card.id)
    assert_maintenance_result(result, trace)
    with db.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(AggregateMembership).where(
            AggregateMembership.closure_id == ids[0])) == 5001
        assert session.scalar(select(func.count()).select_from(AggregateMembership).where(
            AggregateMembership.closure_id == ids[0], AggregateMembership.fingerprint.is_not(None))) == 1
        assert session.get(AggregateClosure, ids[1]).payload_state == "present"
    cursor = read_state(context, card.id)["pending_cursor"]
    assert cursor is not None and cursor.int > 0
    # The DB permits late details even after logical clearing. A lower UUID
    # must remain discoverable; a progress cursor cannot be a seek boundary.
    with db.session_factory.begin() as session:
        session.add(AggregateMembership(closure_id=ids[0], event_id=UUID(int=0),
            event_time=card.anchor_utc, fingerprint="late-after-cursor"))
        expected_ids = set(session.scalars(select(AggregateMembership.event_id).where(
            AggregateMembership.closure_id == ids[0])))
    assert len(expected_ids) == 5002
    for _ in range(2):
        with maintenance_trace(db) as tail:
            result = maintain(context, card.id)
        assert_maintenance_result(result, tail)
        assert all(row["rows"] <= 5000 for row in tail["durations"])
    assert [row["rows"] for row in trace["durations"] if row["kind"] in {"SELECT", "UPDATE"}] == [5000, 5000]
    assert read_state(context, card.id)["auto_cleared_total"] == 1
    with db.session_factory() as session:
        pairs = list(session.execute(select(AggregateMembership.event_id, AggregateMembership.fingerprint,
            AggregateMembership.event_time).where(AggregateMembership.closure_id == ids[0])))
        assert set(pairs) == {(identity, None, None) for identity in expected_ids}


def test_remaining_membership_query_uses_bounded_index_on_selective_history(context, monkeypatch, capsys):
    db, catalog, _, _ = context
    card, (target, other, *_) = history(context, count=513, members=2)
    with db.session_factory.begin() as session:
        session.execute(text("""
            INSERT INTO logs.aggregate_memberships (closure_id, event_id, event_time, fingerprint)
            SELECT :closure, gen_random_uuid(), :at, 'unrelated'
            FROM generate_series(1, 20000)
        """), {"closure": other, "at": card.anchor_utc})
    # Keep just the committed claim, then capture the SELECT on a rollback of
    # the physical portion so both EXPLAINs use the real query on committed data.
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        commit = api.uow.commit

        def stop_after_claim():
            commit()
            monkeypatch.setattr(api.uow, "commit", commit)
            raise DBAPIError(None, None, RuntimeError("lost acknowledgement"))

        monkeypatch.setattr(api.uow, "commit", stop_after_claim)
        assert api.maintain(card.id)["pending"]
    with db.session_factory() as session:
        repository = SqlAlchemyAggregateRepository(session)
        budget = OperationBudget(5)
        state = repository.retention_state(card.id, lock=True)
        assert state.pending_closure_id == target
        repository.retention_authorize(state, "memberships", budget)
        with maintenance_trace(db) as trace:
            repository.compact_memberships(target, budget, automatic=True)
        session.rollback()
    statement, parameters = trace["queries"]["aggregate_memberships"]

    def nodes(node):
        yield node
        for child in node.get("Plans", []):
            yield from nodes(child)

    with db.engine.begin() as connection:
        expected = set(connection.execute(select(AggregateMembership.event_id).where(
            AggregateMembership.closure_id == target)).scalars())
    assert len(expected) == 2
    for phase, expected_rows in (("before", expected), ("after", set())):
        if phase == "after":
            assert maintain(context, card.id)["status"] != "error"
        with db.engine.begin() as connection:
            connection.exec_driver_sql("ANALYZE logs.aggregate_memberships")
            plan = connection.exec_driver_sql("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + statement,
                parameters).scalar_one()[0]
            assert set(connection.exec_driver_sql(statement, parameters).scalars()) == expected_rows
        root = plan["Plan"]
        with capsys.disabled():
            print(f"Remaining membership {phase}: {plan}", flush=True)
        assert root["Actual Rows"] == len(expected_rows)
        assert root["Shared Hit Blocks"] + root["Shared Read Blocks"] <= 32
        assert any(node.get("Index Name") == "ix_membership_retention_remaining" for node in nodes(root)), plan
    with db.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(AggregateMembership).where(
            AggregateMembership.closure_id == other, AggregateMembership.fingerprint == "unrelated")) == 20000


@pytest.mark.parametrize("wrong", ["missing", "foreign", "phase", "transaction"])
def test_narrow_authorization_does_not_clear_foreign_or_present_details(context, wrong):
    db, _, _, _ = context
    card, ids = history(context, members=1)
    with db.session_factory() as session:
        repo = SqlAlchemyAggregateRepository(session)
        state = repo.retention_state(card.id, lock=True)
        if wrong != "missing":
            state.pending_closure_id = ids[1] if wrong == "foreign" else ids[0]
            repo.retention_authorize(state, "outcomes" if wrong == "phase" else "memberships", OperationBudget(1))
            if wrong == "transaction":
                session.commit()
        with pytest.raises(DBAPIError):
            session.execute(update(AggregateMembership).where(AggregateMembership.closure_id == ids[0])
                .values(fingerprint=None, event_time=None))
        session.rollback()
        assert session.get(AggregateClosure, ids[0]).payload_state == "present"
        assert session.get(WorkingWindowPoint, ids[0]) is not None
        assert session.scalar(select(AggregateMembership.fingerprint).where(
            AggregateMembership.closure_id == ids[0])) is not None


def test_payload_delete_rolls_back_if_claim_authorization_is_rejected(context):
    db, _, _, _ = context
    card, ids = history(context, members=1)
    with db.session_factory() as session:
        repo = SqlAlchemyAggregateRepository(session)
        closure = session.get(AggregateClosure, ids[0])
        with pytest.raises(DBAPIError):
            repo.clear_payload(closure, OperationBudget(1), automatic=True)
        session.rollback()
        assert session.get(WorkingWindowPoint, ids[0]) is not None
        assert session.get(AggregateSnapshot, ids[0]) is not None
        assert session.get(AggregateClosure, ids[0]).payload_state == "present"
        assert session.get(AggregateRetentionState, card.id).history_revision == 0


@pytest.mark.parametrize("action", ["create", "start", "stop", "pause", "delete", "manual"])
def test_dependency_barrier_rechecks_real_concurrent_control(context, monkeypatch, action):
    db, catalog, _, _ = context
    card, ids = history(context)
    with db.session_factory() as session:
        models = build_detector_service(session, catalog)
        identity = models.create(card.id, UUID(int=1), "Race", config={"sample_size": 8, "output_after": 8})
        if action != "stop":
            models.stop(identity)
    observed, changed = Event(), Event()
    def mutate():
        assert observed.wait(10)
        with db.session_factory() as session:
            models = build_detector_service(session, catalog)
            api = build_aggregate_service(session, catalog)
            if action == "create":
                models.create(card.id, UUID(int=1), "Concurrent", config={"sample_size": 8, "output_after": 8})
            elif action in {"start", "stop"}:
                getattr(models, action)(identity)
            elif action == "delete":
                api.lifecycle(card.id, "delete", UUID(int=1))
            elif action == "manual":
                api.aggregate_command(card.id, UUID(int=1), uuid4().hex, "cleanup", 512)
            else:
                api.aggregate_command(card.id, UUID(int=1), uuid4().hex, "pause")
        changed.set()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(mutate)
        with db.session_factory() as session:
            api = build_aggregate_service(session, catalog)
            candidate = api.repository.retention_candidate
            def barrier(*args, **kwargs):
                result = candidate(*args, **kwargs)
                if not kwargs.get("lock") and len(args) < 5:
                    observed.set()
                    assert changed.wait(10)
                return result
            monkeypatch.setattr(api.repository, "retention_candidate", barrier)
            result = api.maintain(card.id)
        future.result(timeout=10)
    assert result["status"] == "deferred" and result["reason"] == "dependencies_changed"
    with db.session_factory() as session:
        assert session.get(WorkingWindowPoint, ids[0]) is not None
        assert session.get(AggregateRetentionState, card.id).auto_cleared_total == 0


def test_missing_state_is_read_only_unknown_and_manual_records_each_actual_clear(context):
    db, catalog, _, _ = context
    card, ids = history(context)
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        receipt = api.aggregate_command(card.id, UUID(int=1), uuid4().hex, "cleanup", 518)
        assert api.maintain(card.id)["reason"] == "cleanup_in_progress"
        for _ in range(15):
            result = api.advance(card.id)
            if result.get("state") == "succeeded":
                break
        operation = api.cleanup_operation(card.id, receipt.cleanup_operation_id)
        assert operation.status == "succeeded" and operation.deleted_points == 2
        summary = api.card(card.id).auto_cleanup
        assert summary.history_revision == 2 and summary.auto_cleared_total == 0
        with pytest.raises(DomainError) as error:
            api.point_at(card.id, 0)
        assert error.value.details["clear_reason"] == "manual"
        assert error.value.details["clear_policy_version"] is None
        session.execute(delete(AggregateRetentionState).where(AggregateRetentionState.aggregate_id == card.id))
        session.commit()
        missing = api.card(card.id).auto_cleanup
        assert missing.status == "error" and missing.reason == "retention_state_missing"
        assert missing.policy_version is missing.history_revision is missing.auto_cleared_total is None
        assert missing.reserve_windows is missing.protected_total is missing.retained_from_index is None
        assert missing.protected_by == []
        assert api.maintain(card.id)["reason"] == "retention_state_missing"
        assert session.get(AggregateRetentionState, card.id) is None
        assert session.get(AggregateClosure, ids[0]).clear_reason == "manual"


def test_stale_working_cursor_rejected_before_removable_payload_lookup(context, monkeypatch):
    db, catalog, _, _ = context
    card, _ = history(context)
    with db.session_factory() as session:
        identity = build_detector_service(session, catalog).create(card.id, UUID(int=1), "Stale")
        repo = SqlAlchemyDetectorRepository(session)
        expected = repo.read_context(identity)
        session.rollback()
        build_detector_service(session, catalog).stop(identity)
        with pytest.raises(ValueError, match="stale_detector_candidate"), SqlAlchemyUnitOfWork(session).read_snapshot():
            repo.validate_working_context(expected)
