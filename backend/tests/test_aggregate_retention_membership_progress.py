"""Real PostgreSQL short portions, durable progress, late and manual callers.

Run through backend-db --pytest; storage fixtures never supply ML evidence.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from retention_assertions import assert_maintenance_result, maintenance_trace
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from test_aggregate_retention import history, maintain, read_state
from test_aggregate_retention_races import add_closure
from test_event_time_pipeline import context, create, migrated, turns, write_event

from app.aggregate_composition import build_aggregate_service
from app.models import AggregateClosure, AggregateLateEvent, AggregateMembership

__all__ = ["context", "migrated"]


def claim_only(context, card, monkeypatch):
    database, catalog, _, _ = context
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        commit, first = api.uow.commit, [True]
        def lost_claim_ack():
            commit()
            if first[0]:
                first[0] = False
                raise DBAPIError(None, None, RuntimeError("claim ACK lost"))
        monkeypatch.setattr(api.uow, "commit", lost_claim_ack)
        result = api.maintain(card.id)
        assert result["pending"], result
    state = read_state(context, card.id)
    assert state["pending_phase"] == "memberships"
    assert state["authorization_txid"] is None
    assert state["auto_cleared_total"] == state["history_revision"] == 1


def membership_rows(database, point):
    with database.session_factory() as session:
        return list(session.execute(select(AggregateMembership.closure_id,
            AggregateMembership.event_id, AggregateMembership.fingerprint,
            AggregateMembership.event_time).where(AggregateMembership.closure_id == point)
            .order_by(AggregateMembership.event_id)))


@pytest.mark.parametrize("count", [0, 1, 4999, 5000, 5001, 10000, 10001])
def test_membership_short_full_and_exact_multiples_keep_every_id_pair(context, monkeypatch, count):
    database, _, _, _ = context
    card, points = history(context, count=513, members=count)
    before = membership_rows(database, points[0])
    pairs = [(row.closure_id, row.event_id) for row in before]
    claim_only(context, card, monkeypatch)
    portions = 0
    with maintenance_trace(database) as trace:
        while read_state(context, card.id)["pending_phase"] == "memberships":
            assert portions < 5
            result = maintain(context, card.id)
            assert_maintenance_result(result, trace)
            portions += 1
            state = read_state(context, card.id)
            if state["pending_phase"] == "memberships":
                assert state["pending_cursor"] is not None
            else:
                assert state["pending_cursor"] is None
    # Short positive final portions finish in this call, while an exact
    # multiple retains the conservative next search (including zero rows).
    assert portions == count // 5000 + 1
    selects = [x for x in trace["durations"] if x["kind"] == "SELECT" and x["rows"] >= 0]
    assert len(selects) >= portions
    assert all(x["rows"] <= 5000 for x in trace["durations"] if x["rows"] >= 0)
    remaining = membership_rows(database, points[0])
    assert [(row.closure_id, row.event_id) for row in remaining] == pairs
    assert all(row.fingerprint is None and row.event_time is None for row in remaining)
    if count % 5000:
        assert state["pending_phase"] == "outcomes" and result["pending"]
        result = maintain(context, card.id)
    assert not result["pending"]
    assert read_state(context, card.id)["auto_cleared_total"] == 1


def test_lower_uuid_remains_visible_below_full_portion_cursor(context, monkeypatch):
    database, _, _, _ = context
    card, points = history(context, count=513, members=5001)
    claim_only(context, card, monkeypatch)
    maintain(context, card.id)
    cursor = read_state(context, card.id)["pending_cursor"]
    assert cursor is not None and cursor.int > 0
    # A deliberate direct storage insert preserves the pre-existing regression:
    # the partial predicate, never diagnostic cursor, defines completeness.
    with database.session_factory.begin() as session:
        session.add(AggregateMembership(closure_id=points[0], event_id=UUID(int=0),
            event_time=card.anchor_utc, fingerprint="below-diagnostic-cursor"))
    result = maintain(context, card.id)
    assert result["pending"] and read_state(context, card.id)["pending_phase"] == "outcomes"
    rows = membership_rows(database, points[0])
    assert len(rows) == 5002 and rows[0].event_id == UUID(int=0)
    assert all(row.fingerprint is None and row.event_time is None for row in rows)
    assert not maintain(context, card.id)["pending"]


@pytest.mark.parametrize("lost_ack", [False, True], ids=["rollback", "unknown-commit"])
def test_finished_short_portion_recovers_only_committed_phase(context, monkeypatch, lost_ack):
    database, catalog, _, _ = context
    card, points = history(context, count=513, members=1)
    before = membership_rows(database, points[0])
    claim_only(context, card, monkeypatch)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        commit, first = api.uow.commit, [True]
        def uncertain_commit():
            if first[0]:
                first[0] = False
                if lost_ack:
                    commit()
                raise DBAPIError(None, None, RuntimeError("portion commit not acknowledged"))
            commit()
        monkeypatch.setattr(api.uow, "commit", uncertain_commit)
        result = api.maintain(card.id)
        assert result["reason"] == "retention_database_error"
    state = read_state(context, card.id)
    rows = membership_rows(database, points[0])
    assert [(row.closure_id, row.event_id) for row in rows] == [
        (row.closure_id, row.event_id) for row in before]
    assert state["pending_phase"] == ("outcomes" if lost_ack else "memberships")
    assert (rows[0].fingerprint is None) == lost_ack
    assert state["authorization_txid"] is None
    # Fresh sessions/service instances are the production restart path.
    for _ in range(4):
        result = maintain(context, card.id)
        if not result["pending"]:
            break
    assert not result["pending"]
    assert read_state(context, card.id)["auto_cleared_total"] == 1
    assert membership_rows(database, points[0])[0].fingerprint is None


@pytest.mark.parametrize("count", [0, 1, 5001])
def test_manual_caller_keeps_fence_and_all_id_pairs_with_new_result(context, count):
    database, catalog, _, _ = context
    card, points = history(context, count=513, members=count)
    before = membership_rows(database, points[0])
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        accepted = api.aggregate_command(card.id, UUID(int=1), "membership-manual", "cleanup", 512)
        operation = accepted.cleanup_operation_id
        for _ in range(10):
            result = api.advance(card.id)
            if result["state"] == "succeeded":
                break
        assert result["state"] == "succeeded"
        assert api.cleanup_operation(card.id, operation).deleted_points == 1
    rows = membership_rows(database, points[0])
    assert [(row.closure_id, row.event_id) for row in rows] == [
        (row.closure_id, row.event_id) for row in before]
    assert all(row.fingerprint is None and row.event_time is None for row in rows)
    state = read_state(context, card.id)
    assert state["auto_cleared_total"] == 0 and state["history_revision"] == 1
    with database.session_factory() as session:
        assert session.get(AggregateClosure, points[0]).clear_reason == "manual"


def test_manual_short_details_commit_does_not_start_outcomes_in_same_turn(context, monkeypatch):
    database, catalog, _, _ = context
    card, points = history(context, count=513, members=1)
    before = membership_rows(database, points[0])
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        accepted = api.aggregate_command(card.id, UUID(int=1), "manual-short-commit", "cleanup", 512)
        calls = []

        def unavailable_outcomes(*_args, **_kwargs):
            calls.append("outcomes")
            raise AssertionError("manual short membership must commit before optional outcomes")

        with monkeypatch.context() as patch:
            patch.setattr(api.repository, "compact_outcomes", unavailable_outcomes)
            result = api.advance(card.id)
        assert result["state"] == "running" and result["deleted_points"] == 0
        assert calls == []
        rows = membership_rows(database, points[0])
        assert [(row.closure_id, row.event_id) for row in rows] == [
            (row.closure_id, row.event_id) for row in before]
        assert rows[0].fingerprint is None and rows[0].event_time is None
        assert session.get(AggregateClosure, points[0]).payload_state == "present"
        for _ in range(5):
            result = api.advance(card.id)
            if result["state"] == "succeeded":
                break
        assert result["state"] == "succeeded"
        assert api.cleanup_operation(card.id, accepted.cleanup_operation_id).deleted_points == 1


def test_real_closed_manifest_stays_fixed_during_concurrent_late_and_short_gc(context, monkeypatch):
    database, catalog, _, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=3)).replace(microsecond=0)
    original = write_event(context, (anchor + timedelta(seconds=1)).isoformat(), event_id=UUID(int=100))
    write_event(context, (anchor + timedelta(seconds=601)).isoformat(), offset=1)
    card, _ = create(context, anchor)
    turns(context, card.id, 30)
    with database.session_factory() as session:
        closure = session.scalar(select(AggregateClosure).where(
            AggregateClosure.schedule_id == card.schedule_id, AggregateClosure.window_index == 0))
        point = closure.origin_point_id
        assert closure.calculation_status == "computed" and closure.payload_state == "present"
    before = membership_rows(database, point)
    assert len(before) == 1 and before[0].event_id == original
    add_closure(context, card, 1020)
    claim_only(context, card, monkeypatch)
    inserted = []
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        compact = api.repository.compact_memberships

        def concurrent_late(point_id, budget, automatic=False):
            portion = compact(point_id, budget, automatic)
            assert point_id == point and portion.changed_count == 1 and portion.cursor is None
            # This GC UoW still owns schedule/state locks. Real ingest uses
            # its own transaction and never appends a closed membership.
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(write_event, context,
                    (anchor + timedelta(seconds=2)).isoformat(), offset=2, event_id=UUID(int=0))
                inserted.append(future.result(timeout=10))
            return portion

        monkeypatch.setattr(api.repository, "compact_memberships", concurrent_late)
        result = api.maintain(card.id)
        assert result["status"] == "compacting" and result["pending"], result
    assert inserted == [UUID(int=0)]
    assert read_state(context, card.id)["pending_phase"] == "outcomes"
    after = membership_rows(database, point)
    assert [(row.closure_id, row.event_id) for row in after] == [
        (row.closure_id, row.event_id) for row in before]
    assert all(row.fingerprint is None and row.event_time is None for row in after)
    turns(context, card.id, 40)
    with database.session_factory() as session:
        ledger = list(session.scalars(select(AggregateLateEvent.event_id).where(
            AggregateLateEvent.closure_id == point)))
        assert ledger == [UUID(int=0)]
        assert session.get(AggregateClosure, point).payload_state == "cleared"
    assert membership_rows(database, point) == after
