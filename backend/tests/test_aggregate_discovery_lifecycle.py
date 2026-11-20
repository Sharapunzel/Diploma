"""Real discovery preserves lifecycle fences and independent cleanup progress."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from itertools import product
from uuid import UUID, uuid4

import pytest
from app.aggregate_composition import build_aggregate_service
from app.aggregation.event_time import bounds, close_deadline
from app.core.errors import DomainError
from app.db import Database
from app.ecs import PackagedEcsCatalog
from app.models import (
    Aggregate,
    AggregateClosure,
    AggregateHistorySpan,
    AggregateLateEvent,
    AggregateMembership,
    AggregateRetentionState,
    AggregateSchedule,
    AggregateSnapshot,
    AggregateTimeExclusion,
    KafkaConnection,
    Source,
    WorkingWindowPoint,
)
from app.services.implementations.aggregates import future_index
from sqlalchemy import MetaData, func, select
from sqlalchemy.exc import DBAPIError
from test_aggregate_retention import history, maintain, read_state
from test_event_time_migration import legacy_database
from test_event_time_pipeline import context, create, migrated, turns, write_event

__all__ = ["context", "legacy_database", "migrated"]


@pytest.fixture
def retired_context(legacy_database):
    """Use the existing pre-0021 fixture and its real retirement migration."""
    engine, url, migrate = legacy_database
    metadata = MetaData()
    names = ("aggregate_schedules", "aggregates", "working_window_points")
    metadata.reflect(engine, schema="logs", only=names)
    tables = {name: metadata.tables["logs." + name] for name in names}
    now = datetime.now(UTC)
    anchor = (now - timedelta(days=2)).replace(microsecond=0)
    source_id, connection_id, point_id = uuid4(), uuid4(), uuid4()
    rows = [(uuid4(), uuid4(), state, paused)
        for state, paused in product(("active", "archived", "deleted"), (False, True))]
    features = [{"key": "n", "value": 1, "reason": None, "valid_count": 1, "invalid_count": 0}]
    with engine.begin() as connection:
        for identity, schedule_id, state, _paused in rows:
            connection.execute(tables["aggregate_schedules"].insert(), {
                "id": schedule_id, "source_identity": source_id, "anchor_utc": anchor,
                "duration_seconds": 300, "next_index": 1})
            connection.execute(tables["aggregates"].insert(), {
                "id": identity, "schedule_id": schedule_id, "name": "Legacy discovery",
                "source_identity": source_id, "source_name": "Legacy source", "source_type": "kafka",
                "rule": {"features": [{"key": "n", "name": "Count", "op": "count"}]},
                "rule_hash": "f" * 64, "original_boundary": anchor.isoformat(), "timezone": "UTC",
                "created_at": now, "created_by": UUID(int=1), "state": state,
                "request_key": uuid4().hex, "request_hash": "e" * 64})
        connection.execute(tables["working_window_points"].insert(), {
            "id": point_id, "schedule_id": rows[0][1], "window_index": 0,
            "start_utc": anchor, "end_utc": anchor + timedelta(seconds=300),
            "scheduled_at": anchor + timedelta(seconds=300), "actual_started_at": now,
            "actual_completed_at": now, "event_count": 1, "features": features,
            "completeness": "unknown", "completeness_reason": "legacy_storage_fixture",
            "evidence_refs": {"legacy_storage_fixture": True}})
    migrate("upgrade", "head")
    db = Database(url)
    try:
        # The same ordinary model-based source fixture used by context; all
        # objects belong to legacy_database, whose finally drops this own DB.
        with db.session_factory.begin() as session:
            session.add(KafkaConnection(id=connection_id, name=str(connection_id),
                bootstrap_servers=["kafka:9092"], cluster_identity="event-time-test"))
            session.flush()
            session.add(Source(id=source_id, connection_id=connection_id,
                name=str(source_id), topic_name=str(source_id), is_enabled=True))
            for identity, schedule_id, _state, paused in rows:
                aggregate = session.get(Aggregate, identity)
                schedule = session.get(AggregateSchedule, schedule_id)
                assert aggregate.execution_mode == "retired"
                assert aggregate.semantic_basis == schedule.semantic_basis == "availability_v1"
                schedule.paused = paused
        catalog = PackagedEcsCatalog.load()
        with db.session_factory() as session:
            api = build_aggregate_service(session, catalog)
            cards = [(api.card(identity), state, "retired", paused)
                for identity, _schedule_id, state, paused in rows]
            closure = session.get(AggregateClosure, point_id)
            assert (closure.time_basis, closure.origin) == ("availability_at", "legacy")
            assert session.get(AggregateSnapshot, point_id) is None
            assert not session.scalars(select(AggregateMembership.event_id).where(
                AggregateMembership.closure_id == point_id)).all()
        yield (db, catalog, source_id, connection_id), cards, [point_id]
    finally:
        # This fixture finalizes before legacy_database disposes engine/DROP.
        db.dispose()


def owned_due(context, identities, *, limit=100, exclude_ids=()):
    db, catalog, _, _ = context
    with db.session_factory() as session:
        foreign = list(session.scalars(select(Aggregate.id).where(
            Aggregate.id.not_in(identities))))
        return build_aggregate_service(session, catalog).due(limit, [*foreign, *exclude_ids])


def due_turns(context, identity, count=20, *, now=None):
    db, catalog, _, _ = context
    for _ in range(count):
        assert owned_due(context, [identity]) == [identity]
        with db.session_factory() as session:
            api = build_aggregate_service(session, catalog)
            if now is not None:
                api.repository.now = lambda: now
            api.advance(identity)


def progress(context, card):
    db, _, _, _ = context
    with db.session_factory() as session:
        schedule = session.get(AggregateSchedule, card.schedule_id)
        position = {key: getattr(schedule, key) for key in (
            "anchor_utc", "duration_seconds", "bootstrap_at", "online_start_index",
            "next_index", "scan_after", "scan_cycle", "turn")}
        spans = list(session.scalars(select(AggregateHistorySpan).where(
            AggregateHistorySpan.aggregate_id == card.id).order_by(AggregateHistorySpan.cohort_key)))
        return deepcopy((position, [
            {column.key: getattr(span, column.key) for column in span.__table__.columns}
            for span in spans]))


def closed_snapshot(context, identity):
    db, _, _, _ = context
    with db.session_factory() as session:
        closure = session.get(AggregateClosure, identity)
        point = session.get(WorkingWindowPoint, identity)
        members = list(session.execute(select(
            AggregateMembership.event_id, AggregateMembership.event_time,
            AggregateMembership.fingerprint).where(
                AggregateMembership.closure_id == identity).order_by(AggregateMembership.event_id)))
        return deepcopy((
            {column.key: getattr(closure, column.key) for column in closure.__table__.columns},
            {column.key: getattr(point, column.key) for column in point.__table__.columns},
            session.get(AggregateSnapshot, identity).metadata_json,
            members,
        ))


def make_inactive(context, card, kind):
    db, catalog, _, _ = context
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        if kind == "paused":
            api.aggregate_command(card.id, UUID(int=1), uuid4().hex, "pause")
        elif kind in {"archived", "deleted"}:
            api.lifecycle(card.id, "archive" if kind == "archived" else "delete", UUID(int=1))
        else:
            raise AssertionError("retirement requires the existing migration fixture")


def test_due_lifecycle_matrix_cleanup_exception_exclusions_and_disabled_source(retired_context):
    context, cards, _ = retired_context
    db, _, source_id, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=2)).replace(microsecond=0)
    cards = [*cards, *[(create(context, anchor)[0], state, "current", paused)
        for state, paused in product(("active", "archived", "deleted"), (False, True))]]
    identities = [card.id for card, *_ in cards]
    with db.session_factory.begin() as session:
        session.get(Source, source_id).is_enabled = False
        for card, state, mode, paused in cards:
            aggregate = session.get(Aggregate, card.id)
            assert aggregate.execution_mode == mode
            aggregate.state = state
            schedule = session.get(AggregateSchedule, card.schedule_id)
            schedule.paused = paused
    expected = [card.id for card, state, mode, paused in cards
        if state == "active" and mode == "current" and not paused]
    assert owned_due(context, identities) == expected
    assert owned_due(context, identities, exclude_ids=expected) == []

    # Every lifecycle combination remains discoverable for its existing manual
    # fence. There is no FK from the schedule fence to an operation row.
    delayed = identities[0]
    with db.session_factory.begin() as session:
        for card, *_ in cards:
            session.get(AggregateSchedule, card.schedule_id).active_cleanup_id = uuid4()
        session.get(Aggregate, delayed).last_error_at = datetime.now(UTC)
    ordered = [*sorted(set(identities) - {delayed}), delayed]
    assert owned_due(context, identities) == ordered
    assert owned_due(context, identities, limit=3) == ordered[:3]
    assert owned_due(context, identities, exclude_ids=ordered[:2]) == ordered[2:]
    with db.session_factory.begin() as session:
        for card, *_ in cards:
            session.get(AggregateSchedule, card.schedule_id).active_cleanup_id = None
    assert owned_due(context, identities) == expected


@pytest.mark.parametrize("kind", ["paused", "archived"])
def test_resume_preserves_scan_history_and_closed_membership_then_catches_up(context, kind):
    db, catalog, _, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=2)).replace(microsecond=0)
    first = write_event(context, (anchor + timedelta(seconds=1)).isoformat(), event_id=UUID(int=100))
    card, _ = create(context, anchor)
    turns(context, card.id, 12)
    with db.session_factory() as session:
        closed = build_aggregate_service(session, catalog).point_at(card.id, 0).id
    saved = closed_snapshot(context, closed)
    assert [row[0] for row in saved[3]] == [first]
    # Persist a valid nonempty cycling-scan position to distinguish preservation
    # from resetting to the beginning; lower UUIDs must appear on a later cycle.
    with db.session_factory.begin() as session:
        session.get(AggregateSchedule, card.schedule_id).scan_after = first
    make_inactive(context, card, kind)
    before = progress(context, card)
    assert owned_due(context, [card.id]) == []
    late = write_event(context, (anchor + timedelta(seconds=2)).isoformat(),
        event_id=UUID(int=1), offset=1)
    excluded = write_event(context, "invalid", event_id=UUID(int=2), offset=2)
    backlog = write_event(context, (anchor + timedelta(seconds=601)).isoformat(),
        event_id=UUID(int=300), offset=3)
    assert progress(context, card) == before
    assert closed_snapshot(context, closed) == saved
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        if kind == "paused":
            old_deadline = api.card(card.id).next_close_not_before_at
            api.aggregate_command(card.id, UUID(int=1), uuid4().hex, "unpause")
            assert api.card(card.id).next_close_not_before_at == old_deadline
            expected = before
        else:
            resume_at = datetime.now(UTC) + timedelta(seconds=900)
            schedule = session.get(AggregateSchedule, card.schedule_id)
            next_index = max(schedule.next_index, future_index(schedule, resume_at))
            api.repository.now = lambda: resume_at
            resumed = api.lifecycle(card.id, "resume", UUID(int=1))
            expected = deepcopy(before)
            expected[0]["next_index"] = next_index
            assert resumed.skipped_ranges[-1] == {
                "start_utc": bounds(card.anchor_utc, card.duration_seconds, before[0]["next_index"])[0].isoformat(),
                "end_utc": bounds(card.anchor_utc, card.duration_seconds, next_index)[0].isoformat(),
                "reason": "intentional_archive",
            }
            assert resumed.next_close_not_before_at == close_deadline(
                bounds(card.anchor_utc, card.duration_seconds, next_index)[1], card.duration_seconds)
    assert progress(context, card) == expected
    assert owned_due(context, [card.id]) == [card.id]
    due_turns(context, card.id, 24)
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        assert api.point_at(card.id, 2).event_count == 1
        assert session.scalar(select(AggregateMembership.event_id).where(
            AggregateMembership.closure_id == api.point_at(card.id, 2).id)) == backlog
        assert list(session.scalars(select(AggregateLateEvent.event_id).where(
            AggregateLateEvent.closure_id == closed))) == [late]
        assert session.scalar(select(AggregateTimeExclusion.reason).where(
            AggregateTimeExclusion.aggregate_id == card.id,
            AggregateTimeExclusion.event_id == excluded)) == "event_time_invalid"
        assert session.get(AggregateSchedule, card.schedule_id).scan_cycle > before[0]["scan_cycle"]
    assert closed_snapshot(context, closed) == saved
    due_turns(context, card.id, 8)
    with db.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(AggregateLateEvent).where(
            AggregateLateEvent.closure_id == closed)) == 1
        assert session.scalar(select(func.count()).select_from(AggregateTimeExclusion).where(
            AggregateTimeExclusion.aggregate_id == card.id)) == 1


def test_unpause_closes_missed_online_windows_at_original_deadlines(context):
    db, catalog, _, _ = context
    anchor = (datetime.now(UTC) - timedelta(days=2)).replace(microsecond=0)
    card, _ = create(context, anchor)
    start, end = bounds(card.anchor_utc, card.duration_seconds, card.next_index)
    deadline = close_deadline(end, card.duration_seconds)
    make_inactive(context, card, "paused")
    before = progress(context, card)
    events = [write_event(context, (start + timedelta(seconds=1 + i * card.duration_seconds)).isoformat(),
        offset=i) for i in range(2)]
    now = deadline + timedelta(seconds=card.duration_seconds, microseconds=1)
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        api.repository.now = lambda: now
        api.aggregate_command(card.id, UUID(int=1), uuid4().hex, "unpause")
        assert api.card(card.id).next_close_not_before_at == deadline
    assert progress(context, card) == before
    due_turns(context, card.id, 16, now=now)
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        assert api.card(card.id).next_index == card.next_index + 2
        for offset, event_id in enumerate(events):
            point = api.point_at(card.id, card.next_index + offset)
            closure = session.get(AggregateClosure, point.id)
            assert point.event_count == 1 and closure.calculation_status == "computed"
            assert closure.close_not_before_at == deadline + timedelta(seconds=offset * card.duration_seconds)
            assert session.scalar(select(AggregateMembership.event_id).where(
                AggregateMembership.closure_id == point.id)) == event_id
        assert api.points(card.id, 50, 0)["total"] == 2


@pytest.mark.parametrize("kind", ["paused", "archived", "deleted", "retired"])
def test_inactive_manual_cleanup_is_discovered_until_fence_release(context, kind, request):
    if kind == "retired":
        context, retired, identities = request.getfixturevalue("retired_context")
        card = retired[0][0]
    else:
        card, identities = history(context, count=1, members=1)
        make_inactive(context, card, kind)
    db, catalog, _, _ = context
    assert owned_due(context, [card.id]) == []
    with db.session_factory() as session:
        pairs = set(session.execute(select(AggregateMembership.closure_id, AggregateMembership.event_id).where(
            AggregateMembership.closure_id == identities[0])))
        assert len(pairs) == (0 if kind == "retired" else 1)
        receipt = build_aggregate_service(session, catalog).aggregate_command(
            card.id, UUID(int=1), uuid4().hex, "cleanup", 0)
    assert owned_due(context, [card.id]) == [card.id]
    for _ in range(6):
        with db.session_factory() as session:
            api = build_aggregate_service(session, catalog)
            assert card.id in api.due()
            api.advance(card.id)
            if api.cleanup_operation(card.id, receipt.cleanup_operation_id).status == "succeeded":
                break
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        operation = api.cleanup_operation(card.id, receipt.cleanup_operation_id)
        assert operation.status == "succeeded" and operation.deleted_points == 1
        assert session.get(AggregateSchedule, card.schedule_id).active_cleanup_id is None
        assert session.get(AggregateClosure, identities[0]).payload_state == "cleared"
        assert session.get(WorkingWindowPoint, identities[0]) is None
        assert session.get(AggregateSnapshot, identities[0]) is None
        assert set(session.execute(select(AggregateMembership.closure_id, AggregateMembership.event_id).where(
            AggregateMembership.closure_id == identities[0]))) == pairs
        with pytest.raises(DomainError) as error:
            api.point_by_id(card.id, identities[0])
        assert error.value.status_code == 410
    assert owned_due(context, [card.id]) == []


@pytest.mark.parametrize("kind", ["paused", "archived"])
def test_inactive_pending_gc_remains_maintenance_due_without_new_claims(context, kind):
    db, catalog, _, _ = context
    card, identities = history(context, count=514, members=1)
    with db.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        commit = api.uow.commit
        lost = []

        def lose_claim_ack():
            commit()
            if not lost:
                lost.append(True)
                raise DBAPIError(None, None, RuntimeError("synthetic claim ACK loss"))

        api.uow.commit = lose_claim_ack
        assert api.maintain(card.id)["pending"]
    assert read_state(context, card.id)["pending_closure_id"] == identities[0]
    make_inactive(context, card, kind)
    before = progress(context, card)
    with db.session_factory.begin() as session:
        session.get(AggregateRetentionState, card.id).next_check_at = datetime.now(UTC)
        pairs = set(session.execute(select(AggregateMembership.closure_id, AggregateMembership.event_id).where(
            AggregateMembership.closure_id == identities[0])))
    assert owned_due(context, [card.id]) == []
    for _ in range(4):
        with db.session_factory() as session:
            api = build_aggregate_service(session, catalog)
            assert card.id in api.maintenance_due()
            assert card.id not in api.maintenance_due(exclude_ids=[card.id])
        assert maintain(context, card.id)["status"] != "error"
        if read_state(context, card.id)["pending_closure_id"] is None:
            break
    state = read_state(context, card.id)
    assert state["pending_closure_id"] is None and state["auto_cleared_total"] == 1
    # A subsequent ordinary turn must defer the second eligible old point.
    assert maintain(context, card.id)["status"] == "inactive"
    assert progress(context, card) == before
    with db.session_factory() as session:
        assert session.get(AggregateClosure, identities[0]).payload_state == "cleared"
        assert session.get(AggregateClosure, identities[1]).payload_state == "present"
        assert session.get(WorkingWindowPoint, identities[1]) is not None
        assert session.get(AggregateSnapshot, identities[1]) is not None
        assert set(session.execute(select(AggregateMembership.closure_id, AggregateMembership.event_id).where(
            AggregateMembership.closure_id == identities[0]))) == pairs
        assert session.scalar(select(func.count()).select_from(AggregateMembership).where(
            AggregateMembership.closure_id == identities[0],
            AggregateMembership.fingerprint.is_not(None) | AggregateMembership.event_time.is_not(None))) == 0
        assert session.get(WorkingWindowPoint, identities[0]) is None
        assert session.get(AggregateSnapshot, identities[0]) is None
