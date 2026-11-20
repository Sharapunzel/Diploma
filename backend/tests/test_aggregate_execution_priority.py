"""Real-service deadline priority, durable background debt and candidate fences.

The logical UTC clock releases real windows; SQL, writers and operation budgets
remain real. These tests never import a proposed scheduling implementation.
"""

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from threading import Event
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import pytest
from sqlalchemy import select, update
from test_aggregate_worker import ControlledExecutor
from test_event_time_pipeline import context, migrated, write_event

from app.aggregate_composition import build_aggregate_service
from app.aggregation.event_time import OperationBudget
from app.aggregation.worker_admission import FOREGROUND_BURST
from app.core.errors import DomainError
from app.models import (
    AggregateClosure,
    AggregateHistorySpan,
    AggregateLateEvent,
    AggregateMembership,
    AggregateSchedule,
    AggregateWindowAttempt,
)
from app.schemas.aggregates import AggregatePreviewRequest, CreateAggregateRequest
from app.services.implementations.aggregate_snapshot import AggregateSnapshotService
from app.services.implementations.aggregate_worker import AggregateWorker

__all__ = ["context", "migrated"]

BLOCK = FOREGROUND_BURST + 1
PHASES = 2 * BLOCK


@contextmanager
def service(context, case):
    database, catalog, _, _ = context
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        api.repository.now = lambda: case.clock[0]
        yield api


def set_phase(context, case, phase):
    """Persist a legacy/resume phase explicitly under test, not fixture timing."""
    with context[0].session_factory.begin() as session:
        session.execute(update(AggregateSchedule).where(
            AggregateSchedule.id == case.card.schedule_id).values(turn=phase))


def add_card(context, case, phase):
    source = context[2]
    body = AggregatePreviewRequest(source_id=source,
        aggregate_rule={"features": [{"key": "n", "name": "Count", "op": "count"}]},
        schedule={"boundary": case.anchor, "timezone": "UTC", "duration_seconds": case.duration},
        window_start_utc=case.anchor, time_basis="event_time")
    with service(context, case) as api:
        checked = api.validate(body)
        card = api.create(CreateAggregateRequest(**body.model_dump(), name="Deadline priority",
            rule_hash=checked.rule_hash), UUID(int=1), str(uuid4()))
        assert card.online_start_index == 2
        # Both old and proposed policies scan when the first online deadline
        # has not arrived. This establishes genuine pending historical work.
        assert api.advance(card.id)["state"] == "scanned"
    target = SimpleNamespace(**{**vars(case), "card": card})
    set_phase(context, target, phase)
    return target


def due_case(context, phase=0, *, released=True):
    duration = 300
    # A future bootstrap keeps real writer commit times before every snapshot
    # reference without sleeps or fabricated points/membership/quality.
    bootstrap = datetime.now(UTC) + timedelta(seconds=2 * duration)
    anchor = bootstrap - timedelta(seconds=2 * duration)
    events = {index: write_event(context, (anchor + timedelta(seconds=index * duration + 1)).isoformat(),
        offset=index) for index in range(2 + 2 * FOREGROUND_BURST + 2)}
    case = SimpleNamespace(anchor=anchor, bootstrap=bootstrap, duration=duration,
        clock=[bootstrap], events=events)
    case = add_card(context, case, phase)
    if released:
        case.clock[0] = bootstrap + timedelta(seconds=(len(events) - 1) * duration)
    return case


def history_case(context, *, populated=True):
    """Six real historical windows and future online input, without fake points."""
    duration = 300
    bootstrap = datetime.now(UTC) + timedelta(seconds=2 * duration)
    history_windows = 2 * FOREGROUND_BURST
    anchor = bootstrap - timedelta(seconds=history_windows * duration)
    events = {index: write_event(context, (anchor + timedelta(seconds=index * duration + 1)).isoformat(),
        offset=index) for index in range(history_windows + FOREGROUND_BURST + 1)} if populated else {}
    case = SimpleNamespace(anchor=anchor, bootstrap=bootstrap, duration=duration,
        clock=[bootstrap], events=events)
    body = AggregatePreviewRequest(source_id=context[2],
        aggregate_rule={"features": [{"key": "n", "name": "Count", "op": "count"}]},
        schedule={"boundary": anchor, "timezone": "UTC", "duration_seconds": duration},
        window_start_utc=anchor, time_basis="event_time")
    with service(context, case) as api:
        checked = api.validate(body)
        case.card = api.create(CreateAggregateRequest(**body.model_dump(), name="Historical burst",
            rule_hash=checked.rule_hash), UUID(int=1), str(uuid4()))
    assert case.card.online_start_index == history_windows
    return case


def stored(context, case):
    with context[0].session_factory() as session:
        schedule = session.get(AggregateSchedule, case.card.schedule_id)
        closures = list(session.scalars(select(AggregateClosure).where(
            AggregateClosure.aggregate_id == case.card.id)))
        positions = list(session.execute(select(AggregateHistorySpan.first_index,
            AggregateHistorySpan.last_index, AggregateHistorySpan.next_index).where(
                AggregateHistorySpan.aggregate_id == case.card.id)))
        return SimpleNamespace(phase=schedule.turn, next_index=schedule.next_index,
            generation=schedule.execution_generation, scan_after=schedule.scan_after,
            scan_cycle=schedule.scan_cycle, cleanup=schedule.active_cleanup_id,
            ids={row.origin_point_id for row in closures}, positions=positions)


def advance(context, case):
    with service(context, case) as api:
        return api.advance(case.card.id)


def kind(result):
    return result.get("origin") if result["state"] == "closed" else result["state"]


def assert_point(context, case, index, origin):
    identity = uuid5(case.card.schedule_id, "event-time-window:" + str(index))
    with service(context, case) as api:
        point = api.point_at(case.card.id, index)
        assert (point.id, point.aggregate_id, point.window_index, point.origin) == (
            identity, case.card.id, index, origin)
        assert point.event_count == point.features[0].value == 1
    with context[0].session_factory() as session:
        assert set(session.scalars(select(AggregateMembership.event_id).where(
            AggregateMembership.closure_id == identity))) == {case.events[index]}
    return identity


@pytest.mark.parametrize("legacy_phase", [0, 1])
def test_due_online_runs_before_legacy_background_phase(context, legacy_phase):
    case = due_case(context, legacy_phase)
    before = stored(context, case)
    assert before.positions == [(0, 1, 0)]
    result = advance(context, case)
    assert (result["state"], result.get("origin")) == ("closed", "online"), (
        "A due online window must not wait for scan/history fleet rounds", result)
    identity = assert_point(context, case, before.next_index, "online")
    after = stored(context, case)
    assert after.ids - before.ids == {identity}
    assert after.next_index == before.next_index + 1
    assert (after.scan_after, after.scan_cycle, after.positions, after.generation) == (
        before.scan_after, before.scan_cycle, before.positions, before.generation)


def test_fleet_first_foreground_round_closes_due_online_for_every_target(context):
    first = due_case(context)
    due_at = first.clock[0]
    first.clock[0] = first.bootstrap
    cases = [first] + [add_card(context, first, index % 2) for index in range(1, 20)]
    first.clock[0] = due_at
    observed = {}

    def factory(session, catalog):
        api = build_aggregate_service(session, catalog)
        api.repository.now = lambda: first.clock[0]
        original = api.advance

        def record(identity, *, execution_started=None):
            result = original(identity, execution_started=execution_started)
            observed.setdefault(identity, kind(result))
            return result

        api.advance = record
        return api

    worker = AggregateWorker(context[0].session_factory, factory, context[1], Event())
    executor = ControlledExecutor()
    for _ in range(len(cases) // worker.admission.capacity):
        worker.tick(executor)
        assert len(worker.pending) <= worker.admission.capacity == 2
        executor.finish()
    worker.stop.set()
    worker.tick(executor)
    assert observed == {case.card.id: "online" for case in cases}
    assert len({assert_point(context, case, 2, "online") for case in cases}) == len(cases)


def test_continuous_overdue_preserves_background_debt_across_service_restarts(context):
    case = due_case(context)
    expected = (["online"] * FOREGROUND_BURST + ["scanned"]
        + ["online"] * FOREGROUND_BURST + ["initial_history"])
    online = []
    for phase, expected_kind in enumerate(expected):
        before = stored(context, case)
        result = advance(context, case)  # fresh Session and service every call
        after = stored(context, case)
        assert kind(result) == expected_kind
        assert len(after.ids - before.ids) <= 1
        assert after.phase == (phase + 1) % PHASES
        if expected_kind == "online":
            online.append(before.next_index)
            assert after.next_index == before.next_index + 1
        else:
            assert after.next_index == before.next_index
    assert online == list(range(2, 2 + 2 * FOREGROUND_BURST))
    assert stored(context, case).positions == [(0, 1, 1)]
    for index in online:
        assert_point(context, case, index, "online")
    assert_point(context, case, 0, "initial_history")


@pytest.mark.parametrize("phase,expected", [(0, "scanned"), (BLOCK, "initial_history")])
def test_not_due_online_goes_directly_to_next_background(context, phase, expected):
    case = due_case(context, phase, released=False)
    assert kind(advance(context, case)) == expected
    assert stored(context, case).next_index == 2


@pytest.mark.parametrize("phase,first_event", [(FOREGROUND_BURST, "commit"), (PHASES - 1, "history")])
def test_forced_background_does_not_precheck_online_deadline(context, phase, first_event):
    case, events = due_case(context, phase), []
    with service(context, case) as api:
        now, commit, history = api.repository.now, api.uow.commit, api.repository.history_candidate

        def clock():
            events.append("clock")
            return now()

        def committed():
            events.append("commit")
            return commit()

        def historical(*args):
            events.append("history")
            return history(*args)

        api.repository.now, api.uow.commit, api.repository.history_candidate = clock, committed, historical
        api.advance(case.card.id)
    assert events[0] == first_event


def test_failed_reserved_online_attempt_keeps_forced_scan_after_restart(context, monkeypatch):
    case = due_case(context, FOREGROUND_BURST - 1)

    def fail(*args, **kwargs):
        raise RuntimeError("synthetic failure after durable reservation")

    with monkeypatch.context() as patch:
        patch.setattr(AggregateSnapshotService, "build", fail)
        with pytest.raises(RuntimeError, match="after durable reservation"):
            advance(context, case)
    after = stored(context, case)
    assert after.phase == FOREGROUND_BURST and not after.ids and after.next_index == 2
    assert kind(advance(context, case)) == "scanned"
    assert kind(advance(context, case)) == "online"
    assert_point(context, case, 2, "online")


def test_lost_online_commit_ack_retains_one_identity_and_durable_phase(context, monkeypatch):
    case = due_case(context, FOREGROUND_BURST - 1)
    written = []
    build, reads = AggregateSnapshotService.build, []
    def counted(*args, **kwargs):
        reads.append(1)
        return build(*args, **kwargs)
    monkeypatch.setattr(AggregateSnapshotService, "build", counted)
    with service(context, case) as api:
        add, commit = api.repository.add_event_point, api.uow.commit

        def added(*args):
            add(*args)
            written.append(args[0].id)

        def unknown():
            commit()
            if written:
                raise RuntimeError("synthetic lost online commit acknowledgement")

        api.repository.add_event_point, api.uow.commit = added, unknown
        resolved = api.advance(case.card.id)
        assert resolved["state"] == "closed" and resolved["point_id"] == written[0]
        assert resolved["calculation_status"] == "computed" and reads == [1]
    identity = assert_point(context, case, 2, "online")
    after = stored(context, case)
    assert written == [identity] and after.ids == {identity}
    assert after.next_index == 3 and after.phase == FOREGROUND_BURST
    assert kind(advance(context, case)) == "scanned"
    assert stored(context, case).ids == {identity}


def test_phase_only_change_does_not_invalidate_current_online_candidate(context, monkeypatch):
    case = due_case(context, FOREGROUND_BURST - 1)
    original = AggregateSnapshotService.build

    def another_reservation(*args, **kwargs):
        candidate = original(*args, **kwargs)
        set_phase(context, case, PHASES - 1)
        return candidate

    monkeypatch.setattr(AggregateSnapshotService, "build", another_reservation)
    assert kind(advance(context, case)) == "online"
    assert_point(context, case, 2, "online")
    after = stored(context, case)
    assert after.next_index == 3 and after.phase == PHASES - 1


def test_concurrent_saved_closure_keeps_reserved_background_quota(context, monkeypatch):
    case = due_case(context, FOREGROUND_BURST - 1)
    original = AggregateSnapshotService.build

    def committed_by_another_turn(*args, **kwargs):
        candidate = original(*args, **kwargs)
        # TASK12 forbids duplicating an unconfirmed running attempt. Revoke A
        # through real lifecycle commands before B receives a new reservation.
        with service(context, case) as api:
            active = api.repository.window_attempt(case.card.schedule_id, OperationBudget(5))
            old_id = active.attempt_id
            api.aggregate_command(case.card.id, UUID(int=1), "race-pause", "pause")
            api.aggregate_command(case.card.id, UUID(int=1), "race-unpause", "unpause")
            assert api.repository.window_attempt(case.card.schedule_id, OperationBudget(5)) is None
        with monkeypatch.context() as patch:
            patch.setattr(AggregateSnapshotService, "build", original)
            assert kind(advance(context, case)) == "scanned"
            assert kind(advance(context, case)) == "online"
        with service(context, case) as api:
            assert api.point_at(case.card.id, 2).execution.attempt_id != old_id
        return candidate

    with monkeypatch.context() as patch:
        patch.setattr(AggregateSnapshotService, "build", committed_by_another_turn)
        result = advance(context, case)
    identity = assert_point(context, case, 2, "online")
    assert result == {"state": "already_closed", "point_id": identity}
    after = stored(context, case)
    assert after.ids == {identity} and after.next_index == 3 and after.phase == BLOCK + 1
    # The losing attempt must not reset the other committed reservation. Only
    # the two remaining online slots precede the already-owed history turn.
    assert [kind(advance(context, case)) for _ in range(FOREGROUND_BURST - 1)] == ["online", "online"]
    assert kind(advance(context, case)) == "initial_history"
    assert stored(context, case).phase == 0


def test_unconfirmed_running_attempt_does_not_start_another_rr_or_spend_quota(context, monkeypatch):
    case = due_case(context, FOREGROUND_BURST - 1)
    original, reads = AggregateSnapshotService.build, []
    def in_flight(*args, **kwargs):
        reads.append(1)
        assert len(reads) == 1, "an unconfirmed running attempt must not start another RR"
        candidate = original(*args, **kwargs)
        assert kind(advance(context, case)) == "scanned"  # already-owed scan survives
        assert advance(context, case)["state"] == "attempt_running"
        assert stored(context, case).phase == BLOCK
        return candidate
    with monkeypatch.context() as patch:
        patch.setattr(AggregateSnapshotService, "build", in_flight)
        assert kind(advance(context, case)) == "online"
    identity = assert_point(context, case, 2, "online")
    after = stored(context, case)
    assert reads == [1] and after.ids == {identity} and after.phase == BLOCK
    assert [kind(advance(context, case)) for _ in range(FOREGROUND_BURST)] == ["online"] * FOREGROUND_BURST
    assert kind(advance(context, case)) == "initial_history"
    assert stored(context, case).phase == 0


@pytest.mark.parametrize("action", ["pause", "archive", "cleanup"])
def test_command_fence_rejects_exact_online_candidate_without_losing_quota(context, action, monkeypatch):
    case = due_case(context, FOREGROUND_BURST - 1)
    original = AggregateSnapshotService.build

    def fenced(*args, **kwargs):
        candidate = original(*args, **kwargs)
        with service(context, case) as api:
            if action == "archive":
                api.lifecycle(case.card.id, action, UUID(int=1))
            else:
                api.aggregate_command(case.card.id, UUID(int=1), "priority-race", action,
                    0 if action == "cleanup" else None)
        return candidate

    monkeypatch.setattr(AggregateSnapshotService, "build", fenced)
    if action == "cleanup":
        with pytest.raises(DomainError) as error:
            advance(context, case)
        assert error.value.code == "cleanup_in_progress"
    else:
        assert advance(context, case)["state"] == "candidate_cancelled"
    after = stored(context, case)
    assert not after.ids and after.next_index == 2 and after.generation == 1
    assert after.phase == FOREGROUND_BURST


def test_existing_manual_fence_is_handled_before_phase_dispatch(context, monkeypatch):
    case = due_case(context, PHASES - 1)
    with service(context, case) as api:
        receipt = api.aggregate_command(case.card.id, UUID(int=1), "before-priority", "cleanup", 0)

    def forbidden(*args, **kwargs):
        pytest.fail("Manual fence must be handled before starting any snapshot")

    monkeypatch.setattr(AggregateSnapshotService, "build", forbidden)
    assert advance(context, case)["state"] == "succeeded"
    after = stored(context, case)
    assert after.phase == PHASES - 1 and not after.ids and after.next_index == 2
    with service(context, case) as api:
        assert api.cleanup_operation(case.card.id, receipt.cleanup_operation_id).status == "succeeded"


@pytest.mark.parametrize("phase", [0, 2 * FOREGROUND_BURST])
@pytest.mark.parametrize("action", ["pause", "archive"])
def test_inactive_targets_keep_legacy_scan_frequency_and_never_close(context, phase, action):
    case = due_case(context, phase)
    with service(context, case) as api:
        if action == "pause":
            api.aggregate_command(case.card.id, UUID(int=1), "inactive-priority", "pause")
        else:
            api.lifecycle(case.card.id, action, UUID(int=1))
    results = [advance(context, case) for _ in range(2 * FOREGROUND_BURST)]
    assert sum(result["state"] == "scanned" for result in results) == 2
    after = stored(context, case)
    assert not after.ids and after.next_index == 2 and 0 <= after.phase < FOREGROUND_BURST


def test_not_due_history_burst_closes_four_real_windows_before_next_scan(context):
    case = history_case(context)
    assert kind(advance(context, case)) == "scanned"  # retain immediate first discovery
    phases, identities, saved = [], [], None
    for index in range(BLOCK):
        before = stored(context, case)
        result = advance(context, case)  # each reservation/close uses a fresh Session
        assert kind(result) == "initial_history", (
            "Ready history must use all four history-half attempts before another scan",
            {"already_closed_windows": index, "received": kind(result)})
        identity = assert_point(context, case, index, "initial_history")
        after = stored(context, case)
        assert after.ids - before.ids == {identity} and after.next_index == case.card.next_index
        assert after.generation == before.generation
        identities.append(identity)
        phases.append(after.phase)
        if index == 0:
            with service(context, case) as api:
                saved = api.point_at(case.card.id, 0).model_dump()
            # This event must become a late ledger entry on the forced scan,
            # while the already closed point and exact membership stay fixed.
            late = write_event(context, (case.anchor + timedelta(seconds=1)).isoformat(),
                offset=len(case.events), event_id=UUID(int=1))
    assert phases == [BLOCK + 1, BLOCK + 2, PHASES - 1, 0]
    before = stored(context, case)
    assert kind(advance(context, case)) == "scanned"
    after = stored(context, case)
    assert after.ids == before.ids == set(identities) and after.next_index == case.card.next_index
    assert after.phase == BLOCK and after.positions == [(0, 5, BLOCK)]
    with service(context, case) as api:
        assert api.point_at(case.card.id, 0).model_dump() == saved
    assert_point(context, case, 0, "initial_history")
    with context[0].session_factory() as session:
        assert list(session.scalars(select(AggregateLateEvent.event_id).where(
            AggregateLateEvent.closure_id == identities[0]))) == [late]


def test_empty_history_burst_remains_waiting_without_losing_scan(context):
    case = history_case(context, populated=False)
    assert kind(advance(context, case)) == "scanned"
    observed = []

    def factory(session, catalog):
        api = build_aggregate_service(session, catalog)
        api.repository.now = lambda: case.clock[0]
        original = api.advance

        def record(identity, *, execution_started=None):
            result = original(identity, execution_started=execution_started)
            observed.append(result)
            return result

        api.advance = record
        return api

    worker = AggregateWorker(context[0].session_factory, factory, context[1], Event())
    phases = []
    for _ in range(BLOCK):
        assert worker._advance(case.card.id) == {"state": "waiting"}
        assert observed[-1] == {"state": "history_waiting", "work_available": False}, (
            "An empty history attempt must keep the bounded history phase and idle hint", observed[-1])
        phases.append(stored(context, case).phase)
    assert phases == [BLOCK + 1, BLOCK + 2, PHASES - 1, 0]
    assert advance(context, case) == {"state": "scanned", "records": 0, "work_available": False}
    after = stored(context, case)
    assert not after.ids and after.next_index == case.card.next_index and after.phase == BLOCK


def test_due_online_preempts_not_due_history_burst(context):
    case = history_case(context)
    assert kind(advance(context, case)) == "scanned"
    assert kind(advance(context, case)) == "initial_history"
    before = stored(context, case)
    case.clock[0] = case.bootstrap + timedelta(seconds=3 * case.duration)
    result = advance(context, case)
    assert kind(result) == "online"
    identity = assert_point(context, case, before.next_index, "online")
    after = stored(context, case)
    assert after.ids - before.ids == {identity}
    assert after.next_index == before.next_index + 1 and after.positions == before.positions
    assert after.generation == before.generation


def test_failed_history_reservation_keeps_burst_phase_after_restart(context, monkeypatch):
    case = history_case(context)
    assert kind(advance(context, case)) == "scanned"

    def fail(*args, **kwargs):
        raise RuntimeError("synthetic history failure after durable reservation")

    with monkeypatch.context() as patch:
        patch.setattr(AggregateSnapshotService, "build", fail)
        with pytest.raises(RuntimeError, match="history failure after durable reservation"):
            advance(context, case)
    reserved = stored(context, case)
    with context[0].session_factory() as session:
        retry = session.get(AggregateWindowAttempt, case.card.schedule_id)
        assert retry.state == "retryable"
        retry_id, retry_deadline = retry.attempt_id, retry.deadline_at
    assert not reserved.ids and reserved.next_index == case.card.next_index
    assert reserved.positions == [(0, 5, 0)]
    result = advance(context, case)
    assert kind(result) == "initial_history", (
        "Restart must continue the reserved history half before scanning again", result)
    assert reserved.phase == BLOCK + 1
    assert_point(context, case, 0, "initial_history")
    with service(context, case) as api:
        resumed = api.point_at(case.card.id, 0)
        assert resumed.execution.attempt_id == retry_id
        assert resumed.execution.execution_deadline_at == retry_deadline
    for index in range(1, FOREGROUND_BURST):
        assert kind(advance(context, case)) == "initial_history"
        assert_point(context, case, index, "initial_history")
    assert stored(context, case).phase == PHASES - 1
    # Retrying the same reserved H is not another quota attempt. The fourth
    # unique history window remains owed before the next scan.
    assert kind(advance(context, case)) == "initial_history"
    assert_point(context, case, FOREGROUND_BURST, "initial_history")
    assert stored(context, case).phase == 0
    assert kind(advance(context, case)) == "scanned"
