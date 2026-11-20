"""Real transaction regressions for terminal timeout, recovery and cancellation."""

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import pytest
from app.aggregation.event_time import EventTimeBudgetError, OperationBudget
from app.aggregation.time_policy import execution_diagnostics
from app.core.errors import DomainError
from app.models import (
    AggregateClosure,
    AggregateMembership,
    AggregateSnapshot,
    AggregateWindowAttempt,
    WorkingWindowPoint,
)
from app.schemas.aggregates import AggregatePreviewRequest
from app.services.implementations.aggregate_snapshot import AggregateSnapshotService
from sqlalchemy import func, null, select
from sqlalchemy.exc import DBAPIError, IntegrityError
from test_aggregate_execution_priority import add_card, service
from test_event_time_pipeline import context, migrated, write_event

__all__ = ["context", "migrated"]


def budget_case(context, duration=300):
    # Terminal negative fixtures use physically past windows. Operational UTC
    # deadlines are deliberately separate from the synthetic due clock.
    bootstrap = datetime.now(UTC) - timedelta(seconds=3 * duration)
    anchor = bootstrap - timedelta(seconds=2 * duration)
    events = {index: write_event(context, (anchor + timedelta(seconds=index * duration + min(1, duration / 2))).isoformat(),
        offset=index) for index in range(10)}
    case = SimpleNamespace(anchor=anchor, bootstrap=bootstrap, duration=duration, clock=[bootstrap], events=events)
    case = add_card(context, case, 0)
    case.clock[0] = bootstrap + timedelta(seconds=9 * duration)
    return case


def controlled_budget(monkeypatch):
    clock = [100.0]

    class ControlledBudget(OperationBudget):
        def __init__(self, seconds, clock_override=None, started=None):
            super().__init__(seconds, clock=lambda: clock[0], started=started)

    for module in ("aggregate_execution", "aggregates"):
        monkeypatch.setattr("app.services.implementations." + module + ".OperationBudget", ControlledBudget)
    for module in ("aggregate_execution", "aggregate_attempts", "aggregates"):
        monkeypatch.setattr("app.services.implementations." + module + ".monotonic", lambda: clock[0])
    return clock


@contextmanager
def timed_service(context, case, clock):
    with service(context, case) as api:
        base = case.clock[0]
        api.repository.now = lambda: base + timedelta(seconds=clock[0] - 100)
        if not hasattr(case, "execution_anchor"):
            case.execution_anchor = datetime.now(UTC)
        api.repository.execution_now = lambda: case.execution_anchor + timedelta(seconds=clock[0] - 100)
        yield api


def assert_terminal(context, case, index=2, status="uncomputed"):
    with service(context, case) as api:
        point = api.point_at(case.card.id, index)
        assert point.id == uuid5(case.card.schedule_id, "event-time-window:" + str(index))
        assert point.calculation_status == status
        assert api.point_by_id(case.card.id, point.id).model_dump() == point.model_dump()
        assert api.repository.schedule(case.card.schedule_id).next_index == index + 1
        assert api.repository.window_attempt(case.card.schedule_id, OperationBudget(5)) is None
        if status == "uncomputed":
            assert point.calculation_reason == "execution_budget_exceeded"
            for field in ("event_count", "features", "event_time_min", "event_time_max", "snapshot_reference_at",
                    "snapshot_metadata", "completeness", "completeness_reason", "evidence_refs"):
                assert getattr(point, field) is None
            assert point.execution.hard_budget_seconds == 30
            for model in (AggregateSnapshot, AggregateMembership):
                assert api.repository.session.scalar(select(func.count()).select_from(model).where(
                    model.closure_id == point.id)) == 0
        return point


@pytest.mark.parametrize("phase", ["snapshot", "save"])
def test_hard_timeout_rolls_back_partial_candidate_closes_once_and_next_window_computes(context, monkeypatch, phase):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    original = AggregateSnapshotService.build
    with timed_service(context, case, clock) as api:
        if phase == "snapshot":
            def exceed(*args, **kwargs):
                candidate = original(*args, **kwargs)
                clock[0] += 30.01
                args[-1].check()
                return candidate
            monkeypatch.setattr(AggregateSnapshotService, "build", exceed)
        else:
            add = api.repository.add_event_point
            def exceed(point, closure, members, metadata, budget):
                add(point, closure, members, metadata, budget)
                if closure.calculation_status == "computed":
                    clock[0] += 30.01
                    budget.check()
            api.repository.add_event_point = exceed
        result = api.advance(case.card.id)
        assert result["state"] == "closed" and result["calculation_status"] == "uncomputed"
    assert_terminal(context, case)
    monkeypatch.setattr(AggregateSnapshotService, "build", original)
    with timed_service(context, case, clock) as api:
        assert api.advance(case.card.id)["calculation_status"] == "computed"
    assert_terminal(context, case, 3, "computed")


def test_restart_expired_attempt_preserves_identity_deadline_and_never_rebuilds(context, monkeypatch):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    def crash(*_args, **_kwargs):
        raise RuntimeError("simulated process loss after reservation")
    monkeypatch.setattr(AggregateSnapshotService, "build", crash)
    with timed_service(context, case, clock) as api, pytest.raises(RuntimeError, match="process loss"):
        api.advance(case.card.id)
    with context[0].session_factory() as session:
        before = session.get(AggregateWindowAttempt, case.card.schedule_id)
        identity, deadline = before.attempt_id, before.deadline_at
    clock[0] += 31
    with timed_service(context, case, clock) as api:
        assert api.advance(case.card.id)["calculation_status"] == "uncomputed"
    point = assert_terminal(context, case)
    assert point.execution.attempt_id == identity and point.execution.execution_deadline_at == deadline


@pytest.mark.parametrize(("duration", "hard_seconds"), [(1, .8), (37, 29.6)])
def test_fractional_durable_deadline_terminal_and_next_cursor(context, monkeypatch, duration, hard_seconds):
    case = budget_case(context, duration)
    clock = controlled_budget(monkeypatch)
    original = AggregateSnapshotService.build

    def crash(*_args, **_kwargs):
        raise SystemExit("crash after the fractional attempt was committed")

    monkeypatch.setattr(AggregateSnapshotService, "build", crash)
    with timed_service(context, case, clock) as api, pytest.raises(SystemExit):
        api.advance(case.card.id)
    with context[0].session_factory() as session:
        attempt = session.get(AggregateWindowAttempt, case.card.schedule_id)
        assert attempt.state == "running" and attempt.window_index == 2 and attempt.origin == "online"
        assert attempt.hard_budget_seconds == hard_seconds
        assert attempt.deadline_at - attempt.started_at == timedelta(seconds=hard_seconds)
        identity, started, deadline = attempt.attempt_id, attempt.started_at, attempt.deadline_at
        assert session.scalar(select(func.count()).select_from(AggregateClosure).where(
            AggregateClosure.aggregate_id == case.card.id)) == 0
    clock[0] += hard_seconds + .01
    monkeypatch.setattr(AggregateSnapshotService, "build", lambda *_args, **_kwargs: pytest.fail("expired attempt must not rebuild"))
    with timed_service(context, case, clock) as api:
        assert api.advance(case.card.id)["calculation_status"] == "uncomputed"
    with service(context, case) as api:
        point = api.point_at(case.card.id, 2)
        assert point.id == uuid5(case.card.schedule_id, "event-time-window:2")
        assert point.calculation_status == "uncomputed" and point.calculation_reason == "execution_budget_exceeded"
        assert point.execution.attempt_id == identity and point.execution.started_at == started
        assert point.execution.execution_deadline_at == deadline and point.execution.hard_budget_seconds == hard_seconds
        assert api.point_by_id(case.card.id, point.id).model_dump() == point.model_dump()
        assert api.repository.schedule(case.card.schedule_id).next_index == 3
        assert api.repository.window_attempt(case.card.schedule_id, OperationBudget(5)) is None
        for field in ("event_count", "features", "event_time_min", "event_time_max", "snapshot_reference_at",
                "snapshot_metadata", "completeness", "completeness_reason", "evidence_refs"):
            assert getattr(point, field) is None
        for model in (AggregateSnapshot, AggregateMembership):
            assert api.repository.session.scalar(select(func.count()).select_from(model).where(
                model.closure_id == point.id)) == 0
    monkeypatch.setattr(AggregateSnapshotService, "build", original)
    with timed_service(context, case, clock) as api:
        assert api.advance(case.card.id)["calculation_status"] == "computed"
    with service(context, case) as api:
        following = api.point_at(case.card.id, 3)
        assert following.id == uuid5(case.card.schedule_id, "event-time-window:3")
        assert following.event_count == 1 and following.calculation_status == "computed"
        assert following.execution.attempt_id != identity and following.execution.hard_budget_seconds == hard_seconds
        assert api.repository.schedule(case.card.schedule_id).next_index == 4
        assert api.repository.window_attempt(case.card.schedule_id, OperationBudget(5)) is None
        members = list(api.repository.session.scalars(select(AggregateMembership.event_id).where(
            AggregateMembership.closure_id == following.id)))
        assert members == [case.events[3]]


def test_confirmed_pause_revokes_expired_candidate_without_terminal_or_cursor_change(context, monkeypatch):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    original = AggregateSnapshotService.build
    def pause_then_expire(*args, **kwargs):
        original(*args, **kwargs)
        with service(context, case) as command:
            command.aggregate_command(case.card.id, UUID(int=1), "budget-pause", "pause")
        clock[0] += 31
        args[-1].check()
    monkeypatch.setattr(AggregateSnapshotService, "build", pause_then_expire)
    with timed_service(context, case, clock) as api:
        assert api.advance(case.card.id)["state"] == "candidate_cancelled"
    with service(context, case) as api:
        assert api.card(case.card.id).next_index == 2
        assert api.repository.closure_at(case.card.schedule_id, 2) is None
        assert api.repository.window_attempt(case.card.schedule_id, OperationBudget(5)) is None


def test_lost_ack_after_successful_commit_past_deadline_remains_computed(context, monkeypatch):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    with timed_service(context, case, clock) as api:
        original, calls = api.uow.commit, []
        def lost_ack():
            calls.append(1)
            original()
            if len(calls) == 2:
                clock[0] += 31
                raise RuntimeError("commit acknowledgement lost")
        api.uow.commit = lost_ack
        assert api.advance(case.card.id)["calculation_status"] == "computed"
    assert_terminal(context, case, status="computed")


def test_known_resource_cap_is_not_reclassified_as_timeout_after_restart(context, monkeypatch):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    original = AggregateSnapshotService.build
    def cap(*_args, **_kwargs):
        raise EventTimeBudgetError("snapshot_bytes")
    monkeypatch.setattr(AggregateSnapshotService, "build", cap)
    with timed_service(context, case, clock) as api, pytest.raises(DomainError) as error:
        api.advance(case.card.id)
    assert error.value.details["factor"] == "snapshot_bytes"
    with context[0].session_factory() as session:
        assert session.get(AggregateWindowAttempt, case.card.schedule_id) is None
        assert session.scalar(select(AggregateClosure).where(AggregateClosure.schedule_id == case.card.schedule_id,
            AggregateClosure.window_index == 2)) is None
    clock[0] += 31
    monkeypatch.setattr(AggregateSnapshotService, "build", original)
    with timed_service(context, case, clock) as api:
        assert api.advance(case.card.id)["calculation_status"] == "computed"
    assert_terminal(context, case, status="computed")


def test_early_sql_cancel_retains_attempt_and_original_deadline(context, monkeypatch):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    original = AggregateSnapshotService.build
    class Cancelled(Exception):
        sqlstate = "57014"
    def cancel(*_args, **_kwargs):
        raise DBAPIError(None, None, Cancelled("external cancel"))
    monkeypatch.setattr(AggregateSnapshotService, "build", cancel)
    with timed_service(context, case, clock) as api, pytest.raises(DomainError) as error:
        api.advance(case.card.id)
    assert error.value.code == "aggregate_timeout"
    with context[0].session_factory() as session:
        active = session.get(AggregateWindowAttempt, case.card.schedule_id)
        identity, deadline = active.attempt_id, active.deadline_at
    monkeypatch.setattr(AggregateSnapshotService, "build", original)
    with timed_service(context, case, clock) as api:
        result = api.advance(case.card.id)
        assert result["calculation_status"] == "computed"
        assert result["execution"]["attempt_id"] == str(identity)
        assert result["execution"]["execution_deadline_at"] == deadline.isoformat()


def test_confirmed_rollback_retry_keeps_identity_and_old_error_cannot_clobber_terminal(context, monkeypatch):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    def failed(*_args, **_kwargs):
        raise RuntimeError("candidate stopped; own rollback can be confirmed")
    with timed_service(context, case, clock) as first:
        with monkeypatch.context() as patch:
            patch.setattr(AggregateSnapshotService, "build", failed)
            with pytest.raises(RuntimeError):
                first.advance(case.card.id)
        active = first.repository.window_attempt(case.card.schedule_id, OperationBudget(5))
        assert active.state == "retryable"
        identity, deadline = active.attempt_id, active.deadline_at
        phase = first.repository.schedule(case.card.schedule_id).turn
        first.uow.rollback()
        with timed_service(context, case, clock) as second:
            result = second.advance(case.card.id)
            assert result["execution"]["attempt_id"] == str(identity)
            assert result["execution"]["execution_deadline_at"] == deadline.isoformat()
            assert second.repository.schedule(case.card.schedule_id).turn == phase
        first.record_worker_error(case.card.id, "stale candidate failure")
        assert first.card(case.card.id).last_error is None
    assert_terminal(context, case, status="computed")


def test_old_generation_error_metadata_cannot_follow_lifecycle_cancellation(context, monkeypatch):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    def failed(*_args, **_kwargs):
        raise RuntimeError("discarded candidate")
    monkeypatch.setattr(AggregateSnapshotService, "build", failed)
    with timed_service(context, case, clock) as first:
        with pytest.raises(RuntimeError):
            first.advance(case.card.id)
        with service(context, case) as command:
            command.aggregate_command(case.card.id, UUID(int=1), "metadata-pause", "pause")
            command.aggregate_command(case.card.id, UUID(int=1), "metadata-unpause", "unpause")
        first.record_worker_error(case.card.id, "stale candidate failure")
        card = first.card(case.card.id)
        assert card.last_error is None and card.next_index == 2
        assert first.repository.window_attempt(case.card.schedule_id, OperationBudget(5)) is None


def test_known_source_error_does_not_become_timeout_on_later_retry(context, monkeypatch):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    original = AggregateSnapshotService.build
    def unavailable(*_args, **_kwargs):
        raise DomainError("source_unavailable", "source unavailable", 409)
    monkeypatch.setattr(AggregateSnapshotService, "build", unavailable)
    with timed_service(context, case, clock) as api, pytest.raises(DomainError) as error:
        api.advance(case.card.id)
    assert error.value.code == "source_unavailable"
    with context[0].session_factory() as session:
        assert session.get(AggregateWindowAttempt, case.card.schedule_id) is None
    clock[0] += 31
    monkeypatch.setattr(AggregateSnapshotService, "build", original)
    with timed_service(context, case, clock) as api:
        assert api.advance(case.card.id)["calculation_status"] == "computed"


@pytest.mark.parametrize("commit", [2,3])
def test_finalization_lost_ack_is_resolved_with_same_service_budget(context, monkeypatch, commit):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    original_build = AggregateSnapshotService.build
    def exceed(*args, **kwargs):
        original_build(*args, **kwargs)
        clock[0] += 30.01
        args[-1].check()
    monkeypatch.setattr(AggregateSnapshotService, "build", exceed)
    with timed_service(context, case, clock) as api:
        original_commit, calls = api.uow.commit, []
        def lost_ack():
            calls.append(1)
            original_commit()
            if len(calls) == commit:
                clock[0] += .5
                raise RuntimeError("lost failure acknowledgement")
        api.uow.commit = lost_ack
        result = api.advance(case.card.id)
        assert result["calculation_status"] == "uncomputed"
        assert len(calls) == 3
    assert_terminal(context, case)


def test_crash_without_rollback_proof_waits_then_finalizes_without_new_rr(context, monkeypatch):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    def crash(*_args, **_kwargs):
        raise SystemExit("simulated hard crash without handler proof")
    monkeypatch.setattr(AggregateSnapshotService, "build", crash)
    with timed_service(context, case, clock) as api, pytest.raises(SystemExit):
        api.advance(case.card.id)
    with context[0].session_factory() as session:
        active = session.get(AggregateWindowAttempt, case.card.schedule_id)
        assert active.state == "running"
        identity, deadline = active.attempt_id, active.deadline_at
    monkeypatch.setattr(AggregateSnapshotService, "build", lambda *_args, **_kwargs: pytest.fail("no rollback proof"))
    with timed_service(context, case, clock) as api:
        assert api.advance(case.card.id)["state"] == "attempt_running"
    clock[0] += 30.01
    with timed_service(context, case, clock) as api:
        result = api.advance(case.card.id)
        assert result["calculation_status"] == "uncomputed"
        assert result["execution"]["attempt_id"] == str(identity)
        assert result["execution"]["execution_deadline_at"] == deadline.isoformat()
    assert_terminal(context, case)


@pytest.mark.parametrize("read", [0,1])
def test_utc_backstep_never_grants_new_time_or_computes(context, monkeypatch, read):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    monkeypatch.setattr(AggregateSnapshotService, "build", lambda *_args, **_kwargs: (_ for _ in ()).throw(SystemExit()))
    with timed_service(context, case, clock) as api, pytest.raises(SystemExit):
        api.advance(case.card.id)
    with context[0].session_factory() as session:
        active = session.get(AggregateWindowAttempt, case.card.schedule_id)
        identity, deadline = active.attempt_id, active.deadline_at
    monkeypatch.setattr(AggregateSnapshotService, "build", lambda *_args, **_kwargs: pytest.fail("clock recovery must not compute"))
    with timed_service(context, case, clock) as api:
        calls = []
        def reference():
            index = len(calls); calls.append(1)
            return case.execution_anchor - timedelta(seconds=1) if index == read else case.execution_anchor
        api.repository.execution_now = reference
        with pytest.raises(DomainError) as error:
            api.advance(case.card.id)
        assert error.value.code == "aggregate_recovery_required"
    with context[0].session_factory() as session:
        active = session.get(AggregateWindowAttempt, case.card.schedule_id)
        assert (active.attempt_id, active.deadline_at, active.state) == (identity, deadline, "running")
        assert session.scalar(select(func.count()).select_from(AggregateClosure).where(
            AggregateClosure.aggregate_id == case.card.id)) == 0


def test_expired_finalization_budget_yields_without_a_second_allowance(context, monkeypatch):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    original_build = AggregateSnapshotService.build
    def exceed(*args, **kwargs):
        original_build(*args, **kwargs)
        clock[0] += 30.01
        args[-1].check()
    monkeypatch.setattr(AggregateSnapshotService, "build", exceed)
    with timed_service(context, case, clock) as api:
        original_commit, calls = api.uow.commit, []
        def fail_finalization():
            calls.append(1)
            if len(calls) == 3:
                clock[0] += 5.01
                raise RuntimeError("finalization connection failed")
            original_commit()
        api.uow.commit = fail_finalization
        with pytest.raises(DomainError):
            api.advance(case.card.id)
    with context[0].session_factory() as session:
        active = session.get(AggregateWindowAttempt, case.card.schedule_id)
        assert active.state == "finalizing"
        identity, deadline = active.attempt_id, active.deadline_at
        assert session.scalar(select(func.count()).select_from(AggregateClosure).where(
            AggregateClosure.aggregate_id == case.card.id)) == 0
    monkeypatch.setattr(AggregateSnapshotService, "build", lambda *_args, **_kwargs: pytest.fail("must not recompute"))
    with timed_service(context, case, clock) as api:
        result = api.advance(case.card.id)
        assert result["calculation_status"] == "uncomputed"
        assert result["execution"]["attempt_id"] == str(identity)
        assert result["execution"]["execution_deadline_at"] == deadline.isoformat()


@pytest.mark.parametrize("action", ["pause", "archive", "delete", "cleanup"])
def test_lifecycle_revokes_attempt_before_expiry_without_terminal_or_cursor(context, monkeypatch, action):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    original = AggregateSnapshotService.build
    def revoked(*args, **kwargs):
        original(*args, **kwargs)
        with service(context, case) as api:
            if action in {"archive", "delete"}:
                api.lifecycle(case.card.id, action, UUID(int=1))
            else:
                api.aggregate_command(case.card.id, UUID(int=1), "revoke-" + action, action,
                    0 if action == "cleanup" else None)
        clock[0] += 30.01
        args[-1].check()
    monkeypatch.setattr(AggregateSnapshotService, "build", revoked)
    with timed_service(context, case, clock) as api:
        assert api.advance(case.card.id)["state"] == "candidate_cancelled"
    with service(context, case) as api:
        assert api.card(case.card.id).next_index == 2
        assert api.repository.window_attempt(case.card.schedule_id, OperationBudget(5)) is None
        assert api.points(case.card.id, 20, 0)["total"] == 0
    if action == "pause":
        monkeypatch.setattr(AggregateSnapshotService, "build", original)
        with service(context, case) as api:
            api.aggregate_command(case.card.id, UUID(int=1), "unpause-new-attempt", "unpause")
        with timed_service(context, case, clock) as api:
            assert api.advance(case.card.id)["calculation_status"] == "computed"
        assert_terminal(context, case, status="computed")


def test_preview_first_subject_read_timeout_has_safe_execution_diagnostics_and_no_writes(context, monkeypatch):
    clock = controlled_budget(monkeypatch)
    anchor = datetime.now(UTC) - timedelta(days=1)
    body = AggregatePreviewRequest(source_id=context[2],
        aggregate_rule={"features": [{"key": "n", "name": "Count", "op": "count"}]},
        schedule={"boundary": anchor, "timezone": "UTC", "duration_seconds": 1},
        window_start_utc=anchor, time_basis="event_time")
    with context[0].session_factory() as session:
        from app.aggregate_composition import build_aggregate_service
        api = build_aggregate_service(session, context[1])
        def first_read(_identity):
            clock[0] += .81
            raise EventTimeBudgetError("operation_seconds")
        api.repository.source = first_read
        with pytest.raises(DomainError) as error:
            api.preview(body)
        assert error.value.code == "execution_budget_exceeded"
        diagnostic = error.value.details["execution"]
        assert diagnostic["calculation_status"] == "uncomputed" and diagnostic["hard_budget_seconds"] == .8
        assert diagnostic["measured_through"] == "finalization"
        assert "snapshot_reference_at" not in diagnostic
        assert session.scalar(select(func.count()).select_from(AggregateClosure).where(
            AggregateClosure.source_identity == context[2])) == 0


def test_manual_cleanup_deletes_numeric_payload_only_and_preserves_terminal_diagnostic(context, monkeypatch):
    case = budget_case(context)
    clock = controlled_budget(monkeypatch)
    original = AggregateSnapshotService.build
    def exceed(*args, **kwargs):
        original(*args, **kwargs)
        clock[0] += 30.01
        args[-1].check()
    monkeypatch.setattr(AggregateSnapshotService, "build", exceed)
    with timed_service(context, case, clock) as api:
        assert api.advance(case.card.id)["calculation_status"] == "uncomputed"
    diagnostic = assert_terminal(context, case).model_dump()
    monkeypatch.setattr(AggregateSnapshotService, "build", original)
    with timed_service(context, case, clock) as api:
        assert api.advance(case.card.id)["calculation_status"] == "computed"
    with service(context, case) as api:
        plan = api.cleanup_preview(case.card.id, 0)
        assert plan.total_points == plan.planned_delete == 1  # N counts numeric payloads.
        receipt = api.aggregate_command(case.card.id, UUID(int=1), "cleanup-mixed", "cleanup", 0)
        for _ in range(4):  # memberships/outcomes/logical-clear/operation completion
            api.advance(case.card.id)
            if api.cleanup_operation(case.card.id, receipt.cleanup_operation_id).status == "succeeded":
                break
        assert api.cleanup_operation(case.card.id, receipt.cleanup_operation_id).status == "succeeded"
        assert api.point_at(case.card.id, 2).model_dump() == diagnostic
        assert api.points(case.card.id, 20, 0)["total"] == 1
        with pytest.raises(DomainError) as error:
            api.point_at(case.card.id, 3)
        assert error.value.code == "aggregate_point_cleared"


@pytest.mark.parametrize("model,field,constraint", [
    (AggregateClosure,"calculation_reason","calculation_valid"),
    (AggregateClosure,"execution","calculation_snapshot_valid"),
    (WorkingWindowPoint,"calculation_reason","calculation_valid"),
])
def test_uncomputed_storage_rejects_sql_null_reason_or_execution(context, model, field, constraint):
    """These are storage fixtures, not computed window evidence."""
    case = budget_case(context)
    now = datetime.now(UTC)
    start = case.anchor + timedelta(seconds=2 * case.duration)
    end = start + timedelta(seconds=case.duration)
    attempt = SimpleNamespace(attempt_id=uuid4(), origin="online", window_index=2,
        start_utc=start, end_utc=end, started_at=now-timedelta(seconds=31),
        deadline_at=now-timedelta(seconds=1), hard_budget_seconds=30)
    diagnostic = execution_diagnostics(attempt, now, 31, "uncomputed", "finalization")
    identity = uuid5(case.card.schedule_id, "event-time-window:2")
    if model is AggregateClosure:
        values = {"origin_point_id": identity, "aggregate_id": case.card.id, "schedule_id": case.card.schedule_id,
            "source_identity": context[2], "window_index": 2, "start_utc": start, "end_utc": end,
            "time_basis": "event_time", "origin": "online", "snapshot_reference_at": None,
            "closed_at": now, "close_not_before_at": end+timedelta(seconds=300), "payload_state": "present",
            "point_kind": "working", "calculation_status": "uncomputed",
            "calculation_reason": "execution_budget_exceeded", "execution": diagnostic}
    else:
        values = {"id": identity, "schedule_id": case.card.schedule_id, "window_index": 2,
            "start_utc": start, "end_utc": end, "scheduled_at": end, "actual_started_at": attempt.started_at,
            "actual_completed_at": now, "event_count": None, "features": None, "completeness": None,
            "completeness_reason": None, "evidence_refs": None, "time_basis": "event_time",
            "calculation_status": "uncomputed", "calculation_reason": "execution_budget_exceeded"}
    with context[0].session_factory() as session:
        with pytest.raises(IntegrityError) as error:
            session.execute(model.__table__.insert().values(**{**values, field: null()}))
        assert error.value.orig.diag.constraint_name.endswith(constraint)
        session.rollback()
