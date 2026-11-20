"""Reservation and bounded terminal resolution, without partial candidate state."""

from datetime import timedelta
from time import monotonic
from types import SimpleNamespace
from uuid import uuid4, uuid5

from ...aggregation.event_time import bounds
from ...aggregation.time_policy import (
    RecoveryTimeError,
    execution_diagnostics,
    execution_policy,
    recovery_remaining,
)
from ...core.errors import DomainError
from ...models import AggregateClosure, AggregateWindowAttempt, WorkingWindowPoint


def attempt_context(attempt):
    # ORM rollback expires attributes; recovery must never implicitly reload a
    # context outside its explicit bounded locked read.
    return SimpleNamespace(**{column.key: getattr(attempt, column.key)
        for column in AggregateWindowAttempt.__table__.columns})


class CommitResolutionError(Exception):
    def __init__(self, original):
        self.original = original
        super().__init__("aggregate_commit_unconfirmed")


def nontransient_database_error(error):
    code = getattr(getattr(error, "orig", None), "sqlstate", None)
    return isinstance(code, str) and (code.startswith("22") or code == "23514")


class AggregateAttemptService:
    def __init__(self, repository, uow, locked):
        self.repository, self.uow, self.locked = repository, uow, locked

    @staticmethod
    def _check_time(attempt, reference, expired):
        try:
            remaining = recovery_remaining(attempt.started_at, attempt.deadline_at,
                attempt.hard_budget_seconds, reference)
            if expired and remaining > .000001:
                raise RecoveryTimeError("execution_recovery_time")
            return remaining
        except RecoveryTimeError as error:
            raise DomainError("aggregate_recovery_required", "Не удалось восстановить время попытки", 503) from error

    def _send_commit(self):
        try:
            self.uow.commit()
        except Exception as error:
            raise CommitResolutionError(error) from error

    def reserve(self, aggregate, schedule, index, origin, cohort_key, start, end, budget):
        self.repository.read_budget(budget.milliseconds())
        reference = self.repository.execution_now()
        elapsed = monotonic() - budget.started
        started_at = reference - timedelta(seconds=elapsed)
        attempt = AggregateWindowAttempt(schedule_id=schedule.id, aggregate_id=aggregate.id,
            attempt_id=uuid4(), execution_generation=schedule.execution_generation,
            window_index=index, origin=origin, cohort_key=cohort_key, start_utc=start, end_utc=end,
            started_at=started_at, deadline_at=started_at + timedelta(seconds=budget.seconds),
            hard_budget_seconds=budget.seconds, state="running")
        self.repository.add_window_attempt(attempt, budget)
        return attempt_context(attempt)

    def matches(self, aggregate, schedule, attempt, budget):
        allowed = (aggregate.state == "active" and aggregate.execution_mode == "current"
            and not schedule.paused and schedule.active_cleanup_id is None
            and schedule.execution_generation == attempt.execution_generation
            and (attempt.origin != "online" or schedule.next_index == attempt.window_index))
        if allowed and (attempt.hard_budget_seconds != execution_policy(schedule.duration_seconds)["hard_budget_seconds"]
                or bounds(schedule.anchor_utc, schedule.duration_seconds, attempt.window_index)
                    != (attempt.start_utc, attempt.end_utc)):
            raise DomainError("aggregate_recovery_required", "Контекст попытки несовместим с расписанием", 503)
        if not allowed or attempt.origin != "initial_history":
            return allowed
        self.repository.read_budget(budget.milliseconds())
        return self.repository.history_matches(aggregate.id, attempt.cohort_key, attempt.window_index)

    def clear(self, schedule, attempt, budget):
        if attempt.origin == "online":
            schedule.next_index += 1
        else:
            self.repository.read_budget(budget.milliseconds())
            self.repository.advance_history(attempt.aggregate_id, attempt.cohort_key, attempt.window_index)
        self.repository.revoke_window_attempt(schedule.id, budget)

    def reject(self, attempt, budget):
        """A known non-time rejection ended computation; it is never a timeout."""
        _, schedule = self.locked(attempt.aggregate_id, budget)
        active = self.repository.window_attempt(schedule.id, budget)
        if active is not None and active.attempt_id == attempt.attempt_id:
            self.repository.revoke_window_attempt(schedule.id, budget)
            self.repository.read_budget(budget.milliseconds())
            self.uow.commit()
        else:
            self.uow.rollback()

    def retryable(self, attempt, budget):
        """Only the caller that confirmed its own rollback can grant this proof."""
        aggregate, schedule = self.locked(attempt.aggregate_id, budget)
        active = self.repository.window_attempt(schedule.id, budget)
        if (active is not None and active.attempt_id == attempt.attempt_id and active.state == "running"
                and self.matches(aggregate, schedule, attempt, budget)):
            active.state = "retryable"
            self.repository.read_budget(budget.milliseconds())
            budget.check()
            self.uow.commit()
        else:
            self.uow.rollback()

    def resolve(self, attempt, *, budget, expired=False, reject_absent=False, rollback_proven=False):
        """A fresh locked read resolves an ambiguous commit before any failure write.

        The caller has stopped candidate work and rolled back/released its old
        transaction. Contention consumes at most this separate service budget.
        """
        aggregate, schedule = self.locked(attempt.aggregate_id, budget)
        self.repository.read_budget(budget.milliseconds())
        saved = self.repository.closure_at(attempt.schedule_id, attempt.window_index)
        if saved is not None:
            result = {"state": "closed", "point_id": saved.origin_point_id, "origin": saved.origin,
                "calculation_status": saved.calculation_status, "execution": saved.execution}
            self.uow.rollback()
            return result
        active = self.repository.window_attempt(schedule.id, budget)
        if active is None or active.attempt_id != attempt.attempt_id or not self.matches(aggregate, schedule, attempt, budget):
            self.uow.rollback()
            return {"state": "candidate_cancelled"}
        if reject_absent:
            self.repository.revoke_window_attempt(schedule.id, budget)
            self.repository.read_budget(budget.milliseconds())
            budget.check()
            self.uow.commit()
            return None
        self.repository.read_budget(budget.milliseconds())
        now = self.repository.execution_now()
        self._check_time(active, now, expired or active.state == "finalizing")
        if not expired and now < active.deadline_at:
            # Proved absent, but still within the original deadline. A caller
            # may report its transient error; no fresh execution allowance.
            if rollback_proven and active.state == "running":
                active.state = "retryable"
                self.repository.read_budget(budget.milliseconds())
                budget.check()
                self.uow.commit()
            else:
                self.uow.rollback()
            return None
        if active.state != "finalizing":
            active.state = "finalizing"
            self.repository.read_budget(budget.milliseconds())
            budget.check()
            marker_error = None
            try:
                self._send_commit()
            except CommitResolutionError as error:
                marker_error = error.original
                self.uow.rollback()
            # The pending-finalization marker is durable. Releasing the lock
            # also lets lifecycle cancellation proceed before the failure write.
            aggregate, schedule = self.locked(attempt.aggregate_id, budget)
            self.repository.read_budget(budget.milliseconds())
            saved = self.repository.closure_at(attempt.schedule_id, attempt.window_index)
            if saved is not None:
                result = {"state": "closed", "point_id": saved.origin_point_id, "origin": saved.origin,
                    "calculation_status": saved.calculation_status, "execution": saved.execution}
                self.uow.rollback()
                return result
            active = self.repository.window_attempt(schedule.id, budget)
            if active is None or active.attempt_id != attempt.attempt_id or not self.matches(aggregate, schedule, active, budget):
                self.uow.rollback()
                return {"state": "candidate_cancelled"}
            if marker_error is not None and active.state != "finalizing":
                self.uow.rollback()
                raise marker_error
            self.repository.read_budget(budget.milliseconds())
            now = self.repository.execution_now()
            self._check_time(active, now, True)
        point_id = uuid5(attempt.schedule_id, "event-time-window:" + str(attempt.window_index))
        diagnostics = execution_diagnostics(attempt, now, (now - attempt.started_at).total_seconds(),
            "uncomputed", "finalization")
        point = WorkingWindowPoint(id=point_id, schedule_id=attempt.schedule_id,
            window_index=attempt.window_index, start_utc=attempt.start_utc, end_utc=attempt.end_utc,
            scheduled_at=attempt.end_utc, actual_started_at=attempt.started_at, actual_completed_at=now,
            event_count=None, event_time_min=None, event_time_max=None, features=None,
            completeness=None, completeness_reason=None, evidence_refs=None, time_basis="event_time",
            calculation_status="uncomputed", calculation_reason="execution_budget_exceeded")
        from ...aggregation.event_time import close_deadline
        closure = AggregateClosure(origin_point_id=point_id, aggregate_id=attempt.aggregate_id,
            schedule_id=attempt.schedule_id, source_identity=aggregate.source_identity,
            window_index=attempt.window_index, start_utc=attempt.start_utc, end_utc=attempt.end_utc,
            time_basis="event_time", origin=attempt.origin, snapshot_reference_at=None, closed_at=now,
            close_not_before_at=close_deadline(attempt.end_utc, schedule.duration_seconds)
                if attempt.origin == "online" else attempt.end_utc,
            payload_state="present", point_kind="working", calculation_status="uncomputed",
            calculation_reason="execution_budget_exceeded", execution=diagnostics)
        self.repository.add_event_point(point, closure, (), None, budget)
        self.clear(schedule, attempt, budget)
        aggregate.last_error, aggregate.last_error_at = None, now
        schedule.last_execution = diagnostics
        self.repository.read_budget(budget.milliseconds())
        budget.check()
        result = {"state": "closed", "point_id": point_id, "origin": attempt.origin,
            "calculation_status": "uncomputed", "execution": diagnostics}
        try:
            self.uow.commit()
        except Exception:
            self.uow.rollback()
            aggregate, schedule = self.locked(attempt.aggregate_id, budget)
            self.repository.read_budget(budget.milliseconds())
            saved = self.repository.closure_at(attempt.schedule_id, attempt.window_index)
            if saved is not None:
                resolved = {"state": "closed", "point_id": saved.origin_point_id, "origin": saved.origin,
                    "calculation_status": saved.calculation_status, "execution": saved.execution}
                self.uow.rollback()
                return resolved
            self.uow.rollback()
            raise
        return result
