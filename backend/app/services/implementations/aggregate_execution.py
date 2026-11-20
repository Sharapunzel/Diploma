"""Bounded fair aggregate turns with generation-fenced candidate commits."""

from copy import deepcopy
from datetime import timedelta
from time import monotonic
from uuid import uuid5

from pydantic import ValidationError
from sqlalchemy.exc import DBAPIError

from ...aggregation.event_time import (
    EventTimeBudgetError,
    EventTimeRangeError,
    OperationBudget,
    bounds,
    close_deadline,
)
from ...aggregation.rules import RuleError
from ...aggregation.time_policy import (
    FINALIZATION_SECONDS,
    RecoveryTimeError,
    execution_diagnostics,
    execution_policy,
    recovery_remaining,
)
from ...aggregation.worker_admission import FOREGROUND_BURST
from ...core.errors import DomainError
from ...db_deadline import DatabaseDeadlineExceeded, database_deadline
from ...models import AggregateClosure, WorkingWindowPoint
from .aggregate_attempts import (
    AggregateAttemptService,
    CommitResolutionError,
    attempt_context,
    nontransient_database_error,
)
from .aggregate_snapshot import AggregateSnapshotService


def execution_allowed(aggregate, schedule):
    if aggregate.execution_mode == "retired":
        raise DomainError("aggregate_time_semantics_retired", "Прежний расчёт остановлен; создайте копию", 409)
    if schedule.active_cleanup_id:
        raise DomainError("cleanup_in_progress", "Очистка ещё не завершена", 409)
    return aggregate.state == "active" and not schedule.paused


class AggregateExecutionService:
    def __init__(self, repository, uow, catalog, limits):
        self.repository, self.uow, self.catalog, self.limits = repository, uow, catalog, limits
        self.current_operation_budget = None
        self.current_execution_generation = None
        self.current_window = None
        self._finalization_budget = None
        self.database_deadline_enabled = True

    def _budget(self, budget):
        self.current_operation_budget = budget
        return budget

    def _finalization_budget_once(self):
        if self._finalization_budget is None:
            self._finalization_budget = OperationBudget(FINALIZATION_SECONDS)
        return self._finalization_budget

    def _finalization(self):
        return self._budget(self._finalization_budget_once())

    def _remaining(self, attempt, reference):
        try:
            return recovery_remaining(attempt.started_at, attempt.deadline_at,
                attempt.hard_budget_seconds, reference)
        except RecoveryTimeError as error:
            raise DomainError("aggregate_recovery_required", "Не удалось восстановить время попытки", 503) from error

    def _locked(self, aggregate_id, budget):
        self.repository.read_budget(budget.milliseconds())
        aggregate = self.repository.get(aggregate_id)
        if aggregate is None:
            raise DomainError("aggregate_not_found", "Агрегат не найден", 404)
        self.repository.read_budget(budget.milliseconds())
        schedule = self.repository.schedule(aggregate.schedule_id, True)
        if self.current_execution_generation is None:
            self.current_execution_generation = schedule.execution_generation
        self.repository.read_budget(budget.milliseconds())
        self.repository.refresh(aggregate)
        return aggregate, schedule

    def _work_available(self, aggregate, schedule, budget, *, history=False):
        if aggregate.state != "active" or schedule.paused or aggregate.execution_mode != "current":
            return False
        self.repository.read_budget(budget.milliseconds())
        _, end = bounds(schedule.anchor_utc, schedule.duration_seconds, schedule.next_index)
        if self.repository.now() >= close_deadline(end, schedule.duration_seconds):
            return True
        return history and self.repository.history_candidate(aggregate.id, budget) is not None

    def _reserve_turn(self, aggregate, schedule, budget):
        if not execution_allowed(aggregate, schedule):
            turn = schedule.turn % FOREGROUND_BURST
            schedule.turn = (turn + 1) % FOREGROUND_BURST
            return "scan" if turn == 0 else "idle"
        block = FOREGROUND_BURST + 1
        phase = schedule.turn % (2 * block)
        history = phase >= block
        # Reserve attempts, not successful closures. The existing commit before
        # candidate work makes background debt survive failures and restarts.
        if phase % block < FOREGROUND_BURST and self._work_available(aggregate, schedule, budget):
            schedule.turn = phase + 1
            return "online"
        if history and phase % block < FOREGROUND_BURST:
            schedule.turn = phase + 1
        else:
            schedule.turn = 0 if history else block
        return "history" if history else "scan"

    def advance(self, aggregate_id, *, execution_started=None):
        self._finalization_budget = None
        self.database_deadline_enabled = True
        with database_deadline(lambda: self.current_operation_budget if self.database_deadline_enabled else None,
                on_deadline=self._finalization_budget_once):
            try:
                return self._advance(aggregate_id, execution_started=execution_started)
            except Exception:
                if self.database_deadline_enabled:
                    self._finalization()
                self.uow.rollback()
                raise

    def _advance(self, aggregate_id, *, execution_started=None):
        started = monotonic() if execution_started is None else execution_started
        budget = self._budget(OperationBudget(30, started=started))
        # Duration is immutable and can be read without waiting for the mutex.
        # Tighten the original job deadline before any reservation lock wait.
        self.repository.read_budget(budget.milliseconds())
        metadata = self.repository.get(aggregate_id)
        if metadata is None:
            raise DomainError("aggregate_not_found", "Агрегат не найден", 404)
        self.repository.read_budget(budget.milliseconds())
        plan = self.repository.schedule(metadata.schedule_id)
        if metadata.state == "active" and metadata.execution_mode == "current" and not plan.active_cleanup_id:
            budget = self._budget(OperationBudget(execution_policy(plan.duration_seconds)["hard_budget_seconds"], started=started))
        attempts = AggregateAttemptService(self.repository, self.uow, self._locked)
        preceding = self.repository.window_attempt(plan.id, budget)
        if preceding is not None:
            self.current_window = (preceding.schedule_id, preceding.window_index)
            self.repository.read_budget(budget.milliseconds())
            reference = self.repository.execution_now()
            remaining = self._remaining(preceding, reference)
            if remaining <= 0 or preceding.state == "finalizing":
                preceding = attempt_context(preceding)
                finalization = self._finalization()
                self.uow.rollback()
                return attempts.resolve(preceding, expired=True, budget=finalization)
            budget.deadline = min(budget.deadline, monotonic() + remaining)
        aggregate, schedule = self._locked(aggregate_id, budget)
        if schedule.active_cleanup_id:
            operation_id = schedule.active_cleanup_id
            self.database_deadline_enabled = False
            self.uow.rollback()
            from .aggregate_cleanup import AggregateCleanupService
            cleanup = AggregateCleanupService(self.repository, self.uow)
            try:
                return cleanup.advance(aggregate_id, operation_id)
            finally:
                self.current_operation_budget = cleanup.current_operation_budget
        if aggregate.execution_mode == "retired":
            self.uow.rollback()
            return {"state": "retired"}
        active = self.repository.window_attempt(schedule.id, budget)
        if active is not None:
            if not attempts.matches(aggregate, schedule, active, budget):
                self.repository.revoke_window_attempt(schedule.id, budget)
                self.uow.commit()
                return {"state": "candidate_cancelled"}
            self.repository.read_budget(budget.milliseconds())
            reference = self.repository.execution_now()
            remaining = self._remaining(active, reference)
            active_row, active = active, attempt_context(active)
            # Map the persisted UTC deadline to this process's monotonic clock;
            # never compare a previous process's monotonic origin or renew T.
            budget = self._budget(OperationBudget(active.hard_budget_seconds,
                started=monotonic() - (active.hard_budget_seconds - remaining)))
            block = FOREGROUND_BURST + 1
            if remaining > 0 and schedule.turn % (2 * block) == FOREGROUND_BURST:
                # A retry is the same reservation, not a new quota turn. Keep
                # the already-owed diagnostic scan and the attempt's deadline.
                turn = self._reserve_turn(aggregate, schedule, budget)
                assert turn == "scan"
                source, identity = aggregate.source_identity, schedule.id
                generation, after = schedule.execution_generation, schedule.scan_after
                self.uow.commit()
                return self._scan(aggregate_id, source, identity, generation, after)
            if remaining <= 0 or active.state == "finalizing":
                finalization = self._finalization()
                self.uow.rollback()
                return attempts.resolve(active, expired=True, budget=finalization)
            if active.state != "retryable":
                self.uow.rollback()
                return {"state": "attempt_running"}
            active_row.state, active.state = "running", "running"
            self.repository.read_budget(budget.milliseconds())
            budget.check()
            self.uow.commit()
            return self._calculate(active, budget, started)
        turn = self._reserve_turn(aggregate, schedule, budget)
        generation = schedule.execution_generation
        identity = schedule.id
        source = aggregate.source_identity
        after = schedule.scan_after
        if turn == "scan":
            self.uow.commit()
            return self._scan(aggregate_id, source, identity, generation, after)
        if turn == "idle":
            self.uow.commit()
            return {"state": "paused" if schedule.paused else aggregate.state}
        origin, cohort, span_key = "online", None, None
        index = schedule.next_index
        if turn == "history":
            span = self.repository.history_candidate(aggregate_id, budget)
            if span is None:
                available = self._work_available(aggregate, schedule, budget)
                self.uow.commit()
                return {"state": "history_waiting", "work_available": available}
            index, span_key = span.next_index, span.cohort_key
            cohort = {"load_start": span.load_start, "load_end": span.load_end, "coordinates": span.coordinates}
            origin = "initial_history"
        budget = self._budget(OperationBudget(execution_policy(schedule.duration_seconds)["hard_budget_seconds"], started=started))
        budget.check()
        self.repository.read_budget(budget.milliseconds())
        closed = self.repository.closure_at(identity, index)
        if closed:
            if origin == "initial_history":
                self.repository.read_budget(budget.milliseconds())
                self.repository.advance_history(aggregate_id, span_key, index)
            else:
                schedule.next_index += 1
            self.uow.commit()
            return {"state": "already_closed", "point_id": closed.origin_point_id}
        start, end = bounds(schedule.anchor_utc, schedule.duration_seconds, index)
        deadline = close_deadline(end, schedule.duration_seconds) if origin == "online" else end
        if self.repository.now() < deadline:
            available = self._work_available(aggregate, schedule, budget, history=True)
            self.uow.commit()
            return {"state": "waiting_close_deadline", "close_not_before_at": deadline, "work_available": available}
        rule, duration = deepcopy(aggregate.rule), schedule.duration_seconds
        attempt = attempts.reserve(aggregate, schedule, index, origin, span_key, start, end, budget)
        self.current_window = (attempt.schedule_id, attempt.window_index)
        self.repository.read_budget(budget.milliseconds())
        self.uow.commit()
        return self._calculate(attempt, budget, started, rule=rule, source=source, duration=duration, cohort=cohort)

    def _calculate(self, attempt, budget, started, *, rule=None, source=None, duration=None, cohort=None):
        attempts = AggregateAttemptService(self.repository, self.uow, self._locked)
        try:
            if rule is None:
                self.repository.read_budget(budget.milliseconds())
                aggregate = self.repository.get(attempt.aggregate_id)
                self.repository.read_budget(budget.milliseconds())
                schedule = self.repository.schedule(attempt.schedule_id)
                rule, source, duration = deepcopy(aggregate.rule), aggregate.source_identity, schedule.duration_seconds
                if attempt.origin == "initial_history":
                    cohort = self.repository.history_context(attempt.aggregate_id, attempt.cohort_key, budget)
                self.uow.rollback()
            candidate = AggregateSnapshotService(self.repository, self.uow, self.catalog, self.limits).build(
                source, rule, attempt.start_utc, attempt.end_utc, duration, attempt.origin, cohort, budget)
            return self._publish(attempt, candidate, budget, started)
        except CommitResolutionError as error:
            raise error.original
        except (EventTimeBudgetError, DBAPIError, DatabaseDeadlineExceeded) as error:
            finalization = self._finalization()
            self.uow.rollback()
            timed_out = (isinstance(error, DatabaseDeadlineExceeded)
                or isinstance(getattr(error, "orig", None), DatabaseDeadlineExceeded)
                or (getattr(error.orig, "sqlstate", None) in {"57014", "55P03"}
                    if isinstance(error, DBAPIError) else getattr(error, "factor", None) == "operation_seconds"))
            if budget.remaining() <= 0 and timed_out:
                return attempts.resolve(attempt, expired=True, budget=finalization)
            if isinstance(error, EventTimeBudgetError) and error.factor != "operation_seconds" or isinstance(error, DBAPIError) and nontransient_database_error(error):
                attempts.reject(attempt, finalization)
            else:
                attempts.retryable(attempt, finalization)
                if isinstance(error, EventTimeBudgetError):
                    raise DomainError("aggregate_retry_required", "Ранний отказ бюджета требует повторного чтения", 503) from error
            raise
        except (RuleError, ValidationError, EventTimeRangeError):
            finalization = self._finalization()
            self.uow.rollback()
            attempts.reject(attempt, finalization)
            raise
        except DomainError as error:
            finalization = self._finalization()
            self.uow.rollback()
            if error.code == "source_unavailable":
                attempts.reject(attempt, finalization)
            elif error.code not in {"cleanup_in_progress", "aggregate_inactive", "aggregate_deleted", "aggregate_time_semantics_retired"}:
                attempts.retryable(attempt, finalization)
            raise
        except Exception:
            finalization = self._finalization()
            self.uow.rollback()
            attempts.retryable(attempt, finalization)
            raise

    def _publish(self, attempt, candidate, budget, started):
        attempts = AggregateAttemptService(self.repository, self.uow, self._locked)
        save_started = monotonic()
        aggregate, schedule = self._locked(attempt.aggregate_id, budget)
        self.repository.read_budget(budget.milliseconds())
        saved = self.repository.closure_at(attempt.schedule_id, attempt.window_index)
        if saved:
            point_id = saved.origin_point_id
            self.uow.rollback()
            return {"state": "already_closed", "point_id": point_id}
        if schedule.active_cleanup_id:
            raise DomainError("cleanup_in_progress", "Очистка ещё не завершена", 409)
        active = self.repository.window_attempt(attempt.schedule_id, budget)
        if (active is None or active.attempt_id != attempt.attempt_id or active.state != "running"
                or not attempts.matches(aggregate, schedule, attempt, budget)):
            self.uow.rollback()
            return {"state": "candidate_cancelled"}
        point_id = uuid5(attempt.schedule_id, "event-time-window:" + str(attempt.window_index))
        self.repository.read_budget(budget.milliseconds())
        now = self.repository.now()
        close_at = close_deadline(attempt.end_utc, schedule.duration_seconds) if attempt.origin == "online" else attempt.end_utc
        if now < close_at:
            self.uow.rollback()
            return {"state": "candidate_cancelled"}
        values = {k: v for k, v in candidate["values"].items() if k != "observed_rows"}
        point = WorkingWindowPoint(id=point_id, schedule_id=attempt.schedule_id, window_index=attempt.window_index,
            start_utc=attempt.start_utc, end_utc=attempt.end_utc, scheduled_at=attempt.end_utc, actual_started_at=candidate["reference"],
            actual_completed_at=now, **values, completeness=candidate["completeness"],
            completeness_reason=candidate["completeness_reason"],
            evidence_refs=candidate["metadata"], time_basis="event_time", calculation_status="computed")
        elapsed = max(0, attempt.hard_budget_seconds - budget.remaining())
        diagnostic = execution_diagnostics(attempt, attempt.started_at + timedelta(seconds=elapsed),
            elapsed, "computed", "before_commit", candidate["timings"])
        closure = AggregateClosure(origin_point_id=point_id, aggregate_id=attempt.aggregate_id,
            schedule_id=attempt.schedule_id, source_identity=aggregate.source_identity,
            window_index=attempt.window_index, start_utc=attempt.start_utc, end_utc=attempt.end_utc,
            time_basis="event_time", origin=attempt.origin, snapshot_reference_at=candidate["reference"], closed_at=now,
            close_not_before_at=close_at,
            payload_state="present", point_kind="working", calculation_status="computed", execution=diagnostic)
        self.repository.add_event_point(point, closure, candidate["manifest"], candidate["metadata"], budget)
        attempts.clear(schedule, attempt, budget)
        aggregate.last_error, aggregate.last_error_at = None, attempt.started_at + timedelta(seconds=elapsed)
        schedule.last_execution = diagnostic
        self.repository.read_budget(budget.milliseconds())
        budget.check()
        commit_started = monotonic()
        try:
            self.uow.commit()
        except Exception as error:
            finalization = self._finalization()
            self.uow.rollback()
            try:
                resolved = attempts.resolve(attempt, expired=budget.remaining() <= 0,
                    reject_absent=nontransient_database_error(error), rollback_proven=True, budget=finalization)
            except (DBAPIError, EventTimeBudgetError, DomainError, DatabaseDeadlineExceeded) as resolution_error:
                raise CommitResolutionError(resolution_error) from error
            if resolved is not None:
                return resolved
            raise CommitResolutionError(error) from error
        committed = monotonic()
        # A commit started within T remains a success after a late ACK.
        return {"state": "closed", "point_id": point_id, "origin": attempt.origin,
            "calculation_status": "computed", "execution": diagnostic,
            "timings": {**candidate["timings"], "save": commit_started - save_started,
                "commit": committed - commit_started, "total": committed - started}}

    def _scan(self, aggregate_id, source_id, schedule_id, generation, after):
        self.database_deadline_enabled = False
        budget = self._budget(OperationBudget(1))
        with self.uow.read_snapshot():
            self.repository.read_budget(budget.milliseconds())
            reference = self.repository.now()
            page = self.repository.scan_page(source_id, after, reference, budget)
        aggregate, schedule = self._locked(aggregate_id, budget)
        if (schedule.id != schedule_id or schedule.execution_generation != generation
                or schedule.active_cleanup_id or schedule.scan_after != after):
            self.uow.rollback()
            return {"state": "scan_cancelled"}
        self.repository.save_scan_page(aggregate, schedule, page, reference, budget)
        available = self._work_available(aggregate, schedule, budget, history=True)
        self.repository.read_budget(budget.milliseconds())
        self.uow.commit()
        return {"state": "scanned", "records": len(page), "work_available": available}
