"""Automatic model-history maintenance, independent of manual cleanup control."""

from datetime import timedelta

from sqlalchemy.exc import DBAPIError

from ...aggregation.event_time import EventTimeBudgetError, OperationBudget
from ...aggregation.retention import CHECK_SECONDS, POLICY_VERSION, protection_plan


def lifecycle(aggregate, schedule):
    return (aggregate.id, aggregate.schedule_id, aggregate.state, aggregate.execution_mode,
        aggregate.semantic_basis, schedule.paused, schedule.active_cleanup_id, schedule.execution_generation)


def inactive_reason(aggregate, schedule):
    if schedule.active_cleanup_id:
        return "cleanup_in_progress"
    if aggregate.execution_mode != "current" or aggregate.semantic_basis != "event_time":
        return "aggregate_time_semantics_retired"
    if aggregate.state != "active":
        return "aggregate_inactive"
    if schedule.paused:
        return "aggregate_paused"
    return None


class AggregateRetentionService:
    def __init__(self, repository, uow):
        self.repository, self.uow = repository, uow

    def due(self, limit=100, exclude_ids=()):
        try:
            return self.repository.maintenance_due(limit, exclude_ids)
        finally:
            self.uow.rollback()

    def _target(self, aggregate_id, budget, lock=False):
        self.repository.maintenance_budget(budget)
        aggregate = self.repository.get(aggregate_id)
        if aggregate is None:
            return None, None
        self.repository.maintenance_budget(budget)
        schedule = self.repository.schedule(aggregate.schedule_id, lock)
        if lock:
            self.repository.maintenance_budget(budget)
            self.repository.refresh(aggregate)
        return aggregate, schedule

    @staticmethod
    def _result(state, status=None, reason=None):
        return {"status": status or (state.status if state else "error"),
            "reason": reason if reason is not None else state.reason if state else "retention_state_missing",
            "next_check_at": state.next_check_at if state else None,
            "pending": state.pending_closure_id is not None if state else False}

    def _save(self, state, budget):
        result = self._result(state)
        self.repository.maintenance_budget(budget)
        self.uow.commit()
        return result

    def maintain(self, aggregate_id):
        # Reserve the last fraction for a bounded diagnostic transaction. Both
        # budgets share the original entry time, not a renewed per-portion clock.
        budget, work = OperationBudget(1), OperationBudget(.85)
        transactions = 0
        try:
            transactions += 1
            with self.uow.read_snapshot():
                aggregate, schedule = self._target(aggregate_id, work)
                if aggregate is None:
                    return {"status": "inactive", "reason": "aggregate_inactive", "next_check_at": None, "pending": False}
                state = self.repository.retention_state(aggregate_id, work)
                if state is None:
                    return self._result(None)
                if state.policy_version != POLICY_VERSION:
                    raise ValueError("retention_policy_unavailable")
                pending = state.pending_closure_id
                expected = lifecycle(aggregate, schedule)
                expected_pending = (pending, state.pending_phase, state.pending_cursor)
                reason = inactive_reason(aggregate, schedule)
                plan, headers, candidate_id = None, None, None
                protection_error = False
                if pending is None and reason is None:
                    try:
                        detectors, attempts = self.repository.retention_dependencies(aggregate_id, work)
                        headers = self.repository.retention_headers(aggregate_id, work)
                        last = self.repository.last_closed_index(schedule.id, work)
                        self.repository.maintenance_budget(work)
                        present = self.repository.canonical_last_index(schedule.id)
                        stored_schedule = {key: getattr(schedule, key) for key in
                            ("anchor_utc", "duration_seconds", "online_start_index")}
                        plan = protection_plan(stored_schedule, detectors, attempts, last, present, work.check)
                        row = self.repository.retention_candidate(aggregate_id, plan["keep_from_index"], work)
                        candidate_id = row.origin_point_id if row else None
                    except (KeyError, TypeError, ValueError, ArithmeticError):
                        protection_error = True
                # ORM objects belong to this closed RR; keep only immutable
                # scalar snapshots across the subsequent clean RC transaction.
            transactions += 1
            aggregate, schedule = self._target(aggregate_id, work, True)
            if aggregate is None:
                self.uow.rollback()
                return {"status": "inactive", "reason": "aggregate_inactive", "next_check_at": None, "pending": False}
            if headers is not None:
                actual = self.repository.retention_headers(aggregate_id, work, True)
            else:
                actual = None
            state = self.repository.retention_state(aggregate_id, work, True)
            if state is None:
                self.uow.rollback()
                return self._result(None)
            now = self.repository.now()
            if (lifecycle(aggregate, schedule) != expected or actual != headers
                    or (state.pending_closure_id, state.pending_phase, state.pending_cursor) != expected_pending):
                state.status, state.reason, state.next_check_at = "deferred", "dependencies_changed", now
                return self._save(state, work)
            if pending is not None:
                # Already-cleared physical work is safe even after pause/archive/
                # retirement; an active manual fence owns the target for now.
                if schedule.active_cleanup_id:
                    state.status, state.reason = "deferred", "cleanup_in_progress"
                    state.next_check_at = now + timedelta(seconds=CHECK_SECONDS)
                    return self._save(state, work)
                self._compact(aggregate_id, state, work)
                return self._save(state, work)
            state.last_checked_at, state.retry_count = now, 0
            state.authorization_txid = None
            if reason is not None:
                state.status = "deferred" if reason == "cleanup_in_progress" else "inactive"
                state.reason, state.next_check_at = reason, now + timedelta(seconds=CHECK_SECONDS)
                return self._save(state, work)
            if protection_error:
                state.status, state.reason, state.next_check_at = "protected", "protection_unavailable", now + timedelta(seconds=CHECK_SECONDS)
                state.reserve_windows = state.keep_from_index = state.protected_from_index = state.protected_total = None
                state.protected_by = []
                return self._save(state, work)
            for key, value in plan.items():
                setattr(state, key, value)
            if candidate_id is None:
                state.status = "protected" if plan["protected_total"] else "idle"
                state.reason = "unconsumed_points" if plan["protected_total"] else "model_history_reserve"
                state.next_check_at = now + timedelta(seconds=CHECK_SECONDS)
                return self._save(state, work)
            closure = self.repository.retention_candidate(aggregate_id, plan["keep_from_index"], work, candidate_id, True)
            if closure is None:
                state.status, state.reason, state.next_check_at = "deferred", "dependencies_changed", now
                return self._save(state, work)
            state.pending_closure_id, state.pending_cursor = closure.origin_point_id, None
            self.repository.retention_authorize(state, "claim", work)
            if not self.repository.clear_payload(closure, work, automatic=True, state=state):
                raise ValueError("retention_claim_lost")
            state.status, state.reason = "compacting", None
            state.pending_phase, state.authorization_txid = "memberships", None
            state.next_check_at = now
            result = self._save(state, work)
            # At most one additional short transaction this turn; reserve one
            # transaction for error/backoff and stay below four including RR.
            if work.milliseconds() >= 100:
                transactions += 1
                aggregate, schedule = self._target(aggregate_id, work, True)
                state = self.repository.retention_state(aggregate_id, work, True)
                if schedule.active_cleanup_id:
                    self.uow.rollback()
                    return result
                self._compact(aggregate_id, state, work)
                return self._save(state, work)
            return result
        except Exception as error:  # noqa: BLE001 - maintenance owns a separate safe diagnostic/backoff
            self.uow.rollback()
            code = ("retention_budget" if isinstance(error, EventTimeBudgetError) else
                "retention_database_error" if isinstance(error, DBAPIError) else "retention_turn_failed")
            if transactions >= 4:
                return {"status": "deferred", "reason": code, "next_check_at": None, "pending": True}
            try:
                # Failed/unknown commit is resolved from durable state, never by
                # replaying an in-memory counter or selecting a different point.
                aggregate, _schedule = self._target(aggregate_id, budget, True)
                state = self.repository.retention_state(aggregate_id, budget, True) if aggregate else None
                if state is None:
                    self.uow.rollback()
                    return self._result(None)
                state.retry_count += 1
                state.status, state.reason = "error", code
                state.next_check_at = self.repository.now() + timedelta(seconds=min(60, 2 ** min(state.retry_count - 1, 6)))
                state.authorization_txid = None
                return self._save(state, budget)
            except Exception:  # noqa: BLE001 - an unconfirmed diagnostic never becomes a foreground error
                self.uow.rollback()
                return {"status": "deferred", "reason": code, "next_check_at": None, "pending": True}

    def _compact(self, aggregate_id, state, budget):
        closure = self.repository.retention_pending_closure(aggregate_id, state.pending_closure_id, budget)
        if closure is None:
            raise ValueError("retention_pending_invalid")
        if closure.payload_state == "present":
            # Resolve an interrupted/old claim from durable truth. No payload or
            # counter changes here: the next turn must take a fresh all-model RR
            # protection plan before it can attempt a new logical clear.
            state.pending_closure_id = state.pending_phase = state.pending_cursor = state.authorization_txid = None
            state.status, state.reason, state.next_check_at = "deferred", "dependencies_changed", self.repository.now()
            return
        if closure.payload_state != "cleared" or state.pending_phase not in {"memberships", "outcomes"}:
            raise ValueError("retention_pending_phase_invalid")
        membership_changes = False
        if state.pending_phase == "memberships":
            self.repository.retention_authorize(state, "memberships", budget)
            portion = self.repository.compact_memberships(closure.origin_point_id, budget, automatic=True)
            state.pending_cursor = portion.cursor
            membership_changes = portion.changed_count > 0
            if portion.cursor is None:
                state.pending_phase, state.pending_cursor = "outcomes", None
        # Commit a completed membership mutation and its phase together. An
        # optional outcomes statement must not spend the remainder and roll
        # that progress back. Empty membership keeps the old immediate path.
        if state.pending_phase == "outcomes" and not membership_changes:
            self.repository.retention_authorize(state, "outcomes", budget)
            cursor = self.repository.compact_outcomes(closure.origin_point_id, budget, automatic=True)
            state.pending_cursor = cursor
            if cursor is None:
                state.pending_closure_id = state.pending_phase = state.pending_cursor = None
        state.authorization_txid = None
        state.status, state.reason, state.retry_count = "compacting", None, 0
        state.next_check_at = self.repository.now()
        if state.pending_closure_id is None:
            state.status = "scheduled"
