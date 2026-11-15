"""Bounded fair aggregate turns with generation-fenced candidate commits."""

from copy import deepcopy
from uuid import uuid5

from ...aggregation.event_time import OperationBudget, bounds, close_deadline
from ...core.errors import DomainError
from ...models import AggregateClosure, WorkingWindowPoint
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

    def _locked(self, aggregate_id, budget):
        self.repository.read_budget(budget.milliseconds())
        aggregate = self.repository.get(aggregate_id)
        if aggregate is None:
            raise DomainError("aggregate_not_found", "Агрегат не найден", 404)
        self.repository.read_budget(budget.milliseconds())
        schedule = self.repository.schedule(aggregate.schedule_id, True)
        self.repository.refresh(aggregate)
        return aggregate, schedule

    def advance(self, aggregate_id):
        budget = OperationBudget(30)
        aggregate, schedule = self._locked(aggregate_id, budget)
        if schedule.active_cleanup_id:
            operation_id = schedule.active_cleanup_id
            self.uow.rollback()
            from .aggregate_cleanup import AggregateCleanupService
            return AggregateCleanupService(self.repository, self.uow).advance(aggregate_id, operation_id)
        if aggregate.execution_mode == "retired":
            self.uow.rollback()
            return {"state": "retired"}
        turn = schedule.turn
        schedule.turn = (turn + 1) % 3
        generation = schedule.execution_generation
        identity = schedule.id
        source = aggregate.source_identity
        after = schedule.scan_after
        if turn == 0:
            self.uow.commit()
            return self._scan(aggregate_id, source, identity, generation, after)
        if not execution_allowed(aggregate, schedule):
            self.uow.commit()
            return {"state": "paused" if schedule.paused else aggregate.state}
        origin, cohort, span_key = "online", None, None
        index = schedule.next_index
        if turn == 1:
            span = self.repository.history_candidate(aggregate_id, budget)
            if span is None:
                self.uow.commit()
                return {"state": "history_waiting"}
            index, span_key = span.next_index, span.cohort_key
            cohort = {"load_start": span.load_start, "load_end": span.load_end, "coordinates": span.coordinates}
            origin = "initial_history"
        closed = self.repository.closure_at(identity, index)
        if closed:
            if origin == "initial_history":
                self.repository.advance_history(aggregate_id, span_key, index)
            else:
                schedule.next_index += 1
            self.uow.commit()
            return {"state": "already_closed", "point_id": closed.origin_point_id}
        start, end = bounds(schedule.anchor_utc, schedule.duration_seconds, index)
        deadline = close_deadline(end, schedule.duration_seconds) if origin == "online" else end
        if self.repository.now() < deadline:
            self.uow.commit()
            return {"state": "waiting_close_deadline", "close_not_before_at": deadline}
        rule, duration = deepcopy(aggregate.rule), schedule.duration_seconds
        self.uow.commit()
        candidate = AggregateSnapshotService(self.repository, self.uow, self.catalog, self.limits).build(
            source, rule, start, end, duration, origin, cohort, budget)
        aggregate, schedule = self._locked(aggregate_id, budget)
        saved = self.repository.closure_at(identity, index)
        if saved:
            self.uow.rollback()
            return {"state": "already_closed", "point_id": saved.origin_point_id}
        if (not execution_allowed(aggregate, schedule) or schedule.execution_generation != generation
                or origin == "online" and schedule.next_index != index
                or origin == "initial_history" and not self.repository.history_matches(aggregate_id, span_key, index)
                or self.repository.now() < deadline):
            self.uow.rollback()
            return {"state": "candidate_cancelled"}
        point_id = uuid5(identity, "event-time-window:" + str(index))
        now = self.repository.now()
        values = {k: v for k, v in candidate["values"].items() if k != "observed_rows"}
        point = WorkingWindowPoint(id=point_id, schedule_id=identity, window_index=index,
            start_utc=start, end_utc=end, scheduled_at=end, actual_started_at=candidate["reference"],
            actual_completed_at=now, **values, completeness=candidate["completeness"],
            completeness_reason=candidate["completeness_reason"],
            evidence_refs=candidate["metadata"], time_basis="event_time")
        closure = AggregateClosure(origin_point_id=point_id, aggregate_id=aggregate_id,
            schedule_id=identity, source_identity=source, window_index=index, start_utc=start, end_utc=end,
            time_basis="event_time", origin=origin, snapshot_reference_at=candidate["reference"], closed_at=now,
            close_not_before_at=deadline, payload_state="present", point_kind="working")
        self.repository.add_event_point(point, closure, candidate["manifest"], candidate["metadata"], budget)
        if origin == "online":
            schedule.next_index += 1
        else:
            self.repository.advance_history(aggregate_id, span_key, index)
        self.repository.read_budget(budget.milliseconds())
        self.uow.commit()
        budget.check()
        return {"state": "closed", "point_id": point_id, "origin": origin}

    def _scan(self, aggregate_id, source_id, schedule_id, generation, after):
        budget = OperationBudget(1)
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
        self.repository.read_budget(budget.milliseconds())
        self.uow.commit()
        return {"state": "scanned", "records": len(page)}
