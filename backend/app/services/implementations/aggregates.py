"""Aggregate application scenarios, shared by HTTP and the permanent worker."""

import hashlib
import json
from datetime import UTC, timedelta
from functools import wraps
from uuid import uuid4

from sqlalchemy.exc import DBAPIError

from ...aggregation.budget import estimate
from ...aggregation.event_time import (
    EventTimeBudgetError,
    EventTimeRangeError,
    OperationBudget,
    bounds,
    close_deadline,
    index_at,
    online_start,
)
from ...aggregation.field_catalog import FieldCatalogLimits
from ...aggregation.rules import (
    OPERATORS,
    Limits,
    RuleError,
    compatible,
    compile_rule,
    field_support_reason,
    operator_catalog,
)
from ...core.errors import DomainError
from ...models import Aggregate, AggregateSchedule, HistoricalAggregatePoint
from ...repositories.protocols import UnitOfWork
from ...repositories.protocols.aggregates import AggregateRepository
from ...schemas.aggregates import (
    AggregateCard,
    AggregatePreviewRequest,
    PointCard,
    PreviewResult,
    ScheduleInput,
    ValidateRequest,
    ValidationResult,
)
from .aggregate_execution import AggregateExecutionService
from .aggregate_field_catalog import AggregateFieldCatalogService
from .aggregate_snapshot import AggregateSnapshotService
from .window_schedule import WindowSchedule


def guarded(method):
    @wraps(method)
    def call(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except RuleError as error:
            self.uow.rollback()
            raise DomainError(error.code, "Правило агрегата не прошло проверку", 422,
                              {"path": error.path, "hint": error.hint}) from error
        except EventTimeRangeError as error:
            self.uow.rollback()
            raise DomainError("schedule_datetime_range", "Границы или срок закрытия выходят за допустимый диапазон дат", 422) from error
        except EventTimeBudgetError as error:
            self.uow.rollback()
            raise DomainError("resource_budget_exceeded", "Уменьшите окно или сложность правила", 422,
                {"factor": error.factor, "hint": "Уменьшите объём событий или сложность конфигурации"}) from error
        except DBAPIError as error:
            self.uow.rollback()
            code = getattr(error.orig, "sqlstate", None)
            safe = {"57014": "aggregate_timeout", "40001": "aggregate_retry_required",
                    "40P01": "aggregate_retry_required"}.get(code, "aggregate_database_error")
            raise DomainError(safe, "Расчёт не завершён; повторите запрос или уменьшите стоимость", 503) from error
        except Exception:
            self.uow.rollback()
            raise
    return call


def future_index(schedule, now):
    step = timedelta(seconds=schedule.duration_seconds)
    delta = now - schedule.anchor_utc
    if delta <= timedelta(0):
        return 0
    quotient, remainder = divmod(delta, step)
    return quotient + int(bool(remainder))


class AggregateService:
    def __init__(self, repository: AggregateRepository, windows, evidence, capture,
                 uow: UnitOfWork, catalog, limits: Limits = Limits(), *, detectors,
                 field_catalog_limits: FieldCatalogLimits = FieldCatalogLimits()):
        self.repository, self.windows, self.evidence = repository, windows, evidence
        self.capture, self.uow, self.catalog, self.budget_limits = capture, uow, catalog, limits
        self.detectors = detectors
        self.field_catalog_limits = field_catalog_limits

    def _source(self, source_id):
        source = self.repository.source(source_id)
        if source is None:
            raise DomainError("source_not_found", "Источник не найден", 404)
        if source.source_type != "kafka":
            raise DomainError("own_source_required", "Агрегат доступен только для собственного ECS-источника", 422)
        return source

    def _get(self, aggregate_id, lock=False, active=False):
        aggregate = self.repository.get(aggregate_id)
        if aggregate is None:
            raise DomainError("aggregate_not_found", "Агрегат не найден", 404)
        schedule = self.repository.schedule(aggregate.schedule_id, lock)
        if lock:
            self.repository.refresh(aggregate)
        if active and aggregate.state != "active":
            raise DomainError("aggregate_inactive", "Агрегат не принимает новые расчёты", 409)
        return aggregate, schedule

    def _window(self, schedule, start, latest):
        stored = WindowSchedule(schedule.anchor_utc, schedule.duration_seconds)
        start = start.astimezone(UTC)
        try:
            index = stored.index_at(start)
        except ValueError as error:
            raise DomainError("window_before_anchor", "Окно находится до UTC-якоря", 422) from error
        window = stored.window(index)
        if start != window.start:
            raise DomainError("window_not_aligned", "Начало окна должно совпадать с границей расписания", 422)
        if window.end > latest:
            raise DomainError("window_not_completed", "Доступно только завершённое окно", 422)
        return window

    def _cost(self, compiled, source_id, start, end, basis, duration, preview):
        self.repository.limits(self.budget_limits, preview)
        rows, total, width = self.repository.estimate_rows(source_id, start, end, basis)
        return estimate(compiled, rows, total, duration, self.budget_limits, preview, width)

    def _validate(self, body: ValidateRequest):
        self._source(body.source_id)
        compiled = compile_rule(body.aggregate_rule, self.catalog, self.budget_limits)
        now = self.repository.now()
        budget = self._cost(compiled, body.source_id,
                            now - timedelta(seconds=body.schedule.duration_seconds), now,
                            "event_time", body.schedule.duration_seconds, False)
        return ValidationResult(rule_hash=compiled.sha256, anchor_utc=body.schedule.anchor_utc,
            duration_seconds=body.schedule.duration_seconds,
            feature_keys=[f.key for f in compiled.rule.features], budget=budget)

    @guarded
    def validate(self, body: ValidateRequest):
        return self._validate(body)

    def _preview(self, body):
        if body.time_basis != "event_time":
            raise DomainError("legacy_calculation_removed", "Укажите time_basis=event_time для нового расчёта", 422)
        budget = OperationBudget(10)
        self.repository.read_budget(budget.milliseconds())
        self._source(body.source_id)
        schedule = WindowSchedule(body.schedule.anchor_utc, body.schedule.duration_seconds)
        start = body.window_start_utc.astimezone(UTC)
        index = index_at(schedule.anchor_utc, schedule.duration_seconds, start)
        expected = schedule.anchor_utc + timedelta(seconds=index * schedule.duration_seconds)
        end = start + timedelta(seconds=schedule.duration_seconds)
        if start != expected:
            raise DomainError("window_not_aligned", "Начало окна должно совпадать с границей расписания", 422)
        if end > self.repository.now():
            raise DomainError("window_not_completed", "Доступно только завершённое окно", 422)
        candidate = AggregateSnapshotService(self.repository, self.uow, self.catalog, self.budget_limits).build(
            body.source_id, body.aggregate_rule.model_dump(exclude_none=True), start, end,
            schedule.duration_seconds, "preview", None, budget, True)
        return PreviewResult(rule_hash=candidate["rule_hash"], start_utc=start, end_utc=end,
            scheduled_at=end, **candidate["values"], completeness=candidate["completeness"],
            completeness_reason=candidate["completeness_reason"], time_basis="event_time",
            budget=candidate["budget"], snapshot_reference_at=candidate["reference"],
            close_not_before_at=close_deadline(end, schedule.duration_seconds),
            in_process=index >= 0, snapshot_metadata=candidate["metadata"])

    @guarded
    def preview(self, body: AggregatePreviewRequest):
        return self._preview(body)

    @guarded
    def create(self, body, actor, key):
        self.repository.limits(self.budget_limits, True)
        if not key or len(key) > 200:
            raise DomainError("idempotency_key_required", "Укажите Idempotency-Key до 200 символов", 422)
        request_document = body.model_dump(mode="json")
        if body.time_basis is None:
            request_document["time_basis"] = "availability_at"
        # Preserve pre-extension idempotency digests as well as rule hashes.
        # Other legacy nullable fields must retain their original serialization.
        for feature in [*request_document["aggregate_rule"]["features"],
                        *request_document["aggregate_rule"].get("auxiliary", [])]:
            if feature.get("percentile") is None:
                feature.pop("percentile", None)
        digest = hashlib.sha256(json.dumps(request_document, ensure_ascii=False,
            sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        scoped_key = str(actor) + ":" + key
        self.repository.idempotency_lock(scoped_key)
        previous = self.repository.by_key(scoped_key)
        if previous:
            if previous.request_hash != digest:
                raise DomainError("idempotency_conflict", "Ключ уже использован для другого запроса", 409)
            return self.card(previous.id)
        if body.time_basis != "event_time":
            raise DomainError("legacy_calculation_removed", "Новый агрегат требует time_basis=event_time", 422)
        # Preserve exact replay above; reject new unrepresentable schedules.
        first = online_start(body.schedule.anchor_utc, body.schedule.duration_seconds, self.repository.now())
        _, first_end = bounds(body.schedule.anchor_utc, body.schedule.duration_seconds, first)
        close_deadline(first_end, body.schedule.duration_seconds)
        self._validate(body)
        preview = self._preview(body)
        if preview.rule_hash != body.rule_hash:
            raise DomainError("rule_hash_mismatch", "Хэш не совпадает с проверенным правилом", 409)
        # The RR preview deliberately released the first key lock. Reacquire it
        # and resolve a concurrent create before the final atomic write.
        self.repository.idempotency_lock(scoped_key)
        previous = self.repository.by_key(scoped_key)
        if previous:
            if previous.request_hash != digest:
                raise DomainError("idempotency_conflict", "Ключ уже использован для другого запроса", 409)
            return self.card(previous.id)
        if not self.windows.lock_source(body.source_id):
            raise DomainError("source_unavailable", "Источник удалён или больше не является собственным", 409)
        now = self.repository.now()
        source = self._source(body.source_id)
        schedule = AggregateSchedule(id=uuid4(), source_identity=source.id,
            anchor_utc=body.schedule.anchor_utc, duration_seconds=body.schedule.duration_seconds,
            semantic_basis="event_time", bootstrap_at=now, paused=False,
            online_start_index=online_start(body.schedule.anchor_utc, body.schedule.duration_seconds, now),
            execution_generation=0, scan_cycle=0, turn=0)
        _, first_end = bounds(schedule.anchor_utc, schedule.duration_seconds, schedule.online_start_index)
        close_deadline(first_end, schedule.duration_seconds)
        schedule.next_index = schedule.online_start_index
        aggregate = Aggregate(id=uuid4(), schedule_id=schedule.id, name=body.name,
            source_identity=source.id, source_name=source.name, source_type=source.source_type,
            rule=body.aggregate_rule.model_dump(exclude_none=True), rule_hash=preview.rule_hash,
            original_boundary=body.schedule.boundary.isoformat(), timezone=body.schedule.timezone,
            created_at=now, created_by=actor, state="active", request_key=scoped_key,
            request_hash=digest, skipped_ranges=[], semantic_basis="event_time", execution_mode="current")
        self.repository.add(aggregate, schedule)
        self.uow.commit()
        return self.card(aggregate.id)

    def _card(self, aggregate, schedule):
        _, next_at = bounds(schedule.anchor_utc, schedule.duration_seconds, schedule.next_index)
        deadline = close_deadline(next_at, schedule.duration_seconds) if aggregate.execution_mode == "current" else None
        return AggregateCard(id=aggregate.id, name=aggregate.name, source_id=aggregate.source_identity,
            source_name=aggregate.source_name, source_type=aggregate.source_type,
            aggregate_rule=aggregate.rule, rule_hash=aggregate.rule_hash,
            schedule_id=schedule.id, anchor_utc=schedule.anchor_utc, duration_seconds=schedule.duration_seconds,
            original_boundary=aggregate.original_boundary, timezone=aggregate.timezone,
            created_at=aggregate.created_at, created_by=aggregate.created_by, state=aggregate.state,
            archived_at=aggregate.archived_at, deleted_at=aggregate.deleted_at, deleted_by=aggregate.deleted_by,
            skipped_ranges=aggregate.skipped_ranges, next_index=schedule.next_index,
            next_scheduled_at=next_at, lag_seconds=max(0, (self.repository.now() - deadline).total_seconds()) if deadline and aggregate.state == "active" else 0,
            last_error=aggregate.last_error, last_error_at=aggregate.last_error_at,
            semantic_basis=aggregate.semantic_basis, execution_mode=aggregate.execution_mode,
            execution_reason="aggregate_time_semantics_retired" if aggregate.execution_mode == "retired" else None,
            bootstrap_at=schedule.bootstrap_at, online_start_index=schedule.online_start_index,
            paused=schedule.paused, execution_generation=schedule.execution_generation,
            active_cleanup_id=schedule.active_cleanup_id, next_close_not_before_at=deadline,
            diagnostics=self.repository.diagnostics(aggregate.id))

    @guarded
    def card(self, aggregate_id):
        aggregate, schedule = self._get(aggregate_id)
        return self._card(aggregate, schedule)

    @guarded
    def list(self, limit, offset, q=None, state=None):
        items, total = self.repository.list(limit, offset, q.strip() if q else None, state)
        return {"items": [self._card(a, self.repository.schedule(a.schedule_id)) for a in items], "total": total}

    def _point(self, aggregate, point):
        historical = isinstance(point, HistoricalAggregatePoint)
        closure = self.repository.closure_by_id(point.id)
        return PointCard(id=point.id, aggregate_id=aggregate.id, kind="historical" if historical else "working",
            start_utc=point.start_utc, end_utc=point.end_utc, scheduled_at=point.end_utc,
            actual_started_at=point.actual_started_at, actual_completed_at=point.actual_completed_at,
            event_count=point.event_count, event_time_min=point.event_time_min, event_time_max=point.event_time_max,
            features=point.features, completeness=point.completeness, completeness_reason=point.completeness_reason,
            evidence_refs=point.evidence_refs, time_basis=point.time_basis,
            origin=closure.origin if closure else "legacy", window_index=closure.window_index if closure else None,
            snapshot_reference_at=closure.snapshot_reference_at if closure else None,
            close_not_before_at=closure.close_not_before_at if closure else None,
            snapshot_metadata=self.repository.snapshot_metadata(point.id))

    @guarded
    def points(self, aggregate_id, limit, offset):
        aggregate, _ = self._get(aggregate_id)
        items, total = self.repository.points(aggregate, limit, offset)
        return {"items": [self._point(aggregate, point) for point in items], "total": total}

    @guarded
    def copy(self, aggregate_id):
        aggregate, schedule = self._get(aggregate_id)
        if aggregate.state == "deleted" and aggregate.execution_mode != "retired":
            raise DomainError("aggregate_deleted", "Удалённый агрегат нельзя копировать", 409)
        return ValidateRequest(source_id=aggregate.source_identity, aggregate_rule=aggregate.rule,
            schedule=ScheduleInput(boundary=aggregate.original_boundary, timezone=aggregate.timezone,
                                   duration_seconds=schedule.duration_seconds))

    @guarded
    def lifecycle(self, aggregate_id, action, actor):
        self.repository.limits(self.budget_limits, False)
        aggregate, schedule = self._get(aggregate_id, lock=True)
        if schedule.active_cleanup_id:
            raise DomainError("cleanup_in_progress", "Очистка ещё не завершена", 409)
        if aggregate.execution_mode == "retired" and action != "delete":
            raise DomainError("aggregate_time_semantics_retired", "Прежний расчёт остановлен; создайте копию", 409)
        now = self.repository.now()
        if aggregate.state == "deleted":
            if action == "delete":
                return self._card(aggregate, schedule)
            raise DomainError("aggregate_deleted", "Агрегат удалён", 409)
        schedule.execution_generation += 1
        if action == "archive" and aggregate.state == "active":
            aggregate.state, aggregate.archived_at = "archived", now
        elif action == "resume" and aggregate.state == "archived":
            next_index = max(schedule.next_index, future_index(schedule, now))
            if next_index > schedule.next_index:
                spec = WindowSchedule(schedule.anchor_utc, schedule.duration_seconds)
                aggregate.skipped_ranges = [*aggregate.skipped_ranges,
                    {"start_utc": spec.window(schedule.next_index).start.isoformat(),
                     "end_utc": spec.window(next_index).start.isoformat(), "reason": "intentional_archive"}]
            schedule.next_index, aggregate.state, aggregate.archived_at = next_index, "active", None
        elif action == "delete":
            aggregate.state, aggregate.deleted_at, aggregate.deleted_by = "deleted", now, actor
            if aggregate.execution_mode == "current":
                self.detectors.stop_for_aggregate(aggregate.id, "aggregate_deleted", now)
        self.uow.commit()
        return self.card(aggregate_id)

    @guarded
    def fields(self, source_id, query, limit, offset):
        self._source(source_id)
        fields, total = self.catalog.list_fields(query, None, None, None, limit, offset)
        return {"items": [{"name": f.name, "type": f.type, "is_array": f.is_array,
                "description": f.description, "supported": field_support_reason(f, self.catalog) is None,
                "reason": field_support_reason(f, self.catalog),
                "operators": [op.name for op in OPERATORS.values()
                    if field_support_reason(f, self.catalog) is None and compatible(op, f)]}
                for f in fields], "total": total}

    def operators(self, include_extended=False):
        return operator_catalog(include_extended)

    def field_catalog(self, source_id, query, limit, offset):
        return AggregateFieldCatalogService(self.repository, self.uow, self.catalog,
            self.field_catalog_limits).read(source_id, query, limit, offset)

    def _execution_service(self):
        return AggregateExecutionService(self.repository, self.uow, self.catalog, self.budget_limits)

    def aggregate_command(self, aggregate_id, actor, key, action, keep_last=None, operation_id=None):
        from .aggregate_cleanup import AggregateCleanupService
        return AggregateCleanupService(self.repository, self.uow).command(
            aggregate_id, actor, key, action, keep_last, operation_id)

    def cleanup_preview(self, aggregate_id, keep_last):
        from .aggregate_cleanup import AggregateCleanupService
        return AggregateCleanupService(self.repository, self.uow).preview(aggregate_id, keep_last)

    def cleanup_operation(self, aggregate_id, operation_id):
        from .aggregate_cleanup import AggregateCleanupService
        return AggregateCleanupService(self.repository, self.uow).operation(aggregate_id, operation_id)

    @guarded
    def due(self, limit: int = 1000):
        return self.repository.due(limit)

    @guarded
    def record_worker_error(self, aggregate_id, code: str | None) -> None:
        self.uow.rollback()
        self.repository.record_error(aggregate_id, code)
        self.uow.commit()

    @guarded
    def advance(self, aggregate_id):
        return self._execution_service().advance(aggregate_id)

    @guarded
    def historical(self, aggregate_id, body):
        aggregate, schedule = self._get(aggregate_id)
        if aggregate.execution_mode == "retired":
            raise DomainError("aggregate_time_semantics_retired", "Прежний расчёт остановлен; сохранённые точки доступны для чтения", 409)
        window = self._window(schedule, body.window_start_utc, self.repository.now())
        return self.point_at(aggregate_id, index_at(schedule.anchor_utc, schedule.duration_seconds, window.start))

    @guarded
    def point_at(self, aggregate_id, index):
        aggregate, schedule = self._get(aggregate_id)
        closure = self.repository.closure_at(schedule.id, index)
        if closure is None:
            raise DomainError("canonical_point_not_ready", "Окно ещё не рассчитано", 409)
        return self._saved_point(aggregate, closure)

    @guarded
    def point_by_id(self, aggregate_id, point_id):
        aggregate, _ = self._get(aggregate_id)
        closure = self.repository.closure_by_id(point_id)
        if closure is None or closure.aggregate_id != aggregate.id:
            raise DomainError("aggregate_point_not_found", "Точка не найдена", 404)
        return self._saved_point(aggregate, closure)

    def _saved_point(self, aggregate, closure):
        if closure.payload_state == "cleared":
            raise DomainError("aggregate_point_cleared", "Данные точки очищены", 410,
                {"origin_point_id": str(closure.origin_point_id), "window_index": closure.window_index,
                 "time_basis": closure.time_basis, "origin": closure.origin})
        return self._point(aggregate, self.repository.payload(closure))
