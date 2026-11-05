"""Aggregate application scenarios, shared by HTTP and the permanent worker."""

import hashlib
import json
import time
from datetime import UTC, timedelta
from functools import wraps
from uuid import uuid4, uuid5

from sqlalchemy.exc import DBAPIError

from ...aggregation.budget import estimate
from ...aggregation.results import interpret_result
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
from .saved_coverage import evaluate_saved_coverage
from .window_schedule import WindowSchedule
from .working_windows import WorkingWindowService


def guarded(method):
    @wraps(method)
    def call(self, *args, **kwargs):
        for attempt in range(3):
            try:
                return method(self, *args, **kwargs)
            except RuleError as error:
                self.uow.rollback()
                raise DomainError(error.code, "Правило агрегата не прошло проверку", 422,
                                  {"path": error.path, "hint": error.hint}) from error
            except DBAPIError as error:
                self.uow.rollback()
                code = getattr(error.orig, "sqlstate", None)
                if code in {"40001", "40P01"} and attempt < 2:
                    continue
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
                 uow: UnitOfWork, catalog, limits: Limits = Limits()):
        self.repository, self.windows, self.evidence = repository, windows, evidence
        self.capture, self.uow, self.catalog, self.budget_limits = capture, uow, catalog, limits

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
                            "availability_at", body.schedule.duration_seconds, False)
        return ValidationResult(rule_hash=compiled.sha256, anchor_utc=body.schedule.anchor_utc,
            duration_seconds=body.schedule.duration_seconds,
            feature_keys=[f.key for f in compiled.rule.features], budget=budget)

    @guarded
    def validate(self, body: ValidateRequest):
        return self._validate(body)

    def _capture_ready(self):
        state = self.capture.status()
        if not state.ready:
            raise DomainError("availability_capture_unavailable", "Захват времени доступности не готов", 503,
                              {"reason": state.reason})

    def _preview(self, body):
        self._source(body.source_id)
        compiled = compile_rule(body.aggregate_rule, self.catalog, self.budget_limits)
        schedule = WindowSchedule(body.schedule.anchor_utc, body.schedule.duration_seconds)
        window = self._window(schedule, body.window_start_utc, self.repository.now())
        if body.time_basis == "availability_at":
            progress = self.repository.progress()
            if (progress is None or progress.availability_verified_from_at is None
                    or window.start < progress.availability_verified_from_at):
                raise DomainError("historical_time_basis_unknown",
                    "Для этого окна доступен только явно выбранный ретроспективный preview", 422)
            self._capture_ready()
        barrier = (self.repository.coverage_barrier(body.source_id, window.end)
                   if body.time_basis == "availability_at" else None)
        budget = self._cost(compiled, body.source_id, window.start, window.end, body.time_basis,
                            schedule.duration_seconds, True)
        values = interpret_result(self.repository.calculate(body.source_id, window.start, window.end, body.time_basis, compiled), compiled, schedule.duration_seconds)
        # Recheck the actual statement's snapshot, even if writes occurred after
        # estimation. Never return a truncated or over-budget partial result.
        estimate(compiled, values["observed_rows"], budget["source_rows"], schedule.duration_seconds,
                 self.budget_limits, True, values.pop("observed_width"))
        completeness, reason = "unverified", "preview_snapshot_without_coverage_proof"
        if barrier is not None:
            proven, proven_reason, _ = evaluate_saved_coverage(self.evidence, body.source_id,
                window.start, window.end, barrier.availability_at)
            if proven in {"complete", "interrupted"}:
                completeness, reason = proven, proven_reason
        return PreviewResult(rule_hash=compiled.sha256, start_utc=window.start, end_utc=window.end,
            scheduled_at=window.end, **values, completeness=completeness,
            completeness_reason=reason, time_basis=body.time_basis,
            budget=budget)

    @guarded
    def preview(self, body: AggregatePreviewRequest):
        return self._preview(body)

    @guarded
    def create(self, body, actor, key):
        self.repository.limits(self.budget_limits, True)
        if not key or len(key) > 200:
            raise DomainError("idempotency_key_required", "Укажите Idempotency-Key до 200 символов", 422)
        request_document = body.model_dump(mode="json")
        # Preserve pre-extension idempotency digests as well as rule hashes.
        # Other legacy nullable fields must retain their original serialization.
        for feature in request_document["aggregate_rule"]["features"]:
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
        self._validate(body)
        preview = self._preview(body)
        if preview.rule_hash != body.rule_hash:
            raise DomainError("rule_hash_mismatch", "Хэш не совпадает с проверенным правилом", 409)
        if not self.windows.lock_source(body.source_id):
            raise DomainError("source_unavailable", "Источник удалён или больше не является собственным", 409)
        now = self.repository.now()
        source = self._source(body.source_id)
        schedule = AggregateSchedule(id=uuid4(), source_identity=source.id,
            anchor_utc=body.schedule.anchor_utc, duration_seconds=body.schedule.duration_seconds)
        schedule.next_index = future_index(schedule, now)
        aggregate = Aggregate(id=uuid4(), schedule_id=schedule.id, name=body.name,
            source_identity=source.id, source_name=source.name, source_type=source.source_type,
            rule=body.aggregate_rule.model_dump(exclude_none=True), rule_hash=preview.rule_hash,
            original_boundary=body.schedule.boundary.isoformat(), timezone=body.schedule.timezone,
            created_at=now, created_by=actor, state="active", request_key=scoped_key,
            request_hash=digest, skipped_ranges=[])
        self.repository.add(aggregate, schedule)
        self.uow.commit()
        return self.card(aggregate.id)

    def _card(self, aggregate, schedule):
        next_at = WindowSchedule(schedule.anchor_utc, schedule.duration_seconds).window(schedule.next_index).end
        return AggregateCard(id=aggregate.id, name=aggregate.name, source_id=aggregate.source_identity,
            source_name=aggregate.source_name, source_type=aggregate.source_type,
            aggregate_rule=aggregate.rule, rule_hash=aggregate.rule_hash,
            schedule_id=schedule.id, anchor_utc=schedule.anchor_utc, duration_seconds=schedule.duration_seconds,
            original_boundary=aggregate.original_boundary, timezone=aggregate.timezone,
            created_at=aggregate.created_at, created_by=aggregate.created_by, state=aggregate.state,
            archived_at=aggregate.archived_at, deleted_at=aggregate.deleted_at, deleted_by=aggregate.deleted_by,
            skipped_ranges=aggregate.skipped_ranges, next_index=schedule.next_index,
            next_scheduled_at=next_at, lag_seconds=max(0, (self.repository.now() - next_at).total_seconds()) if aggregate.state == "active" else 0,
            last_error=aggregate.last_error, last_error_at=aggregate.last_error_at)

    @guarded
    def card(self, aggregate_id):
        aggregate, schedule = self._get(aggregate_id)
        return self._card(aggregate, schedule)

    @guarded
    def list(self, limit, offset):
        items, total = self.repository.list(limit, offset)
        return {"items": [self._card(a, self.repository.schedule(a.schedule_id)) for a in items], "total": total}

    def _point(self, aggregate, point):
        historical = isinstance(point, HistoricalAggregatePoint)
        return PointCard(id=point.id, aggregate_id=aggregate.id, kind="historical" if historical else "working",
            start_utc=point.start_utc, end_utc=point.end_utc, scheduled_at=point.end_utc,
            actual_started_at=point.actual_started_at, actual_completed_at=point.actual_completed_at,
            event_count=point.event_count, event_time_min=point.event_time_min, event_time_max=point.event_time_max,
            features=point.features, completeness=point.completeness, completeness_reason=point.completeness_reason,
            evidence_refs=point.evidence_refs, time_basis=point.time_basis if historical else "availability_at")

    @guarded
    def points(self, aggregate_id, limit, offset):
        aggregate, _ = self._get(aggregate_id)
        items, total = self.repository.points(aggregate, limit, offset)
        return {"items": [self._point(aggregate, point) for point in items], "total": total}

    @guarded
    def copy(self, aggregate_id):
        aggregate, schedule = self._get(aggregate_id)
        if aggregate.state == "deleted":
            raise DomainError("aggregate_deleted", "Удалённый агрегат нельзя копировать", 409)
        return ValidateRequest(source_id=aggregate.source_identity, aggregate_rule=aggregate.rule,
            schedule=ScheduleInput(boundary=aggregate.original_boundary, timezone=aggregate.timezone,
                                   duration_seconds=schedule.duration_seconds))

    @guarded
    def lifecycle(self, aggregate_id, action, actor):
        self.repository.limits(self.budget_limits, False)
        aggregate, schedule = self._get(aggregate_id, lock=True)
        now = self.repository.now()
        if aggregate.state == "deleted":
            if action == "delete":
                return self._card(aggregate, schedule)
            raise DomainError("aggregate_deleted", "Агрегат удалён", 409)
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

    def operators(self):
        return operator_catalog()

    def _working_calculation(self, aggregate, stored, window):
        from ...aggregation.rules import AggregateRule
        compiled = compile_rule(AggregateRule.model_validate(aggregate.rule), self.catalog, self.budget_limits)
        budget = self._cost(compiled, stored.source_identity, window.start, window.end, "availability_at", stored.duration_seconds, False)
        values = interpret_result(self.repository.calculate(stored.source_identity, window.start, window.end, "availability_at", compiled), compiled, stored.duration_seconds)
        estimate(compiled, values["observed_rows"], budget["source_rows"], stored.duration_seconds, self.budget_limits, False, values.pop("observed_width"))
        return values

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
        self.repository.limits(self.budget_limits, False)
        aggregate, _ = self._get(aggregate_id)
        service = WorkingWindowService(self.windows, self.evidence, self.capture, self.uow,
            calculator=lambda stored, window: self._working_calculation(aggregate, stored, window))
        service.prepare_barrier(aggregate.schedule_id)
        self.repository.limits(self.budget_limits, False)
        return service.close_next_window(aggregate.schedule_id)

    @guarded
    def historical(self, aggregate_id, body):
        self.repository.limits(self.budget_limits, False)
        aggregate, schedule = self._get(aggregate_id, lock=True, active=True)
        window = self._window(schedule, body.window_start_utc, aggregate.created_at)
        saved = self.repository.historical(aggregate_id, window.start)
        if saved:
            return self._point(aggregate, saved)
        progress = self.repository.progress()
        if (progress and progress.legacy_cutover_at is not None and window.end <= progress.legacy_cutover_at):
            basis = "legacy_markerless"
        elif (progress and progress.availability_verified_from_at is not None
              and window.start >= progress.availability_verified_from_at):
            basis = "availability_at"
        else:
            raise DomainError("historical_time_basis_unknown", "Основание времени исторического окна не доказано", 422)
        started = self.repository.now()
        barrier = None
        if basis == "availability_at":
            self._capture_ready()
            if not self.windows.lock_source(aggregate.source_identity):
                raise DomainError("source_unavailable", "Источник недоступен", 409)
            marker_id = uuid5(aggregate.id, "historical:" + window.start.isoformat())
            self.windows.ensure_barrier(marker_id, aggregate.source_identity, started)
            self.uow.commit()
            deadline = time.monotonic() + 10
            while True:
                aggregate, schedule = self._get(aggregate_id, lock=True, active=True)
                saved = self.repository.historical(aggregate_id, window.start)
                if saved:
                    return self._point(aggregate, saved)
                self._capture_ready()
                barrier = self.windows.barrier(marker_id)
                if barrier and barrier.availability_at is not None:
                    break
                self.uow.rollback()
                if time.monotonic() >= deadline:
                    raise DomainError("barrier_pending", "Барьер ещё не материализован; повторите запрос", 503)
                time.sleep(.05)
            if self.repository.markerless(aggregate.source_identity, window.start, window.end):
                raise DomainError("availability_boundary_violation", "В окне найдены записи без маркера доступности", 409)
        values = self._working_calculation(aggregate, schedule, window) if basis == "availability_at" else None
        if values is None:
            from ...aggregation.rules import AggregateRule
            compiled = compile_rule(AggregateRule.model_validate(aggregate.rule), self.catalog, self.budget_limits)
            budget = self._cost(compiled, aggregate.source_identity, window.start, window.end, basis, schedule.duration_seconds, False)
            values = interpret_result(self.repository.calculate(aggregate.source_identity, window.start, window.end, basis, compiled), compiled, schedule.duration_seconds)
            estimate(compiled, values["observed_rows"], budget["source_rows"], schedule.duration_seconds, self.budget_limits, False, values.pop("observed_width"))
        completeness, reason, refs = (evaluate_saved_coverage(self.evidence, aggregate.source_identity,
            window.start, window.end, barrier.availability_at) if barrier else ("unknown", "legacy_coverage_unproven", {}))
        values.pop("observed_rows")
        point = HistoricalAggregatePoint(aggregate_id=aggregate.id, start_utc=window.start, end_utc=window.end,
            actual_started_at=started, actual_completed_at=self.repository.now(), **values,
            time_basis="backend_processed_at_historical" if basis == "legacy_markerless" else basis,
            completeness=completeness, completeness_reason=reason, evidence_refs=refs)
        self.repository.add_historical(point)
        self.uow.commit()
        return self._point(aggregate, point)
