"""PostgreSQL queries for immutable aggregates and their snapshots."""

from datetime import UTC
from uuid import UUID

from sqlalchemy import Text, exists, func, select, text, update
from sqlalchemy.orm import Session, defer
from sqlalchemy.orm.attributes import set_committed_value

from ...aggregation.rules import CompiledRule, Limits
from ...models import (
    Aggregate,
    AggregateSchedule,
    AvailabilityMarker,
    HistoricalAggregatePoint,
    ParsedLog,
    ReplicationProgress,
    Source,
    WorkingWindowPoint,
)
from .aggregate_compiler import aggregate_statement, read_statistics


class SqlAlchemyAggregateRepository:
    def __init__(self, session: Session):
        self.session = session

    def now(self):
        return self.session.scalar(select(func.clock_timestamp()))

    def source(self, source_id):
        return self.session.get(Source, source_id)

    def limits(self, limits: Limits, preview: bool):
        timeout = limits.preview_timeout_ms if preview else limits.working_timeout_ms
        self.session.execute(select(func.set_config("statement_timeout", str(timeout), True)))
        self.session.execute(select(func.set_config("work_mem", f"{limits.work_mem_mb}MB", True)))
        # Large guarded JSON expressions otherwise trigger LLVM compilation
        # even for a one-row window. Keep latency bounded by query work rather
        # than compiling hundreds of transient expression functions per point.
        self.session.execute(select(func.set_config("jit", "off", True)))

    def _base(self, source_id, start, end, basis):
        query = select(ParsedLog.ecs_data).where(ParsedLog.source_id == source_id)
        if basis == "availability_at":
            query = query.join(AvailabilityMarker, AvailabilityMarker.event_id == ParsedLog.id)
            query = query.where(AvailabilityMarker.source_identity == source_id,
                                AvailabilityMarker.kind == "event")
            clock = AvailabilityMarker.availability_at
        else:
            clock = ParsedLog.backend_processed_at
            if basis == "legacy_markerless":
                query = query.where(~exists(select(1).where(AvailabilityMarker.event_id == ParsedLog.id)))
        if start is not None:
            query = query.where(clock >= start)
        if end is not None:
            query = query.where(clock < end)
        return query

    def estimate_rows(self, source_id, start, end, basis):
        base = self._base(source_id, start, end, basis).subquery()
        rows, size = self.session.execute(select(func.count(), func.max(func.octet_length(base.c.ecs_data.cast(Text)))).select_from(base)).one()
        total = self.session.scalar(select(func.count()).select_from(ParsedLog).where(ParsedLog.source_id == source_id))
        return rows, total, (size or 0) + 64

    def calculate(self, source_id, start, end, basis, compiled: CompiledRule):
        base = self._base(source_id, start, end, basis).subquery()
        return read_statistics(self.session.execute(aggregate_statement(base, compiled)).mappings().one(), compiled)

    def progress(self):
        return self.session.get(ReplicationProgress, 1)

    def coverage_barrier(self, source_id, end):
        return self.session.scalar(select(AvailabilityMarker).where(
            AvailabilityMarker.source_identity == source_id, AvailabilityMarker.kind == "barrier",
            AvailabilityMarker.created_at >= end, AvailabilityMarker.availability_at >= end)
            .order_by(AvailabilityMarker.availability_at.desc()).limit(1))

    def markerless(self, source_id, start, end):
        return self.session.scalar(select(exists(select(1).select_from(ParsedLog).where(
            ParsedLog.source_id == source_id, ParsedLog.backend_processed_at >= start,
            ParsedLog.backend_processed_at < end,
            ~exists(select(1).where(AvailabilityMarker.event_id == ParsedLog.id))))))

    def idempotency_lock(self, key):
        self.session.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(key, 0))))

    def by_key(self, key):
        return self.session.scalar(select(Aggregate).where(Aggregate.request_key == key))

    def add(self, aggregate, schedule):
        self.session.add(schedule)
        self.session.flush()
        self.session.add(aggregate)
        self.session.flush()

    def get(self, aggregate_id):
        return self.session.get(Aggregate, aggregate_id)

    def schedule(self, schedule_id, lock=False):
        query = select(AggregateSchedule).where(AggregateSchedule.id == schedule_id)
        return self.session.scalar(query.with_for_update() if lock else query)

    def refresh(self, aggregate):
        self.session.refresh(aggregate)

    def list(self, limit, offset):
        query = select(Aggregate).where(Aggregate.state != "deleted")
        total = self.session.scalar(select(func.count()).select_from(query.subquery()))
        return list(self.session.scalars(query.order_by(Aggregate.created_at.desc(), Aggregate.id).limit(limit).offset(offset))), total

    def points(self, aggregate, limit, offset):
        # Fetch at most offset+limit from each stream, then merge by the same
        # stable ordering. This avoids loading the full history into memory.
        working = select(WorkingWindowPoint).where(WorkingWindowPoint.schedule_id == aggregate.schedule_id)
        historical = select(HistoricalAggregatePoint).where(HistoricalAggregatePoint.aggregate_id == aggregate.id)
        total = sum(self.session.scalar(select(func.count()).select_from(q.subquery())) for q in (working, historical))
        items = self._point_rows(working.order_by(WorkingWindowPoint.start_utc.desc(), WorkingWindowPoint.id).limit(offset + limit), WorkingWindowPoint)
        items += self._point_rows(historical.order_by(HistoricalAggregatePoint.start_utc.desc(), HistoricalAggregatePoint.id).limit(offset + limit), HistoricalAggregatePoint)
        items.sort(key=lambda p: (-p.start_utc.timestamp(), str(p.id)))
        return items[offset:offset + limit], total

    def _point_rows(self, query, model):
        # Preserve ORM snapshots, but transport ECS bounds in technical UTC:
        # timestamptz's local rendering can exceed Python's range at a boundary.
        query = query.options(defer(model.event_time_min), defer(model.event_time_max)).add_columns(
            func.timezone("UTC", model.event_time_min), func.timezone("UTC", model.event_time_max))
        items = []
        for point, minimum, maximum in self.session.execute(query):
            set_committed_value(point, "event_time_min", minimum.replace(tzinfo=UTC) if minimum is not None else None)
            set_committed_value(point, "event_time_max", maximum.replace(tzinfo=UTC) if maximum is not None else None)
            items.append(point)
        return items

    def historical(self, aggregate_id, start):
        items = self._point_rows(select(HistoricalAggregatePoint).where(HistoricalAggregatePoint.aggregate_id == aggregate_id, HistoricalAggregatePoint.start_utc == start), HistoricalAggregatePoint)
        return items[0] if items else None

    def add_historical(self, point):
        self.session.add(point)
        self.session.flush()

    def due(self, limit=100):
        return list(self.session.scalars(select(Aggregate.id).join(AggregateSchedule, Aggregate.schedule_id == AggregateSchedule.id).where(
            Aggregate.state == "active", AggregateSchedule.anchor_utc +
            (AggregateSchedule.next_index + 1) * AggregateSchedule.duration_seconds * text("interval '1 second'") <= func.clock_timestamp()
        ).order_by(Aggregate.last_error_at.asc().nullsfirst(), AggregateSchedule.anchor_utc + (AggregateSchedule.next_index + 1) * AggregateSchedule.duration_seconds * text("interval '1 second'"), Aggregate.id).limit(limit)))

    def record_error(self, aggregate_id: UUID, code: str | None):
        self.session.execute(update(Aggregate).where(Aggregate.id == aggregate_id).values(last_error=code, last_error_at=func.clock_timestamp()))
