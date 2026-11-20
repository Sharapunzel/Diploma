"""PostgreSQL queries for immutable aggregates and their snapshots."""

from datetime import UTC
from time import monotonic
from uuid import UUID

from sqlalchemy import Text, func, select, update
from sqlalchemy.orm import Session, defer
from sqlalchemy.orm.attributes import set_committed_value

from ...aggregation.event_time import EventTimeBudgetError
from ...aggregation.rules import Limits
from ...models import (
    Aggregate,
    AggregateSchedule,
    HistoricalAggregatePoint,
    ParsedLog,
    Source,
    WorkingWindowPoint,
)
from .aggregate_attempts import SqlAlchemyAggregateAttemptQueries
from .aggregate_cleanup import SqlAlchemyAggregateCleanupQueries
from .aggregate_discovery import SqlAlchemyAggregateDiscovery
from .aggregate_event_time import SqlAlchemyEventTimeQueries
from .aggregate_field_catalog import SqlAlchemyFieldCatalogQueries
from .aggregate_overlap import SqlAlchemyAggregateOverlapQueries
from .aggregate_retention import SqlAlchemyAggregateRetentionQueries


class SqlAlchemyAggregateRepository(SqlAlchemyFieldCatalogQueries, SqlAlchemyEventTimeQueries,
                                   SqlAlchemyAggregateDiscovery, SqlAlchemyAggregateCleanupQueries,
                                   SqlAlchemyAggregateOverlapQueries, SqlAlchemyAggregateRetentionQueries,
                                   SqlAlchemyAggregateAttemptQueries):
    def __init__(self, session: Session):
        self.session = session

    def now(self):
        return self.session.scalar(select(func.clock_timestamp()))

    def execution_now(self):
        """Operational UTC anchor, independent of synthetic event release clocks."""
        return self.session.scalar(select(func.clock_timestamp()))

    def source(self, source_id):
        return self.session.get(Source, source_id)

    def read_budget(self, milliseconds):
        deadline = monotonic() + milliseconds / 1000
        with self.session.no_autoflush:
            # Checkout/network wait itself is a separate infrastructure bound.
            # It must never give the next statement a stale full allowance.
            self.session.connection()
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise EventTimeBudgetError("operation_seconds")
            milliseconds = max(1, int(remaining * 1000))
            self.session.execute(select(
                func.set_config("statement_timeout", str(milliseconds), True),
                func.set_config("lock_timeout", str(milliseconds), True)))

    def limits(self, limits: Limits, preview: bool, budget=None):
        if budget is None:
            timeout = limits.preview_timeout_ms if preview else limits.working_timeout_ms
            self.session.execute(select(func.set_config("statement_timeout", str(timeout), True)))
            self.session.execute(select(func.set_config("work_mem", f"{limits.work_mem_mb}MB", True)))
            self.session.execute(select(func.set_config("jit", "off", True)))
            return
        # Large guarded JSON expressions otherwise trigger LLVM compilation
        # even for a one-row window.
        with self.session.no_autoflush:
            self.read_budget(budget.milliseconds())
            budget.check()
            self.session.execute(select(func.set_config("work_mem", f"{limits.work_mem_mb}MB", True),
                func.set_config("jit", "off", True)))
            budget.check()

    def _base(self, source_id, start, end, basis):
        if basis != "event_time":
            raise ValueError("legacy_calculation_removed")
        from .aggregate_event_time import classified_events
        events = classified_events(source_id, self.now())
        query = select(events.c.ecs_data).where(events.c.static_reason.is_(None), events.c.eligible)
        if start is not None:
            query = query.where(events.c.event_time >= start.astimezone(UTC).replace(tzinfo=None))
        if end is not None:
            query = query.where(events.c.event_time < end.astimezone(UTC).replace(tzinfo=None))
        return query

    def estimate_rows(self, source_id, start, end, basis, before_read=None):
        base = self._base(source_id, start, end, basis).subquery()
        if before_read:
            before_read()
        rows, size = self.session.execute(select(func.count(), func.max(func.octet_length(base.c.ecs_data.cast(Text)))).select_from(base)).one()
        if before_read:
            before_read()
        total = self.session.scalar(select(func.count()).select_from(ParsedLog).where(ParsedLog.source_id == source_id))
        return rows, total, (size or 0) + 64


    def idempotency_lock(self, key):
        self.session.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(key, 0))))

    def by_key(self, key):
        return self.session.scalar(select(Aggregate).where(Aggregate.request_key == key))

    def add(self, aggregate, schedule):
        self.session.add(schedule)
        self.session.flush()
        self.session.add(aggregate)
        self.session.flush()
        self.add_retention_state(aggregate, aggregate.created_at)

    def get(self, aggregate_id):
        return self.session.get(Aggregate, aggregate_id)

    def schedule(self, schedule_id, lock=False):
        query = select(AggregateSchedule).where(AggregateSchedule.id == schedule_id)
        if lock:
            query = query.execution_options(populate_existing=True)
        return self.session.scalar(query.with_for_update() if lock else query)

    def refresh(self, aggregate):
        self.session.refresh(aggregate)

    def list(self, limit, offset, q=None, state=None):
        query = select(Aggregate).where(Aggregate.state != "deleted")
        if state is not None:
            query = query.where(Aggregate.state == state)
        if q:
            literal = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            query = query.where(Aggregate.name.ilike(f"%{literal}%", escape="\\"))
        total = self.session.scalar(select(func.count()).select_from(query.subquery()))
        return list(self.session.scalars(query.order_by(Aggregate.created_at.desc(), Aggregate.id.desc()).limit(limit).offset(offset))), total

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


    def due(self, limit=100, exclude_ids=()):
        query = select(Aggregate.id).join(AggregateSchedule,
            Aggregate.schedule_id == AggregateSchedule.id).where(
                AggregateSchedule.active_cleanup_id.is_not(None)
                | ((Aggregate.execution_mode == "current") & (Aggregate.state == "active")
                   & AggregateSchedule.paused.is_(False))
            )
        if exclude_ids:
            query = query.where(Aggregate.id.not_in(exclude_ids))
        return list(self.session.scalars(query.order_by(Aggregate.last_error_at.asc().nullsfirst(), Aggregate.id).limit(limit)))

    def record_error(self, aggregate_id: UUID, code: str | None):
        self.session.execute(update(Aggregate).where(Aggregate.id == aggregate_id).values(last_error=code, last_error_at=func.clock_timestamp()))
