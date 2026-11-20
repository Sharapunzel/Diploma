"""Overlap recomputation reads only exact, fingerprint-verified membership."""

from sqlalchemy import Text, func, select

from ...aggregation.event_time import MAX_EVENTS, MAX_SNAPSHOT_BYTES, EventTimeBudgetError
from ...detection.contracts import DetectionError
from ...models import AggregateClosure, AggregateMembership, ParsedLog, WorkingWindowPoint
from .aggregate_compiler import aggregate_statement, read_statistics


class SqlAlchemyAggregateOverlapQueries:
    def overlap_statistics(self, schedule_id, start, end, base_indices, compiled, budget):
        self.read_budget(budget.milliseconds())
        closures = list(self.session.scalars(select(AggregateClosure).where(
            AggregateClosure.schedule_id == schedule_id, AggregateClosure.window_index.in_(base_indices)
            ).order_by(AggregateClosure.window_index)))
        if len(closures) != len(base_indices) or any(c.payload_state != "present" for c in closures):
            raise DetectionError("warmup_canonical_coverage_missing")
        if any(c.calculation_status != "computed" for c in closures):
            raise DetectionError("warmup_window_uncomputed")
        ids = [c.origin_point_id for c in closures]
        self.read_budget(budget.milliseconds())
        qualities = list(self.session.scalars(select(WorkingWindowPoint.completeness).where(WorkingWindowPoint.id.in_(ids))))
        if len(qualities) != len(ids) or any(q != "complete" for q in qualities):
            raise DetectionError("coverage:unknown")
        membership = select(AggregateMembership).where(AggregateMembership.closure_id.in_(ids),
            AggregateMembership.event_time >= start, AggregateMembership.event_time < end).subquery()
        pair = func.jsonb_build_array(ParsedLog.ecs_data, ParsedLog.normalization_diagnostics).cast(Text)
        joined = membership.outerjoin(ParsedLog, ParsedLog.id == membership.c.event_id)
        self.read_budget(budget.milliseconds())
        count, missing, changed, size = self.session.execute(select(func.count(),
            func.count().filter(ParsedLog.id.is_(None)),
            func.count().filter(membership.c.fingerprint.is_distinct_from(
                func.encode(func.sha256(func.convert_to(pair, "UTF8")), "hex"))),
            func.coalesce(func.sum(func.octet_length(pair)), 0)).select_from(joined)).one()
        if missing or changed:
            raise DetectionError("warmup_membership_source_unavailable")
        if count > MAX_EVENTS or size > MAX_SNAPSHOT_BYTES:
            raise EventTimeBudgetError("overlap_input_bytes")
        base = select(ParsedLog.ecs_data, func.timezone("UTC", membership.c.event_time).label("event_time")
            ).select_from(joined).where(ParsedLog.id.is_not(None)).subquery()
        self.read_budget(budget.milliseconds())
        statistics = read_statistics(self.session.execute(aggregate_statement(base, compiled)).mappings().one(), compiled)
        budget.check()
        return statistics, ids
