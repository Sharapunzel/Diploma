"""Cleanup persistence and bounded compaction; no commits or policy decisions."""

from copy import deepcopy
from datetime import datetime

from sqlalchemy import Text, delete, func, select, update

from ...aggregation.event_time import MAX_SNAPSHOT_BYTES, EventTimeBudgetError
from ...models import (
    AggregateCleanupOperation,
    AggregateClosure,
    AggregateCommand,
    AggregateMembership,
    AggregateSnapshot,
    Detector,
    DetectorOutcome,
    DetectorWarmup,
    HistoricalAggregatePoint,
    WorkingWindowPoint,
)


def document(row):
    return deepcopy({c.key: getattr(row, c.key) for c in row.__table__.columns})


def identity(row):
    return {"origin_point_id": str(row.origin_point_id), "window_index": row.window_index,
        "start_utc": row.start_utc.isoformat(), "point_kind": row.point_kind}


class SqlAlchemyAggregateCleanupQueries:
    def warmup_payload_preflight(self, schedule_id, starts, before_read):
        from .warmup_payload import warmup_payload_preflight
        warmup_payload_preflight(self.session, schedule_id, starts, before_read)

    def record_aggregate_resume(self, aggregate, schedule, detectors, budget):
        from datetime import timedelta
        from uuid import uuid4

        from ...models import DetectorRecovery
        now = self.now()
        last = (now - schedule.anchor_utc - timedelta(seconds=min(schedule.duration_seconds, 300))) // timedelta(seconds=schedule.duration_seconds) - 1
        for detector in detectors:
            if detector["execution_state"] not in {"requested", "starting", "running"}:
                continue
            first = (detector["consumed_index"] + 1 if detector["consumed_index"] is not None
                else detector["evaluation_start_index"] if detector["evaluation_start_index"] is not None
                else schedule.next_index)
            if first is not None and last >= first:
                self.read_budget(budget.milliseconds())
                self.session.add(DetectorRecovery(id=uuid4(), detector_id=detector["id"],
                    first_index=first, last_index=last, reason="aggregate_resume", started_at=now,
                    coordinator_generation=0))

    def aggregate_command(self, actor, action, key, budget):
        self.read_budget(budget.milliseconds())
        self.idempotency_lock(f"aggregate-command:{actor}:{action}:{key}")
        row = self.session.scalar(select(AggregateCommand).where(AggregateCommand.actor_id == actor,
            AggregateCommand.action == action, AggregateCommand.key == key))
        return document(row) if row else None

    def save_aggregate_command(self, values, budget):
        self.read_budget(budget.milliseconds())
        self.session.add(AggregateCommand(**values))
        self.session.flush()

    def protection_detectors(self, aggregate_id, budget, lock=False):
        result, after, size = [], None, 0
        keys = ("id", "aggregate_id", "name", "config", "config_hash", "feature_keys", "rule_hash",
                "checkpoint", "consumed_index", "evaluation_start_index", "active_attempt_id",
                "execution_state", "execution_generation", "revision", "failure_code", "accelerated", "step_seconds")
        json_size = sum(func.coalesce(func.octet_length(getattr(Detector, key).cast(Text)), 0)
            for key in ("checkpoint", "config", "feature_keys"))
        while True:
            self.read_budget(budget.milliseconds())
            query = select(Detector.id, json_size).where(
                Detector.aggregate_id == aggregate_id).order_by(Detector.id).limit(100)
            if after is not None:
                query = query.where(Detector.id > after)
            headers = self.session.execute(query).all()
            if not headers:
                break
            size += sum(length + 4096 for _, length in headers)
            if size > MAX_SNAPSHOT_BYTES:
                raise EventTimeBudgetError("protection_bytes")
            self.read_budget(budget.milliseconds())
            rows = select(*(getattr(Detector, key) for key in keys)).where(Detector.id.in_([i for i, _ in headers])).order_by(Detector.id)
            if lock:
                rows = rows.with_for_update()
            result.extend(deepcopy(dict(row)) for row in self.session.execute(rows).mappings())
            after = headers[-1][0]
            if len(headers) < 100:
                break
        return result

    def cleanup_count(self, aggregate_id, earliest=None):
        query = select(func.count()).select_from(AggregateClosure).where(
            AggregateClosure.aggregate_id == aggregate_id, AggregateClosure.payload_state == "present")
        if earliest is not None:
            query = query.where(AggregateClosure.window_index >= earliest)
        return self.session.scalar(query)

    def cleanup_tail(self, aggregate_id, offset=0):
        row = self.session.scalar(select(AggregateClosure).where(AggregateClosure.aggregate_id == aggregate_id,
            AggregateClosure.payload_state == "present").order_by(
            AggregateClosure.start_utc.desc(), AggregateClosure.origin_point_id,
            AggregateClosure.point_kind).offset(offset).limit(1))
        return identity(row) if row else None

    def canonical_last_index(self, schedule_id):
        return self.session.scalar(select(func.max(AggregateClosure.window_index)).where(
            AggregateClosure.schedule_id == schedule_id, AggregateClosure.payload_state == "present",
            AggregateClosure.time_basis == "event_time"))

    def prepared_attempt(self, attempt_id, budget):
        if attempt_id is None:
            return None
        self.read_budget(budget.milliseconds())
        size = self.session.scalar(select(func.octet_length(DetectorWarmup.inputs.cast(Text))).where(DetectorWarmup.id == attempt_id))
        if size is not None and size > MAX_SNAPSHOT_BYTES:
            raise EventTimeBudgetError("protection_bytes")
        self.read_budget(budget.milliseconds())
        row = self.session.get(DetectorWarmup, attempt_id, populate_existing=True)
        return document(row) if row else None

    def add_cleanup_operation(self, values, budget):
        self.read_budget(budget.milliseconds())
        self.session.add(AggregateCleanupOperation(**values))
        self.session.flush()

    def cleanup_operation(self, aggregate_id, operation_id, lock=False):
        query = select(AggregateCleanupOperation).where(AggregateCleanupOperation.id == operation_id,
            AggregateCleanupOperation.aggregate_id == aggregate_id)
        if lock:
            query = query.execution_options(populate_existing=True)
        row = self.session.scalar(query.with_for_update() if lock else query)
        return row

    def cleanup_batch(self, aggregate_id, cutoff, budget):
        from sqlalchemy import and_, or_
        at = datetime.fromisoformat(cutoff["start_utc"])
        from uuid import UUID
        point_id = UUID(cutoff["origin_point_id"])
        self.read_budget(min(1000, budget.milliseconds()))
        return list(self.session.scalars(select(AggregateClosure).where(
            AggregateClosure.aggregate_id == aggregate_id, AggregateClosure.payload_state == "present",
            or_(AggregateClosure.start_utc < at,
                and_(AggregateClosure.start_utc == at, AggregateClosure.origin_point_id >= point_id))
            ).order_by(AggregateClosure.start_utc, AggregateClosure.origin_point_id.desc()).limit(256)))

    def compact_point(self, closure, budget):
        """One dependency portion, at most 5,000 modified rows per statement."""
        point_id = closure.origin_point_id
        self.read_budget(min(1000, budget.milliseconds()))
        ids = list(self.session.scalars(select(AggregateMembership.event_id).where(
            AggregateMembership.closure_id == point_id,
            AggregateMembership.fingerprint.is_not(None)).order_by(AggregateMembership.event_id).limit(5000)))
        if ids:
            self.read_budget(min(1000, budget.milliseconds()))
            self.session.execute(update(AggregateMembership).where(AggregateMembership.closure_id == point_id,
                AggregateMembership.event_id.in_(ids)).values(fingerprint=None, event_time=None))
            return False
        self.read_budget(min(1000, budget.milliseconds()))
        outcomes = list(self.session.scalars(select(DetectorOutcome.id).where(
            DetectorOutcome.working_point_id == point_id,
            DetectorOutcome.data["compacted"].as_boolean().is_distinct_from(True)).limit(5000)))
        if outcomes:
            self.read_budget(min(1000, budget.milliseconds()))
            # Thin provenance remains; no features/result copy of the series.
            self.session.execute(update(DetectorOutcome).where(DetectorOutcome.id.in_(outcomes)).values(
                data=func.jsonb_build_object("compacted", True,
                    "start", DetectorOutcome.data["start"], "end", DetectorOutcome.data["end"],
                    "actual_started_at", DetectorOutcome.data["actual_started_at"],
                    "actual_completed_at", DetectorOutcome.data["actual_completed_at"],
                    "late", DetectorOutcome.data["late"], "recovery", DetectorOutcome.data["recovery"])))
            return False
        self.read_budget(min(1000, budget.milliseconds()))
        self.session.execute(delete(AggregateSnapshot).where(AggregateSnapshot.closure_id == point_id))
        self.read_budget(min(1000, budget.milliseconds()))
        model = WorkingWindowPoint if closure.point_kind == "working" else HistoricalAggregatePoint
        self.session.execute(delete(model).where(model.id == point_id))
        closure.payload_state = "cleared"
        return True
