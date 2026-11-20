"""Bounded event-time reads. The caller owns the read-only RR and deadline."""

from datetime import UTC
from time import perf_counter

from sqlalchemy import Text, and_, case, func, or_, select, true

from ...aggregation.event_time import (
    MAX_EVENTS,
    MAX_MEMORY_BYTES,
    MAX_SNAPSHOT_BYTES,
    EventTimeBudgetError,
)
from ...models import (
    AggregateClosure,
    AggregateMembership,
    AggregateSnapshot,
    KafkaCheckpoint,
    KafkaOperationalEvent,
    ParsedLog,
    ProcessedKafkaRecord,
    SourceInterruption,
)
from .aggregate_compiler import aggregate_statement, read_statistics


def classified_events(source_id, reference_at):
    """Exact UTC carrier avoids session-TZ overflows at years 1 and 9999."""
    value = ParsedLog.event_time_utc
    # This FK identifies at most one durable record. Keep that bounded lookup
    # explicit: with freshly loaded sources PostgreSQL may estimate both sides
    # as one row and choose an unparameterized nested-loop full index scan.
    record = select(ProcessedKafkaRecord.id, ProcessedKafkaRecord.source_id,
        ProcessedKafkaRecord.connection_identity, ProcessedKafkaRecord.kafka_topic_identity,
        ProcessedKafkaRecord.kafka_topic, ProcessedKafkaRecord.kafka_partition,
        ProcessedKafkaRecord.kafka_offset, ProcessedKafkaRecord.result_status).where(
        ProcessedKafkaRecord.id == ParsedLog.processed_record_id).correlate(ParsedLog).limit(1).lateral()
    proven = and_(record.c.id == ParsedLog.processed_record_id,
        record.c.source_id == ParsedLog.source_id,
        record.c.connection_identity == ParsedLog.connection_id,
        record.c.kafka_topic == ParsedLog.kafka_topic,
        record.c.kafka_partition == ParsedLog.kafka_partition,
        record.c.kafka_offset == ParsedLog.kafka_offset,
        record.c.result_status.in_(["complete", "partial"]))
    eligible = and_(proven, ParsedLog.event_time_reason.is_(None),
        value <= reference_at.astimezone(UTC).replace(tzinfo=None))
    reason = case((~func.coalesce(proven, False), "event_time_unknown_provenance"),
        (ParsedLog.event_time_reason.is_not(None), ParsedLog.event_time_reason),
        (value > reference_at.astimezone(UTC).replace(tzinfo=None), "event_time_future"),
        else_=None)
    pair = func.jsonb_build_array(ParsedLog.ecs_data, ParsedLog.normalization_diagnostics).cast(Text)
    return select(ParsedLog.id.label("event_id"), ParsedLog.ecs_data,
        value.label("event_time"), reason.label("reason"),
        eligible.label("eligible"),
        ParsedLog.event_time_reason.label("static_reason"),
        func.octet_length(pair).label("json_bytes"),
        func.encode(func.sha256(func.convert_to(pair, "UTF8")), "hex").label("fingerprint"),
        ParsedLog.backend_processed_at.label("loaded_at"),
        record.c.connection_identity, record.c.kafka_topic_identity,
        ParsedLog.kafka_partition, ParsedLog.kafka_offset,
    ).select_from(ParsedLog).outerjoin(record, true()).where(ParsedLog.source_id == source_id).subquery()


def eligible_events(source_id, start, end, reference_at):
    events = classified_events(source_id, reference_at)
    # Keep this explicit predicate aligned with the partial range index. Dynamic
    # provenance/future checks remain additional requirements in the same RR.
    return select(events).where(events.c.static_reason.is_(None), events.c.eligible,
        events.c.event_time >= start.astimezone(UTC).replace(tzinfo=None),
        events.c.event_time < end.astimezone(UTC).replace(tzinfo=None)).subquery()


class SqlAlchemyEventTimeQueries:
    def event_cohorts(self, source_id, start, end, reference_at, budget, memory_limit):
        """Offsets describe delivery, never the chronological order of events."""
        events = classified_events(source_id, reference_at)
        groups = select(events.c.connection_identity.label("connection"),
            events.c.kafka_topic_identity.label("topic"), events.c.kafka_partition.label("partition"),
            func.min(events.c.kafka_offset).label("first_offset"),
            func.max(events.c.kafka_offset).label("last_offset"),
            func.min(events.c.loaded_at).label("load_start"),
            func.max(events.c.loaded_at).label("load_end")).where(events.c.eligible).group_by(
                events.c.connection_identity, events.c.kafka_topic_identity, events.c.kafka_partition).having(
                func.min(events.c.event_time) < end.astimezone(UTC).replace(tzinfo=None),
                func.max(events.c.event_time) >= start.astimezone(UTC).replace(tzinfo=None)).subquery()
        self.read_budget(budget.milliseconds())
        memory = self.session.scalar(select(func.coalesce(func.sum(
            2048 + 8 * func.coalesce(func.octet_length(groups.c.topic), 0)), 0)))
        if memory > memory_limit:
            raise EventTimeBudgetError("total_memory")
        result = []
        self.read_budget(budget.milliseconds())
        rows = self.session.execute(select(groups).execution_options(yield_per=500)).mappings()
        try:
            while True:
                self.read_budget(budget.milliseconds())
                page = rows.fetchmany(500)
                if not page:
                    break
                for row in page:
                    result.append({**row, "connection": str(row["connection"])})
                budget.check()
        finally:
            rows.close()
        return result

    def event_source_rows(self, source_id, budget):
        self.read_budget(budget.milliseconds())
        return self.session.scalar(select(func.count()).select_from(ParsedLog).where(ParsedLog.source_id == source_id))

    def event_base(self, source_id, start, end, reference_at):
        events = eligible_events(source_id, start, end, reference_at)
        return select(events.c.ecs_data, events.c.event_time).subquery()

    def snapshot_events(self, source_id, start, end, reference_at, budget, *, preview=False, timings=None):
        began = perf_counter()
        eligible = eligible_events(source_id, start, end, reference_at)
        self.read_budget(budget.milliseconds())
        rows, size, width = self.session.execute(select(func.count(),
            func.coalesce(func.sum(eligible.c.json_bytes + 160), 0),
            func.coalesce(func.max(eligible.c.json_bytes), 0)).select_from(eligible)).one()
        if rows > MAX_EVENTS:
            raise EventTimeBudgetError("membership_events")
        if size > MAX_SNAPSHOT_BYTES:
            raise EventTimeBudgetError("snapshot_bytes")
        if rows == 0:
            # The count and every subsequent read share the caller's RR snapshot.
            # Quality and feature evaluation still run in the snapshot service.
            budget.check()
            if timings is not None:
                timings["selection"] = perf_counter() - began
                timings["manifest"] = 0.0
            return [], [], 0, int(width) + 64
        cohorts_sql = select(eligible.c.connection_identity, eligible.c.kafka_topic_identity,
            eligible.c.kafka_partition, func.min(eligible.c.kafka_offset).label("first_offset"),
            func.max(eligible.c.kafka_offset).label("last_offset"),
            func.min(eligible.c.loaded_at).label("load_start"),
            func.max(eligible.c.loaded_at).label("load_end")).group_by(eligible.c.connection_identity,
                eligible.c.kafka_topic_identity, eligible.c.kafka_partition).subquery()
        self.read_budget(budget.milliseconds())
        cohort_memory = self.session.scalar(select(func.coalesce(func.sum(
            1536 + 4 * func.coalesce(func.octet_length(cohorts_sql.c.kafka_topic_identity), 0)), 0)))
        # Include Python manifests, coordinate dictionaries and transient JSON
        # serialization before fetching either collection across the driver.
        if rows * 512 + cohort_memory + size > MAX_MEMORY_BYTES:
            raise EventTimeBudgetError("total_memory")
        self.read_budget(budget.milliseconds())
        cohorts = [{"connection": str(row.connection_identity), "topic": row.kafka_topic_identity,
            "partition": row.kafka_partition, "first_offset": row.first_offset,
            "last_offset": row.last_offset, "load_start": row.load_start, "load_end": row.load_end}
            for row in self.session.execute(select(cohorts_sql))]
        if timings is not None:
            timings["selection"] = perf_counter() - began
        began = perf_counter()
        manifest = []
        if not preview:
            # One bounded cursor selects membership once. A LIMIT/keyset query
            # per page would repeatedly select/classify the same large window.
            self.read_budget(budget.milliseconds())
            cursor = self.session.execute(select(eligible.c.event_id, eligible.c.event_time,
                eligible.c.fingerprint).order_by(eligible.c.event_id).execution_options(yield_per=500)).mappings()
            try:
                while len(manifest) < rows:
                    self.read_budget(budget.milliseconds())
                    page = cursor.fetchmany(500)
                    if not page or len(manifest) + len(page) > rows:
                        budget.check()
                        raise RuntimeError("aggregate_manifest_count_mismatch")
                    manifest.extend({"event_id": row["event_id"],
                        "event_time": row["event_time"].replace(tzinfo=UTC),
                        "fingerprint": row["fingerprint"]} for row in page)
                    budget.check()
            finally:
                cursor.close()
        if timings is not None:
            timings["manifest"] = perf_counter() - began
        budget.check()
        return manifest, cohorts, int(rows), int(width) + 64

    def event_statistics(self, source_id, start, end, reference_at, compiled, budget):
        self.read_budget(budget.milliseconds())
        base = self.event_base(source_id, start, end, reference_at)
        result = read_statistics(self.session.execute(aggregate_statement(base, compiled)).mappings().one(), compiled)
        budget.check()
        return result

    def received_facts(self, source_id, load_start, load_end, reference_at, budget, coordinates=()):
        self.read_budget(budget.milliseconds())
        stopped = list(self.session.scalars(select(SourceInterruption.id).where(
            SourceInterruption.source_identity == source_id, SourceInterruption.started_at < load_end,
            (SourceInterruption.ended_at.is_(None) | (SourceInterruption.ended_at > load_start))).limit(501)))
        self.read_budget(budget.milliseconds())
        errors = self.session.scalar(select(func.count()).select_from(ProcessedKafkaRecord).where(
            ProcessedKafkaRecord.source_id == source_id, ProcessedKafkaRecord.backend_processed_at >= load_start,
            ProcessedKafkaRecord.backend_processed_at <= load_end, ProcessedKafkaRecord.result_status == "failed"))
        self.read_budget(budget.milliseconds())
        gaps = set()
        from uuid import UUID
        for offset in range(0, max(1, len(coordinates)), 100):
            self.read_budget(budget.milliseconds())
            query = select(KafkaOperationalEvent.id).where(KafkaOperationalEvent.source_identity == source_id,
                KafkaOperationalEvent.detected_at <= reference_at)
            page = coordinates[offset:offset + 100]
            if page:
                query = query.where(or_(*(and_(KafkaOperationalEvent.connection_identity == UUID(c["connection"]),
                    KafkaOperationalEvent.old_topic_identity == c["topic"],
                    or_(KafkaOperationalEvent.kind == "topic_recreated", and_(
                        KafkaOperationalEvent.kafka_partition == c["partition"],
                        KafkaOperationalEvent.offset_start <= c["last_offset"],
                        KafkaOperationalEvent.offset_end > c["first_offset"]))) for c in page)))
            else:
                query = query.where(KafkaOperationalEvent.detected_at >= load_start)
            gaps.update(self.session.scalars(query.limit(501)))
            if len(gaps) > 500:
                raise EventTimeBudgetError("evidence_records")
        self.read_budget(budget.milliseconds())
        events = classified_events(source_id, reference_at)
        exclusions = dict(self.session.execute(select(events.c.reason, func.count()).where(
            events.c.reason.is_not(None), events.c.loaded_at >= load_start,
            events.c.loaded_at <= load_end).group_by(events.c.reason)).all())
        self.read_budget(budget.milliseconds())
        checkpoint_query = select(KafkaCheckpoint).where(
            KafkaCheckpoint.source_identity == source_id, KafkaCheckpoint.observed_at <= reference_at,
            KafkaCheckpoint.observed_at <= load_end,
            KafkaCheckpoint.observed_at >= load_start)
        if coordinates:
            checkpoint_query = checkpoint_query.where(KafkaCheckpoint.topic_identity.in_({c["topic"] for c in coordinates if c["topic"]}))
        checkpoint = self.session.scalar(checkpoint_query.order_by(KafkaCheckpoint.observed_at.desc()).limit(1))
        backlog = False
        if checkpoint:
            backlog = any(not isinstance(v, dict) or v.get("high") != v.get("committed")
                or v.get("low", 0) > v.get("committed", 0) for v in checkpoint.partitions.values())
        budget.check()
        if len(stopped) > 500 or len(gaps) > 500:
            raise EventTimeBudgetError("evidence_records")
        return {"interruptions": [str(x) for x in stopped], "gaps": [str(x) for x in gaps],
            "processing_failures": errors, "exclusions": exclusions, "backlog": backlog,
            "checkpoint": str(checkpoint.id) if checkpoint else None}

    def closure_at(self, schedule_id, index):
        return self.session.scalar(select(AggregateClosure).where(
            AggregateClosure.schedule_id == schedule_id, AggregateClosure.window_index == index))

    def closure_by_id(self, point_id):
        return self.session.get(AggregateClosure, point_id)

    def payload(self, closure):
        from ...models import HistoricalAggregatePoint, WorkingWindowPoint
        model = WorkingWindowPoint if closure.point_kind == "working" else HistoricalAggregatePoint
        rows = self._point_rows(select(model).where(model.id == closure.origin_point_id), model)
        return rows[0] if rows else None

    def snapshot_metadata(self, point_id):
        return self.session.scalar(select(AggregateSnapshot.metadata_json).where(
            AggregateSnapshot.closure_id == point_id)) or {}

    def add_event_point(self, point, closure, membership, metadata, budget):
        self.read_budget(budget.milliseconds())
        self.session.add(closure)
        self.session.flush()
        self.read_budget(budget.milliseconds())
        self.session.add(point)
        if closure.calculation_status == "computed":
            self.session.add(AggregateSnapshot(closure_id=point.id, metadata_json=metadata))
        self.session.flush()
        if membership:
            statement = AggregateMembership.__table__.insert().values(closure_id=point.id)
            for offset in range(0, len(membership), 500):
                self.read_budget(budget.milliseconds())
                self.session.execute(statement, membership[offset:offset + 500])
        budget.check()
