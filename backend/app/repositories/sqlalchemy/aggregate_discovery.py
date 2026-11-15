"""Cycling scans are diagnostic progress, never commit-time watermarks."""

import hashlib
from datetime import UTC

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert

from ...aggregation.event_time import index_at
from ...models import (
    AggregateClosure,
    AggregateHistorySpan,
    AggregateLateEvent,
    AggregateMembership,
    AggregateTimeExclusion,
)
from .aggregate_event_time import classified_events


class SqlAlchemyAggregateDiscovery:
    def scan_page(self, source_id, after, reference, budget):
        events = classified_events(source_id, reference)
        self.read_budget(budget.milliseconds())
        query = select(events.c.event_id, events.c.event_time, events.c.reason, events.c.loaded_at,
            events.c.connection_identity, events.c.kafka_topic_identity,
            events.c.kafka_partition, events.c.kafka_offset).order_by(events.c.event_id).limit(500)
        if after is not None:
            query = query.where(events.c.event_id > after)
        page = [dict(row) for row in self.session.execute(query).mappings()]
        for row in page:
            if row["event_time"] is not None:
                row["event_time"] = row["event_time"].replace(tzinfo=UTC)
        budget.check()
        return page

    def save_scan_page(self, aggregate, schedule, page, reference, budget):
        ids = [r["event_id"] for r in page]
        indices = {index_at(schedule.anchor_utc, schedule.duration_seconds, r["event_time"])
            for r in page if r["reason"] is None}
        self.read_budget(budget.milliseconds())
        closures = {c.window_index: c for c in self.session.scalars(select(AggregateClosure).where(
            AggregateClosure.schedule_id == schedule.id, AggregateClosure.window_index.in_(indices)))}
        self.read_budget(budget.milliseconds())
        members = set(self.session.execute(select(AggregateMembership.closure_id, AggregateMembership.event_id).where(
            AggregateMembership.closure_id.in_([c.origin_point_id for c in closures.values()]),
            AggregateMembership.event_id.in_(ids))).all())
        exclusions, late, valid_ids, spans = [], [], [], {}
        for row in page:
            if row["reason"]:
                exclusions.append({"aggregate_id": aggregate.id, "event_id": row["event_id"],
                    "reason": row["reason"], "detected_at": reference})
                continue
            valid_ids.append(row["event_id"])
            index = index_at(schedule.anchor_utc, schedule.duration_seconds, row["event_time"])
            if index < 0:
                continue
            closed = closures.get(index)
            if closed:
                if (closed.origin_point_id, row["event_id"]) not in members:
                    late.append({"closure_id": closed.origin_point_id, "event_id": row["event_id"],
                        "event_time": row["event_time"], "detected_at": reference,
                        "reason": "event_not_in_closing_snapshot"})
                continue
            if index >= schedule.online_start_index:
                continue
            coordinate = [str(row["connection_identity"]), row["kafka_topic_identity"], row["kafka_partition"]]
            key = hashlib.sha256(repr(coordinate).encode()).hexdigest()
            span = spans.setdefault(key, {"first_index": index, "last_index": index,
                "load_start": row["loaded_at"], "load_end": row["loaded_at"],
                "coordinates": {"connection": coordinate[0], "topic": coordinate[1],
                    "partition": coordinate[2], "first_offset": row["kafka_offset"],
                    "last_offset": row["kafka_offset"]}})
            span["coordinates"]["first_offset"] = min(span["coordinates"]["first_offset"], row["kafka_offset"])
            span["coordinates"]["last_offset"] = max(span["coordinates"]["last_offset"], row["kafka_offset"])
            for name, value, fn in (("first_index", index, min), ("last_index", index, max),
                ("load_start", row["loaded_at"], min), ("load_end", row["loaded_at"], max)):
                span[name] = fn(span[name], value)
        if valid_ids:
            self.read_budget(budget.milliseconds())
            self.session.execute(delete(AggregateTimeExclusion).where(
                AggregateTimeExclusion.aggregate_id == aggregate.id, AggregateTimeExclusion.event_id.in_(valid_ids)))
        if exclusions:
            self.read_budget(budget.milliseconds())
            statement = insert(AggregateTimeExclusion).values(exclusions)
            self.session.execute(statement.on_conflict_do_update(index_elements=["aggregate_id", "event_id"],
                set_={"reason": statement.excluded.reason}))
        if late:
            self.read_budget(budget.milliseconds())
            self.session.execute(insert(AggregateLateEvent).values(late).on_conflict_do_nothing())
        for key, span in spans.items():
            self.read_budget(budget.milliseconds())
            old = self.session.get(AggregateHistorySpan, (aggregate.id, key))
            if old is None:
                self.session.add(AggregateHistorySpan(aggregate_id=aggregate.id, cohort_key=key,
                    next_index=span["first_index"], **span))
            else:
                # A new smaller UUID/date may extend an already scanned span.
                old.next_index = min(old.next_index, span["first_index"])
                old.first_index = min(old.first_index, span["first_index"])
                old.last_index = max(old.last_index, span["last_index"])
                old.load_start = min(old.load_start, span["load_start"])
                old.load_end = max(old.load_end, span["load_end"])
                old.coordinates = {**old.coordinates,
                    "first_offset": min(old.coordinates["first_offset"], span["coordinates"]["first_offset"]),
                    "last_offset": max(old.coordinates["last_offset"], span["coordinates"]["last_offset"])}
        if page:
            schedule.scan_after = page[-1]["event_id"]
        if len(page) < 500:
            schedule.scan_after = None
            schedule.scan_cycle += 1
        budget.check()

    def history_candidate(self, aggregate_id, budget):
        # Anti-join finality avoids re-creating cleared or previously closed data.
        # Discovery may lower a span after a new load; it never lowers next_index
        # past an existing closure as part of a successful calculation.
        self.read_budget(budget.milliseconds())
        span = self.session.scalar(select(AggregateHistorySpan).where(
            AggregateHistorySpan.aggregate_id == aggregate_id,
            AggregateHistorySpan.next_index <= AggregateHistorySpan.last_index
        ).order_by(AggregateHistorySpan.next_index, AggregateHistorySpan.cohort_key).limit(1))
        return span

    def history_matches(self, aggregate_id, key, index):
        span = self.session.get(AggregateHistorySpan, (aggregate_id, key), populate_existing=True)
        return span is not None and span.next_index == index and index <= span.last_index

    def advance_history(self, aggregate_id, key, index):
        span = self.session.get(AggregateHistorySpan, (aggregate_id, key), populate_existing=True)
        if span and span.next_index == index:
            span.next_index += 1

    def diagnostics(self, aggregate_id):
        self.read_budget(5000)
        exclusions = dict(self.session.execute(select(AggregateTimeExclusion.reason, func.count()).where(
            AggregateTimeExclusion.aggregate_id == aggregate_id).group_by(AggregateTimeExclusion.reason)).all())
        count, first, last = self.session.execute(select(func.count(), func.min(AggregateLateEvent.event_time),
            func.max(AggregateLateEvent.event_time)).join(AggregateClosure,
                AggregateClosure.origin_point_id == AggregateLateEvent.closure_id).where(
                AggregateClosure.aggregate_id == aggregate_id)).one()
        return {"exclusions": exclusions, "late_count": count, "late_start": first,
                "late_end": last, "late_reason": "event_not_in_closing_snapshot" if count else None}
