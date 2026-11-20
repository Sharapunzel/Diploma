"""Real-writer membership identity, bounded I/O and atomic publication regressions."""

import hashlib
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4, uuid5

import pytest
from app.aggregate_composition import build_aggregate_service
from app.aggregation.event_time import EventTimeBudgetError, OperationBudget, bounds, close_deadline
from app.models import (
    AggregateClosure,
    AggregateMembership,
    AggregateSchedule,
    AggregateSnapshot,
    ParsedLog,
    ProcessedKafkaRecord,
    SourceInterruption,
    WorkingWindowPoint,
)
from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
from app.services.implementations.aggregate_snapshot import AggregateSnapshotService
from sqlalchemy import Text, event, func, select, text
from sqlalchemy.sql import Insert, Select
from test_event_time_pipeline import context, create, migrated, turns, write_event

__all__ = ["context", "migrated"]

ANCHOR = datetime(2000, 1, 1, tzinfo=UTC)
BOUNDARIES = [0, 499, 500, 501, 1000]


def seed_members(context, count):
    """Use the accepted atomic writer; preserve offset timestamps and Unicode JSON."""
    database, _, source, connection = context
    timestamps = {}
    now = datetime.now(UTC)
    with database.session_factory.begin() as session:
        writer = SqlAlchemyProcessingRepository(session)
        for offset in range(count):
            identity = uuid4()
            at = ANCHOR + timedelta(seconds=1, microseconds=997 * offset)
            timestamps[identity] = at
            record = ProcessedKafkaRecord(id=uuid4(), connection_id=connection,
                connection_identity=connection, kafka_topic_identity="membership-io",
                source_id=source, kafka_topic=str(source), kafka_partition=0, kafka_offset=offset,
                result_status="complete", backend_received_at=now, backend_processed_at=now)
            event = ParsedLog(id=identity, source_id=source, connection_id=connection,
                normalizer_version=1, normalizer_name="Synthetic membership I/O",
                source_name="Synthetic", connection_name="Synthetic", kafka_topic=str(source),
                kafka_partition=0, kafka_offset=offset, deduplication_key=str(identity),
                fluent_bit_collected_at=now, backend_received_at=now, backend_processed_at=now,
                raw="synthetic", normalization_status="complete",
                normalization_diagnostics=[{"code": "synthetic_note", "message": "данные ✓"}],
                ecs_data={"@timestamp": at.astimezone(timezone(timedelta(hours=5, minutes=30))).isoformat(),
                    "network": {"bytes": 2**60 + offset}, "tags": ["synthetic", "данные"]})
            assert writer.store_event(record, event)
    # Hash PostgreSQL's exact JSONB-pair text independently in Python, rather
    # than obtaining expected fingerprints from snapshot_events itself.
    with database.session_factory() as session:
        pairs = session.execute(select(ParsedLog.id,
            func.jsonb_build_array(ParsedLog.ecs_data, ParsedLog.normalization_diagnostics).cast(Text)).where(
                ParsedLog.source_id == source)).all()
    expected = {identity: {"event_id": identity, "event_time": timestamps[identity],
        "fingerprint": hashlib.sha256(pair.encode("utf-8")).hexdigest()} for identity, pair in pairs}
    return [expected[identity] for identity in sorted(expected)]


class ManifestCursor:
    def __init__(self, result, page_limit, stop_after):
        self.result = result
        self.page_limit, self.stop_after = page_limit, stop_after
        self.requests, self.lengths = [], []
        self.returned, self.closed = 0, 0

    def mappings(self):
        self.result = self.result.mappings()
        return self

    def fetchmany(self, size):
        self.requests.append(size)
        limit = min(size, self.page_limit)
        if self.stop_after is not None:
            limit = min(limit, max(0, self.stop_after - self.returned))
        # Only negative EOF tests hide a result. Positive cases always read
        # the real cursor, including the deliberately short-page scenario.
        page = [] if limit == 0 else self.result.fetchmany(limit)
        self.returned += len(page)
        self.lengths.append(len(page))
        return page

    def close(self):
        self.closed += 1
        self.result.close()


@contextmanager
def observe_manifest(repository, monkeypatch, *, page_limit=500, stop_after=None, after_count=None):
    probe = SimpleNamespace(timeouts=[], cursors=[], reads=[])
    original_execute, original_budget = repository.session.execute, repository.read_budget

    def read(state):
        if not isinstance(state.statement, Select):
            return
        keys = tuple(state.statement.selected_columns.keys())
        if keys and not keys[0].startswith("set_config"):
            probe.reads.append(state.statement)

    def execute(statement, *args, **kwargs):
        result = original_execute(statement, *args, **kwargs)
        if after_count is not None and probe.reads and statement is probe.reads[0]:
            after_count()
        if isinstance(statement, Select) and tuple(statement.selected_columns.keys()) == (
                "event_id", "event_time", "fingerprint"):
            cursor = ManifestCursor(result, page_limit, stop_after)
            probe.cursors.append(cursor)
            return cursor
        return result

    def read_budget(milliseconds):
        probe.timeouts.append(milliseconds)
        return original_budget(milliseconds)

    event.listen(repository.session, "do_orm_execute", read)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(repository.session, "execute", execute)
            patch.setattr(repository, "read_budget", read_budget)
            yield probe
    finally:
        event.remove(repository.session, "do_orm_execute", read)


def read_manifest(context, monkeypatch, *, preview=False, page_limit=500, start=ANCHOR):
    database, catalog, source, _ = context
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        with api.uow.read_snapshot():
            assert session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
            assert session.scalar(text("SHOW transaction_read_only")) == "on"
            reference = api.repository.now()
            with observe_manifest(api.repository, monkeypatch, page_limit=page_limit) as probe:
                manifest, probe.cohorts, rows, probe.width = api.repository.snapshot_events(
                    source, start, start + timedelta(seconds=300), reference, OperationBudget(10),
                    preview=preview)
    return manifest, rows, probe


@pytest.mark.parametrize("count", BOUNDARIES)
def test_snapshot_known_count_avoids_terminal_empty_fetch(context, monkeypatch, count):
    expected = seed_members(context, count)
    manifest, rows, probe = read_manifest(context, monkeypatch)
    assert rows == count and manifest == expected
    assert all(row["event_time"].utcoffset() == timedelta(0) for row in manifest)
    if count == 0:
        assert len(probe.reads) == len(probe.timeouts) == 1
        assert probe.cohorts == probe.cursors == []
        assert probe.width == 64
        return
    assert len(probe.cursors) == 1
    cursor = probe.cursors[0]
    assert cursor.closed == 1
    pages = (count + 499) // 500
    assert cursor.requests == [500] * pages, "Known RR count must not require a terminal empty fetch"
    assert cursor.lengths == [min(500, count - offset) for offset in range(0, count, 500)]
    assert len(probe.timeouts) == 4 + pages, "Keep every real-read checkpoint; omit only the empty-fetch checkpoint"
    assert all(0 < timeout <= 10000 for timeout in probe.timeouts)


def test_empty_preview_reads_only_eligible_count_with_other_events_in_source(context, monkeypatch):
    seed_members(context, 1)
    manifest, rows, probe = read_manifest(context, monkeypatch, preview=True,
        start=ANCHOR + timedelta(seconds=300))
    assert rows == 0 and manifest == probe.cohorts == probe.cursors == []
    assert probe.width == 64 and len(probe.reads) == len(probe.timeouts) == 1


def test_empty_selection_still_checks_deadline_after_the_real_count(context, monkeypatch):
    database, catalog, source, _ = context
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        now = [0.0]
        budget = OperationBudget(1, clock=lambda: now[0])

        def expire():
            now[0] = budget.deadline

        with api.uow.read_snapshot():
            reference = api.repository.now()
            with (observe_manifest(api.repository, monkeypatch, after_count=expire) as probe,
                    pytest.raises(EventTimeBudgetError, match="operation_seconds")):
                api.repository.snapshot_events(source, ANCHOR, ANCHOR + timedelta(seconds=300),
                    reference, budget)
        assert len(probe.reads) == len(probe.timeouts) == 1 and probe.cursors == []
        assert now[0] == budget.deadline and not session.in_transaction()


def test_snapshot_short_pages_continue_until_exact_known_count(context, monkeypatch):
    expected = seed_members(context, 501)
    manifest, rows, probe = read_manifest(context, monkeypatch, page_limit=249)
    assert rows == 501 and manifest == expected
    cursor = probe.cursors[0]
    assert cursor.closed == 1
    assert cursor.requests == [500, 500, 500]
    assert cursor.lengths == [249, 249, 3]
    assert len(probe.timeouts) == 7


@pytest.mark.parametrize("stop_after", [0, 500], ids=["before-first", "after-500"])
def test_snapshot_early_eof_fails_closed_and_rolls_back_rr(context, monkeypatch, stop_after):
    expected = seed_members(context, 501)
    database, catalog, source, _ = context
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        with (
            observe_manifest(api.repository, monkeypatch, stop_after=stop_after) as probe,
            pytest.raises(RuntimeError),
            api.uow.read_snapshot(),
        ):
            api.repository.snapshot_events(source, ANCHOR, ANCHOR + timedelta(seconds=300),
                api.repository.now(), OperationBudget(10))
        assert not session.in_transaction()
        assert probe.cursors[0].closed == 1
        assert probe.cursors[0].returned == stop_after
    with database.session_factory() as session:
        assert session.scalar(select(func.count()).select_from(ParsedLog).where(ParsedLog.source_id == source)) == len(expected)
        assert session.scalar(select(func.count()).select_from(AggregateClosure).where(AggregateClosure.source_identity == source)) == 0


def test_snapshot_preview_keeps_count_without_materializing_membership(context, monkeypatch):
    seed_members(context, 501)
    manifest, rows, probe = read_manifest(context, monkeypatch, preview=True)
    assert rows == 501 and manifest == []
    assert probe.cursors == [] and len(probe.timeouts) == 3
    assert len(probe.reads) == 3 and len(probe.cohorts) == 1 and probe.width > 64


def assert_empty_values(result, quality, reason):
    assert result.event_count == 0
    assert result.event_time_min is result.event_time_max is None
    assert (result.completeness, result.completeness_reason) == (quality, reason)
    assert [(feature.key, feature.value, feature.reason) for feature in result.features] == [
        ("n", 0, None), ("bytes", None, "no_values")]


def assert_empty_storage(session, point, origin):
    closure = session.get(AggregateClosure, point.id)
    payload = session.get(WorkingWindowPoint, point.id)
    snapshot = session.get(AggregateSnapshot, point.id)
    assert closure.origin == origin and closure.payload_state == "present"
    assert closure.calculation_status == payload.calculation_status == point.calculation_status == "computed"
    assert closure.snapshot_reference_at == point.snapshot_reference_at
    assert snapshot.metadata_json == payload.evidence_refs == point.evidence_refs
    assert snapshot.metadata_json["membership_count"] == 0
    assert session.scalars(select(AggregateMembership.event_id).where(
        AggregateMembership.closure_id == point.id)).all() == []


@pytest.mark.parametrize("interrupted", [False, True])
def test_empty_online_point_keeps_quality_and_publishes_a_single_closure(context, monkeypatch, interrupted):
    database, catalog, source, _ = context
    card, _ = create(context, ANCHOR)
    start, end = bounds(ANCHOR, 300, card.next_index)
    deadline = close_deadline(end, 300)
    interruption = uuid4()
    if interrupted:
        with database.session_factory.begin() as session:
            session.add(SourceInterruption(id=interruption, source_identity=source,
                kind="processing_stopped", started_at=start + timedelta(seconds=1),
                ended_at=start + timedelta(seconds=2)))
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        # Advance just past this window's close deadline, with the next window still open.
        monkeypatch.setattr(api.repository, "now", lambda: deadline + timedelta(seconds=1))
        for _ in range(6):
            result = api.advance(card.id)
            if result["state"] == "closed":
                break
        assert result["state"] == "closed" and result["origin"] == "online"
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        point = api.point_at(card.id, card.next_index)
        expected = ("interrupted", "known_source_or_processing_stop") if interrupted else ("complete", "received_event_snapshot")
        assert_empty_values(point, *expected)
        assert_empty_storage(session, point, "online")
        assert point.evidence_refs["coordinates"] == []
        assert point.evidence_refs["facts"]["interruptions"] == ([str(interruption)] if interrupted else [])
        assert point.close_not_before_at == deadline
        assert session.get(AggregateSchedule, card.schedule_id).next_index == card.next_index + 1
        assert session.scalars(select(AggregateClosure.origin_point_id).where(
            AggregateClosure.aggregate_id == card.id)).all() == [point.id]


def test_empty_preview_and_initial_history_keep_separate_delivery_cohort(context):
    database, catalog, _, connection = context
    first = write_event(context, (ANCHOR + timedelta(seconds=1)).isoformat())
    last = write_event(context, (ANCHOR + timedelta(seconds=601)).isoformat(), offset=1)
    card, request = create(context, ANCHOR)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        preview = api.preview(request.model_copy(update={"window_start_utc": ANCHOR + timedelta(seconds=300)}))
        assert_empty_values(preview, "complete", "received_event_snapshot")
        assert preview.snapshot_metadata["membership_count"] == 0
        assert len(preview.snapshot_metadata["coordinates"]) == 1
        metadata = preview.snapshot_metadata["coordinates"][0]
        assert (metadata["connection"], metadata["partition"], metadata["first_offset"], metadata["last_offset"]) == (
            str(connection), 0, 0, 1)
        assert session.scalars(select(AggregateClosure.origin_point_id).where(
            AggregateClosure.aggregate_id == card.id)).all() == []
        assert session.get(AggregateSchedule, card.schedule_id).next_index == card.next_index
    turns(context, card.id)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        point = api.point_at(card.id, 1)
        assert_empty_values(point, "complete", "received_event_snapshot")
        assert_empty_storage(session, point, "initial_history")
        assert metadata in point.evidence_refs["coordinates"]
        assert session.get(AggregateSchedule, card.schedule_id).next_index == card.next_index
        for index, event in ((0, first), (2, last)):
            filled = api.point_at(card.id, index)
            assert filled.event_count == 1 and filled.features[1].value == 2**60 + 1
            assert session.scalars(select(AggregateMembership.event_id).where(
                AggregateMembership.closure_id == filled.id)).all() == [event]


def storage_candidate(context, count):
    expected = seed_members(context, count)
    card, request = create(context, ANCHOR)
    database, catalog, source, _ = context
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        candidate = AggregateSnapshotService(api.repository, api.uow, catalog, api.budget_limits).build(
            source, request.aggregate_rule, ANCHOR, ANCHOR + timedelta(seconds=300), 300,
            "initial_history", None, OperationBudget(10))
    assert candidate["manifest"] == expected
    return card, candidate, expected


def storage_rows(card, candidate):
    identity = uuid5(card.schedule_id, "event-time-window:0")
    end = ANCHOR + timedelta(seconds=300)
    point = WorkingWindowPoint(id=identity, schedule_id=card.schedule_id, window_index=0,
        start_utc=ANCHOR, end_utc=end, scheduled_at=end, actual_started_at=candidate["reference"],
        actual_completed_at=candidate["reference"], time_basis="event_time",
        completeness=candidate["completeness"], completeness_reason=candidate["completeness_reason"],
        evidence_refs=candidate["metadata"],
        **{key: value for key, value in candidate["values"].items() if key != "observed_rows"})
    closure = AggregateClosure(origin_point_id=identity, aggregate_id=card.id, schedule_id=card.schedule_id,
        source_identity=card.source_id, window_index=0, start_utc=ANCHOR, end_utc=end,
        time_basis="event_time", origin="initial_history", snapshot_reference_at=candidate["reference"],
        closed_at=candidate["reference"], close_not_before_at=end, payload_state="present", point_kind="working")
    return point, closure


@contextmanager
def observe_inserts(session, monkeypatch, *, fail_after_first=False, closure_id=None):
    probe = SimpleNamespace(statements=[], batches=[], staged=None)
    original_execute = session.execute

    def execute(statement, *args, **kwargs):
        if not isinstance(statement, Insert) or statement.table is not AggregateMembership.__table__:
            return original_execute(statement, *args, **kwargs)
        parameters = args[0] if args else kwargs["params"]
        probe.statements.append(statement)
        probe.batches.append(tuple(parameters))
        result = original_execute(statement, *args, **kwargs)
        if fail_after_first:
            assert closure_id is not None
            probe.staged = original_execute(select(func.count()).select_from(AggregateMembership).where(
                AggregateMembership.closure_id == closure_id)).scalar_one()
            raise RuntimeError("synthetic failure after actual membership INSERT")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(session, "execute", execute)
        yield probe


@pytest.mark.parametrize("count", BOUNDARIES)
def test_membership_save_preserves_exact_rows_and_reuses_input_and_statement(context, monkeypatch, count):
    card, candidate, expected = storage_candidate(context, count)
    database, catalog, _, _ = context
    original_manifest = deepcopy(candidate["manifest"])
    point, closure = storage_rows(card, candidate)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        api.repository.schedule(card.schedule_id, True)
        with observe_inserts(session, monkeypatch) as probe:
            api.repository.add_event_point(point, closure, candidate["manifest"], candidate["metadata"], OperationBudget(10))
        assert candidate["manifest"] == original_manifest
        api.uow.commit()
        assert candidate["manifest"] == original_manifest
    with database.session_factory() as session:
        actual = [dict(row) for row in session.execute(select(AggregateMembership.event_id,
            AggregateMembership.event_time, AggregateMembership.fingerprint).where(
                AggregateMembership.closure_id == point.id).order_by(AggregateMembership.event_id)).mappings()]
        assert actual == expected
        assert session.get(WorkingWindowPoint, point.id).event_count == count
        assert session.get(AggregateClosure, point.id).payload_state == "present"
        assert session.get(AggregateSnapshot, point.id).metadata_json == candidate["metadata"]
    assert [len(batch) for batch in probe.batches] == [min(500, count - offset) for offset in range(0, count, 500)]
    transmitted = [row for batch in probe.batches for row in batch]
    regressions = []
    if not all(actual is original for actual, original in zip(transmitted, candidate["manifest"], strict=True)):
        regressions.append("Every membership mapping was copied merely to repeat closure_id")
    if probe.statements and not all(statement is probe.statements[0] for statement in probe.statements):
        regressions.append("The same INSERT was constructed again for each 500-row portion")
    assert regressions == [], regressions


def test_membership_write_failure_rolls_back_all_payload_and_preserves_input(context, monkeypatch):
    card, candidate, expected = storage_candidate(context, 501)
    database, catalog, source, _ = context
    original_manifest = deepcopy(candidate["manifest"])
    point, closure = storage_rows(card, candidate)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        api.repository.schedule(card.schedule_id, True)
        with observe_inserts(session, monkeypatch, fail_after_first=True, closure_id=point.id) as probe:
            try:
                with pytest.raises(RuntimeError, match="synthetic failure"):
                    api.repository.add_event_point(point, closure, candidate["manifest"], candidate["metadata"], OperationBudget(10))
            finally:
                api.uow.rollback()
        assert probe.staged == 500 and [len(batch) for batch in probe.batches] == [500]
        assert candidate["manifest"] == original_manifest
    with database.session_factory() as session:
        for model in (AggregateClosure, WorkingWindowPoint, AggregateSnapshot):
            assert session.get(model, point.id) is None
        assert session.scalar(select(func.count()).select_from(AggregateMembership).where(
            AggregateMembership.closure_id == point.id)) == 0
        assert session.scalar(select(func.count()).select_from(ParsedLog).where(ParsedLog.source_id == source)) == len(expected)
