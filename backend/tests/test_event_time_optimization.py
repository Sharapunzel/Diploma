"""Real-writer exact snapshots and selective query-plan/performance checks."""

import platform
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
import sqlalchemy
from sqlalchemy import event, func, select, text, update
from sqlalchemy.dialects import postgresql
from test_event_time_pipeline import context, create, migrated, turns, write_event

from app.aggregate_composition import build_aggregate_service
from app.aggregation.event_time import EventTimeBudgetError, OperationBudget, event_timestamp
from app.aggregation.results import interpret_result
from app.aggregation.rules import AggregateRule, Limits, compile_rule
from app.aggregation.worker_admission import FOREGROUND_BURST
from app.models import (
    AggregateClosure,
    AggregateHistorySpan,
    AggregateLateEvent,
    AggregateMembership,
    AggregateSchedule,
    AvailabilityMarker,
    ParsedLog,
    ProcessedKafkaRecord,
    Source,
)
from app.repositories.sqlalchemy.aggregate_compiler import _scalar, aggregate_statement
from app.repositories.sqlalchemy.aggregate_event_time import classified_events
from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
from app.schemas.aggregates import AggregatePreviewRequest, CreateAggregateRequest
from app.services.implementations.aggregate_snapshot import AggregateSnapshotService

# Bind fixtures locally even when their provider was collected first.
__all__ = ["context", "migrated"]


def test_projection_record_event_and_marker_roll_back_together(context, monkeypatch):
    database, _, _, _ = context
    original, identities = SqlAlchemyProcessingRepository.store_event, []

    def fail_after_atomic_write(repository, record, event):
        assert original(repository, record, event)
        identities.extend([record.id, event.id])
        assert repository.session.scalar(select(ParsedLog.event_time_utc).where(
            ParsedLog.id == event.id)) == datetime(2000, 1, 1, tzinfo=UTC).replace(tzinfo=None)
        assert repository.session.scalar(select(func.count()).select_from(AvailabilityMarker).where(
            AvailabilityMarker.event_id == event.id)) == 1
        raise RuntimeError("synthetic rollback after complete atomic write")

    with monkeypatch.context() as patch:
        patch.setattr(SqlAlchemyProcessingRepository, "store_event", fail_after_atomic_write)
        with pytest.raises(RuntimeError, match="synthetic rollback"):
            write_event(context, "2000-01-01T00:00:00Z")
    record_id, event_id = identities
    with database.session_factory() as session:
        assert session.get(ProcessedKafkaRecord, record_id) is None
        assert session.get(ParsedLog, event_id) is None
        assert session.scalar(select(func.count()).select_from(AvailabilityMarker).where(
            AvailabilityMarker.event_id == event_id)) == 0


@pytest.mark.parametrize("zone", ["UTC", "Asia/Yekaterinburg", "America/New_York"])
def test_projection_matches_original_guard_and_dynamic_future_provenance(context, zone):
    database, _, source, _ = context
    timestamps = ["1900-01-01T00:00:00.123456+05:21:10.654321",
        "0001-01-01T00:00:00Z", "9999-12-31T23:59:59.999999Z",
        "2026-02-30T00:00:00Z", "0001-01-01T00:00:00+01:00",
        "9999-12-31T23:59:59-01:00", None, True, [], {}, "2020-01-01T00:00:00",
        "2026-11-01T01:30:00.000001-04:00", "2026-11-01T01:30:00.000001-05:00"]
    identities = [write_event(context, stamp, offset=index) for index, stamp in enumerate(timestamps)]
    fallback = write_event(context, "2020-01-01T00:00:00Z", offset=len(timestamps),
        diagnostics=[{"code": "event_time_fallback"}])
    unproven = write_event(context, "2020-01-01T00:00:00Z", offset=len(timestamps) + 1)
    with database.session_factory.begin() as session:
        session.execute(update(ParsedLog).where(ParsedLog.id == unproven).values(processed_record_id=None))
    reference = datetime(2026, 11, 16, tzinfo=UTC)
    with database.session_factory() as session:
        session.execute(select(func.set_config("TimeZone", zone, True)))
        _, _, original = _scalar(ParsedLog.ecs_data["@timestamp"], SimpleNamespace(type="date"))
        rows = session.execute(select(ParsedLog.id, ParsedLog.event_time_utc, original.label("original"))
            .where(ParsedLog.source_id == source)).all()
        assert all(row.event_time_utc == row.original for row in rows)
        events = classified_events(source, reference)
        reasons = dict(session.execute(select(events.c.event_id, events.c.reason)).all())
        for identity, stamp in zip(identities, timestamps, strict=True):
            assert reasons[identity] == event_timestamp({"@timestamp": stamp}, [], True, reference)[1]
        assert reasons[fallback] == "event_time_fallback"
        assert reasons[unproven] == "event_time_unknown_provenance"
        events = classified_events(source, datetime.max.replace(tzinfo=UTC))
        assert session.scalar(select(events.c.reason).where(events.c.event_id == identities[2])) is None


def write_batch(context, in_window):
    database, _, source, connection = context
    anchor, now = datetime(2000, 1, 1, tzinfo=UTC), datetime.now(UTC)
    began = time.perf_counter()
    with database.session_factory.begin() as session:
        repository = SqlAlchemyProcessingRepository(session)
        for offset in range(10003):
            identity = uuid4()
            timestamp = anchor + timedelta(seconds=1 if offset < in_window else 3601)
            record = ProcessedKafkaRecord(id=uuid4(), connection_id=connection,
                connection_identity=connection, kafka_topic_identity="performance-generation",
                source_id=source, normalizer_id=None, kafka_topic=str(source), kafka_partition=0,
                kafka_offset=offset, result_status="complete", backend_received_at=now, backend_processed_at=now)
            event = ParsedLog(id=identity, source_id=source, connection_id=connection,
                normalizer_id=None, normalizer_version=1, normalizer_name="synthetic",
                source_name="synthetic", connection_name="synthetic", kafka_topic=str(source),
                kafka_partition=0, kafka_offset=offset, deduplication_key=str(identity),
                fluent_bit_collected_at=now, backend_received_at=now, backend_processed_at=now,
                raw="synthetic", ecs_data={"@timestamp": timestamp.isoformat(),
                    "process": {"pid": 20 if offset % 3 == 2 else 10}}, normalization_diagnostics=[])
            assert repository.store_event(record, event)
    return anchor, time.perf_counter() - began


def plan_indexes(value):
    if isinstance(value, dict):
        if "Index Name" in value:
            yield value["Index Name"]
        for child in value.values():
            yield from plan_indexes(child)
    elif isinstance(value, list):
        for child in value:
            yield from plan_indexes(child)


def plan_summary(value):
    if isinstance(value, dict):
        if "Node Type" in value:
            yield {key: value[key] for key in ("Node Type", "Relation Name", "Index Name",
                "Plan Rows", "Plan Width", "Total Cost", "Join Type", "Index Cond",
                "Join Filter", "Filter", "Inner Unique") if key in value}
        for child in value.values():
            yield from plan_summary(child)
    elif isinstance(value, list):
        for child in value:
            yield from plan_summary(child)


@contextmanager
def query_profile(engine, metrics):
    """Report bounded structural diagnostics even when the original limit fires."""
    active = {}

    def before(connection, cursor, statement, parameters, context, many):
        if not statement.lstrip().upper().startswith(("SELECT", "WITH")) or "set_config(" in statement:
            return
        kind = ("cohort_envelope" if "HAVING" in statement else
            "window_cohorts" if "GROUP BY" in statement else
            "statistics" if "aggregate_typed" in statement else "snapshot_read")
        current = {"phase": kind}
        if kind in {"cohort_envelope", "window_cohorts"}:
            with connection.connection.driver_connection.cursor() as explain:
                explain.execute("EXPLAIN (FORMAT JSON) " + statement, parameters)
                current["plan"] = list(plan_summary(explain.fetchone()[0]))[:20]
        metrics.append(current)
        active[id(context)] = (time.perf_counter(), current)

    def after(connection, cursor, statement, parameters, context, many):
        started = active.pop(id(context), None)
        if started:
            started[1]["seconds"] = time.perf_counter() - started[0]

    event.listen(engine, "before_cursor_execute", before)
    event.listen(engine, "after_cursor_execute", after)
    try:
        yield
    finally:
        for started, current in active.values():
            current["seconds_until_failure"] = time.perf_counter() - started
        event.remove(engine, "before_cursor_execute", before)
        event.remove(engine, "after_cursor_execute", after)


@pytest.mark.parametrize("in_window", [3, 5003])
def test_real_writer_10003_preview_count_distinct_and_selective_index(context, in_window, monkeypatch, capsys):
    database, catalog, source, _ = context
    anchor, ingest_seconds = write_batch(context, in_window)
    observed = []
    original = AggregateSnapshotService.build

    def capture(*args, **kwargs):
        candidate = original(*args, **kwargs)
        observed.append(candidate)
        return candidate

    monkeypatch.setattr(AggregateSnapshotService, "build", capture)
    metrics = {}
    for operator in ("count", "distinct_count"):
        rule = {"where": {"op": "exists", "field": "process.pid"}, "features": [{
            "key": "n", "name": "N", "op": operator,
            **({"field": "process.pid"} if operator == "distinct_count" else {})}]}
        body = AggregatePreviewRequest(source_id=source, aggregate_rule=rule,
            schedule={"boundary": anchor, "timezone": "UTC", "duration_seconds": 300},
            window_start_utc=anchor, time_basis="event_time")
        with database.session_factory() as session:
            api = build_aggregate_service(session, catalog)
            began = time.perf_counter()
            sql_metrics = []
            try:
                with query_profile(database.engine, sql_metrics):
                    preview = api.preview(body)
            finally:
                with capsys.disabled():
                    print("Event-time SQL profile:", {"selected": in_window, "operator": operator,
                        "ingest_seconds": ingest_seconds, "wall": time.perf_counter() - began,
                        "queries": sql_metrics}, flush=True)
            elapsed = time.perf_counter() - began
            assert elapsed < 10
            assert preview.features[0].value == (in_window if operator == "count" else 2)
            assert preview.snapshot_metadata["membership_count"] == in_window
            assert observed[-1]["manifest"] == []
            metrics[operator] = {"wall": elapsed, **observed[-1]["timings"]}
            compiled = compile_rule(AggregateRule.model_validate(rule), catalog)
            with api.uow.read_snapshot():
                api.repository.read_budget(10000)
                manifest, _, rows, _ = api.repository.snapshot_events(source, anchor,
                    anchor + timedelta(seconds=300), api.repository.now(), OperationBudget(10))
                assert rows == len(manifest) == in_window
                stats = api.repository.event_statistics(source, anchor, anchor + timedelta(seconds=300),
                    api.repository.now(), compiled, OperationBudget(10))
                assert interpret_result(stats, compiled)["features"][0]["value"] == preview.features[0].value
        if in_window == 3:
            with database.session_factory() as session:
                session.execute(text("ANALYZE logs.parsed_logs"))
                api = build_aggregate_service(session, catalog)
                statement = aggregate_statement(api.repository.event_base(source, anchor,
                    anchor + timedelta(seconds=300), datetime.now(UTC)), compiled)
                sql = str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
                assert "pg_input_is_valid" not in sql
                plan = session.scalar(text("EXPLAIN (ANALYZE, FORMAT JSON) " + sql))
                assert "ix_parsed_logs_source_event_time" in set(plan_indexes(plan))
    with capsys.disabled():
        print("Event-time snapshot profile:", {"rows": 10003, "selected": in_window,
            "ingest_seconds": ingest_seconds, "preview": metrics})


def test_pid_exists_exact_invalid_missing_and_canonical_membership_are_not_replayed(context):
    database, catalog, source, _ = context
    anchor = datetime(2000, 1, 1, tzinfo=UTC)
    identities = [write_event(context, (anchor + timedelta(seconds=1)).isoformat(), offset=index)
        for index in range(6)]
    values = [10, 10, 20, None, "wrong", "absent"]
    with database.session_factory.begin() as session:
        for identity, value in zip(identities, values, strict=True):
            session.execute(update(ParsedLog).where(ParsedLog.id == identity).values(ecs_data={
                "@timestamp": (anchor + timedelta(seconds=1)).isoformat(),
                **({"process": {"pid": value}} if value != "absent" else {})}))
    body = AggregatePreviewRequest(source_id=source,
        aggregate_rule={"where": {"op": "exists", "field": "process.pid"}, "features": [
            {"key": "n", "name": "N", "op": "distinct_count", "field": "process.pid"}]},
        schedule={"boundary": anchor, "timezone": "UTC", "duration_seconds": 300},
        window_start_utc=anchor, time_basis="event_time")
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        preview = api.preview(body)
        assert preview.features[0].value is None and preview.features[0].reason == "invalid_input_type"
        assert preview.features[0].valid_count == 3 and preview.features[0].invalid_count == 1
        checked = api.validate(body)
        card = api.create(CreateAggregateRequest(**body.model_dump(), name="PID exact",
            rule_hash=checked.rule_hash), uuid4(), str(uuid4()))
    turns(context, card.id, 9)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        point = api.point_at(card.id, 0)
        assert point.features == preview.features
        saved = point.model_dump()
        assert set(session.scalars(select(AggregateMembership.event_id).where(
            AggregateMembership.closure_id == point.id))) == set(identities)
    write_event(context, (anchor + timedelta(seconds=2)).isoformat(), offset=6)
    turns(context, card.id, 9)
    with database.session_factory() as session:
        assert build_aggregate_service(session, catalog).point_at(card.id, 0).model_dump() == saved


def write_scan_batch(context, count, cohorts=1):
    """Small real-writer inputs; deterministic UUIDs leave room for a late UUID."""
    database, _, source, connection = context
    anchor = (datetime.now(UTC) - timedelta(seconds=600)).replace(microsecond=0)
    now, identities = datetime.now(UTC), []
    with database.session_factory.begin() as session:
        repository = SqlAlchemyProcessingRepository(session)
        for offset in range(count):
            identity = UUID(int=1000 + offset)
            partition = offset % cohorts
            record = ProcessedKafkaRecord(id=uuid4(), connection_id=connection,
                connection_identity=connection, kafka_topic_identity="scan-baseline",
                source_id=source, kafka_topic=str(source), kafka_partition=partition,
                kafka_offset=offset, result_status="complete", backend_received_at=now,
                backend_processed_at=now)
            row = ParsedLog(id=identity, source_id=source, connection_id=connection,
                normalizer_version=1, normalizer_name="synthetic", source_name="synthetic",
                connection_name="synthetic", kafka_topic=str(source), kafka_partition=partition,
                kafka_offset=offset, deduplication_key=str(identity), raw="synthetic",
                fluent_bit_collected_at=now, backend_received_at=now, backend_processed_at=now,
                ecs_data={"@timestamp": (anchor + timedelta(seconds=1)).isoformat(),
                    "network": {"bytes": offset + 1}}, normalization_diagnostics=[])
            assert repository.store_event(record, row)
            identities.append(identity)
    return anchor, identities


@contextmanager
def scan_query_profile(api, engine, monkeypatch, metrics):
    """Count every SQL round trip without EXPLAIN overhead or data disclosure.

    These are direct-service timings in the selected backend-db process, not
    Windows/browser or production worker timings. In particular they exclude
    admission, discovery, other targets and the detector/JVM process.
    """
    phase, active = ["advance"], {}
    metrics.update(system=platform.system(), component="backend-db direct service",
        phases={}, sql={}, settings_by_transaction={})

    transactions = {}

    def begin(connection):
        transactions[connection] = len(transactions_started) + 1
        transactions_started.append(phase[0])

    transactions_started = []

    def measured(name, original):
        def call(*args, **kwargs):
            previous, phase[0] = phase[0], name
            wall, cpu = time.perf_counter(), time.process_time()
            try:
                return original(*args, **kwargs)
            finally:
                values = metrics["phases"].setdefault(name, {"calls": 0, "wall": 0, "cpu": 0})
                values["calls"] += 1
                values["wall"] += time.perf_counter() - wall
                values["cpu"] += time.process_time() - cpu
                phase[0] = previous
        return call

    def before(connection, cursor, statement, parameters, execution, many):
        sql = statement.lower()
        if "set_config(" in sql:
            kind = "settings"
            transaction = str(transactions[connection])
            settings = metrics["settings_by_transaction"].setdefault(transaction, {})
            settings[phase[0]] = settings.get(phase[0], 0) + 1
        elif "from logs.aggregate_closures" in sql:
            kind = "closure_read"
        elif "from logs.aggregate_memberships" in sql:
            kind = "membership_read"
        elif "from logs.aggregate_history_spans" in sql:
            kind = "history_span_read"
        elif "from logs.parsed_logs" in sql:
            kind = "event_page_read"
        else:
            kind = statement.lstrip().split(None, 1)[0].lower()
        key = phase[0] + ":" + kind
        values = metrics["sql"].setdefault(key, {"calls": 0, "seconds": 0})
        values["calls"] += 1
        active[id(execution)] = (time.perf_counter(), values)

    def after(connection, cursor, statement, parameters, execution, many):
        started, values = active.pop(id(execution))
        values["seconds"] += time.perf_counter() - started

    wall, cpu = time.perf_counter(), time.process_time()
    with monkeypatch.context() as patch:
        for name in ("scan_page", "save_scan_page"):
            patch.setattr(api.repository, name, measured(name, getattr(api.repository, name)))
        patch.setattr(api.uow, "commit", measured("commit", api.uow.commit))
        patch.setattr(api, "record_worker_error",
            measured("record_worker_error", api.record_worker_error))
        event.listen(engine, "begin", begin)
        event.listen(engine, "before_cursor_execute", before)
        event.listen(engine, "after_cursor_execute", after)
        try:
            yield
        finally:
            metrics["wall"] = time.perf_counter() - wall
            metrics["cpu"] = time.process_time() - cpu
            metrics["sql_total_calls"] = sum(row["calls"] for row in metrics["sql"].values())
            for started, values in active.values():
                values["seconds_until_failure"] = time.perf_counter() - started
            event.remove(engine, "before_cursor_execute", before)
            event.remove(engine, "after_cursor_execute", after)
            event.remove(engine, "begin", begin)


def test_read_budget_sets_both_actual_local_timeouts_and_rolls_back(context, monkeypatch):
    database, catalog, _, _ = context
    settings = select(func.current_setting("statement_timeout"), func.current_setting("lock_timeout"))
    calls = []

    def count_settings(connection, cursor, statement, parameters, execution, many):
        if "set_config(" in statement:
            calls.append(statement.count("set_config("))

    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        original = tuple(session.execute(settings).one())
        session.execute(select(func.set_config("statement_timeout", "1733", False),
            func.set_config("lock_timeout", "1777", False)))
        session.commit()
        try:
            assert tuple(session.execute(settings).one()) == ("1733ms", "1777ms")
            for milliseconds, elapsed, expected in ((1250, .125, 1125), (1000, .25, 750)):
                calls.clear()
                event.listen(database.engine, "before_cursor_execute", count_settings)
                try:
                    moments = iter((0.0, elapsed))
                    with monkeypatch.context() as patch:
                        patch.setattr("app.repositories.sqlalchemy.aggregates.monotonic", lambda moments=moments: next(moments))
                        api.repository.read_budget(milliseconds)
                finally:
                    event.remove(database.engine, "before_cursor_execute", count_settings)
                assert calls == [2]
                assert tuple(session.execute(settings).one()) == (f"{expected}ms",) * 2
            session.rollback()
            assert tuple(session.execute(settings).one()) == ("1733ms", "1777ms")
            api.repository.read_budget(2345)
            session.commit()
            assert tuple(session.execute(settings).one()) == ("1733ms", "1777ms")
        finally:
            session.rollback()
            session.execute(select(func.set_config("statement_timeout", original[0], False),
                func.set_config("lock_timeout", original[1], False)))
            session.commit()


def test_read_budget_does_not_autoflush_pending_write_before_new_timeouts(context, capsys, monkeypatch):
    database, catalog, _, connection_id = context
    observed = []
    pending_id = uuid4()

    def current_settings(session):
        # Connection-level SHOW never recursively enters Session.execute or
        # triggers ORM autoflush, including inside the before_flush listener.
        connection = session.connection()
        return tuple(connection.exec_driver_sql("SHOW " + setting).scalar_one()
            for setting in ("statement_timeout", "lock_timeout"))

    def before_flush(session, flush_context, instances):
        observed.append(current_settings(session))

    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        original = current_settings(session)
        connection = session.connection()
        connection.exec_driver_sql("SET LOCAL statement_timeout = '1733ms'")
        connection.exec_driver_sql("SET LOCAL lock_timeout = '1777ms'")
        assert current_settings(session) == ("1733ms", "1777ms")
        pending = Source(id=pending_id, connection_id=connection_id, name=str(pending_id),
            topic_name=str(pending_id), is_enabled=False)
        session.add(pending)
        event.listen(session, "before_flush", before_flush)
        try:
            with monkeypatch.context() as patch:
                patch.setattr("app.repositories.sqlalchemy.aggregates.monotonic", lambda: 0.0)
                api.repository.read_budget(987)
            after_setting = current_settings(session)
            with capsys.disabled():
                print("Read-budget pending ORM order:", {
                    "system": platform.system(), "python": platform.python_version(),
                    "sqlalchemy": sqlalchemy.__version__, "old_local": ("1733ms", "1777ms"),
                    "before_flush": observed, "after_setting": after_setting,
                    "still_pending": pending in session.new}, flush=True)
            assert observed == [], "read_budget must set new GUCs before any pending ORM write"
            assert pending in session.new
            assert after_setting == ("987ms", "987ms")
            session.flush()
            assert observed == [("987ms", "987ms")]
            assert session.connection().exec_driver_sql(
                "SELECT count(*) FROM app.sources WHERE id = %s", (pending_id,)).scalar_one() == 1
        finally:
            event.remove(session, "before_flush", before_flush)
            session.rollback()
            assert current_settings(session) == original
            assert session.connection().exec_driver_sql(
                "SELECT count(*) FROM app.sources WHERE id = %s", (pending_id,)).scalar_one() == 0
            session.rollback()


def test_snapshot_limits_coalesce_local_settings_and_stop_at_deadline(context, monkeypatch):
    database, catalog, _, _ = context
    settings = select(*(func.current_setting(name) for name in
        ("statement_timeout", "lock_timeout", "work_mem", "jit")))
    calls = []

    def count_settings(_connection, _cursor, statement, _parameters, _execution, _many):
        if "set_config(" in statement:
            calls.append(statement.count("set_config("))

    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        initial = tuple(session.execute(settings).one())
        event.listen(database.engine, "before_cursor_execute", count_settings)
        try:
            with monkeypatch.context() as patch:
                patch.setattr("app.repositories.sqlalchemy.aggregates.monotonic", lambda: 0.0)
                api.repository.limits(Limits(work_mem_mb=17), False,
                    OperationBudget(1.25, clock=lambda: 0.0, started=0.0))
                assert tuple(session.execute(settings).one()) == ("1250ms", "1250ms", "17MB", "off")
                assert calls == [2, 2]
                session.rollback()
                calls.clear()
                moments = iter((0.0, 1.0))
                with pytest.raises(EventTimeBudgetError, match="operation_seconds"):
                    api.repository.limits(Limits(work_mem_mb=17), True,
                        OperationBudget(1.0, clock=lambda: next(moments), started=0.0))
                assert calls == [2]
        finally:
            event.remove(database.engine, "before_cursor_execute", count_settings)
            session.rollback()
        assert tuple(session.execute(settings).one()) == initial


def advance_until(context, card, ready, *, scan_pages):
    """Observe progress within the required real scan pages and history halves."""
    database, catalog, _, _ = context
    # A not-due cycle contains one scan and four history reservations. A full
    # 500-row page needs a second page before its cursor can wrap; callers name
    # that page requirement rather than running an arbitrary extra turn count.
    for _ in range(scan_pages * (FOREGROUND_BURST + 2)):
        with database.session_factory() as session:
            api = build_aggregate_service(session, catalog)
            result = api.advance(card.id)
            if ready(session, result):
                return result
    pytest.fail("Canonical closure/cyclic scan did not reach the required state")


@pytest.mark.parametrize("rows", [1, 500])
def test_scan_closed_membership_sql_baseline(context, rows, monkeypatch, capsys):
    database, catalog, _, _ = context
    anchor, identities = write_scan_batch(context, rows)
    card, _ = create(context, anchor)
    # Close window zero and finish the actual cursor cycle, including the empty
    # tail of a full 500-row page. Stop after background history so the next
    # advance measures a full scan page rather than an incidental phase.
    def closed_and_wrapped(session, result):
        schedule = session.get(AggregateSchedule, card.schedule_id)
        return (result["state"] != "scanned" and schedule.turn == 0
            and schedule.scan_after is None and schedule.scan_cycle > 0
            and session.scalar(select(AggregateClosure.origin_point_id).where(
                AggregateClosure.aggregate_id == card.id, AggregateClosure.window_index == 0)) is not None)

    advance_until(context, card, closed_and_wrapped, scan_pages=1 + rows // 500)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        saved = api.point_at(card.id, 0).model_dump()
        assert saved["event_count"] == rows
        assert set(session.scalars(select(AggregateMembership.event_id).where(
            AggregateMembership.closure_id == saved["id"]))) == set(identities)
        session.rollback()
        metrics = {"rows": rows, "case": "closed_membership_rescan"}
        try:
            with scan_query_profile(api, database.engine, monkeypatch, metrics):
                result = api.advance(card.id)
                api.record_worker_error(card.id, None)
        finally:
            with capsys.disabled():
                print("Event-time scan SQL baseline:", metrics, flush=True)
        assert result["state"] == "scanned" and result["records"] == rows
        # These are two page-wide reads, irrespective of the number of events.
        assert metrics["sql"]["save_scan_page:closure_read"]["calls"] == 1
        assert metrics["sql"]["save_scan_page:membership_read"]["calls"] == 1
        assert metrics["sql"]["scan_page:event_page_read"]["calls"] == 1
        # Each settings statement sets both local GUCs. The calls stay flat
        # across page sizes and are reset/reapplied at transaction boundaries.
        assert metrics["settings_by_transaction"] == {
            "1": {"advance": 8},  # definition, schedule, attempts, locked reservation and readiness
            "2": {"advance": 1, "scan_page": 1},  # RR reference and page read
            "3": {"advance": 6, "save_scan_page": 3},  # fence, set-wise writes, readiness and commit
            "4": {"record_worker_error": 5},  # locked metadata, clock read and commit
        }
        assert api.point_at(card.id, 0).model_dump() == saved
        assert session.scalar(select(func.count()).select_from(AggregateLateEvent).where(
            AggregateLateEvent.closure_id == saved["id"])) == 0
    late = write_event(context, (anchor + timedelta(seconds=2)).isoformat(),
        offset=rows, event_id=UUID(int=1), generation="scan-baseline")
    # The smaller UUID must be discovered after the cursor wraps, without
    # modifying the canonical point, membership or duplicating the late ledger.
    advance_until(context, card, lambda session, result: session.scalar(select(AggregateLateEvent.event_id).where(
        AggregateLateEvent.closure_id == saved["id"], AggregateLateEvent.event_id == late)) == late, scan_pages=2)
    wrapped = [False]

    def repeated_late_scan(session, result):
        if wrapped[0] and result["state"] == "scanned":
            assert result["records"] == min(rows + 1, 500)
            return True
        wrapped[0] = session.get(AggregateSchedule, card.schedule_id).scan_after is None
        return False

    # Encounter the same late UUID again after a complete cursor wrap, so the
    # unique ledger/finality assertion also proves a repeated scan is harmless.
    advance_until(context, card, repeated_late_scan, scan_pages=2)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        assert api.point_at(card.id, 0).model_dump() == saved
        assert set(session.scalars(select(AggregateMembership.event_id).where(
            AggregateMembership.closure_id == saved["id"]))) == set(identities)
        assert list(session.scalars(select(AggregateLateEvent.event_id).where(
            AggregateLateEvent.closure_id == saved["id"]))) == [late]


@pytest.mark.parametrize("cohorts", [1, 8])
def test_scan_open_history_cohort_sql_baseline(context, cohorts, monkeypatch, capsys):
    database, catalog, _, _ = context
    anchor, identities = write_scan_batch(context, 32, cohorts)
    card, _ = create(context, anchor)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        metrics = {"rows": len(identities), "cohorts": cohorts, "case": "open_history_scan"}
        try:
            with scan_query_profile(api, database.engine, monkeypatch, metrics):
                result = api.advance(card.id)
                api.record_worker_error(card.id, None)
        finally:
            with capsys.disabled():
                print("Event-time scan SQL baseline:", metrics, flush=True)
        assert result["state"] == "scanned" and result["records"] == len(identities)
        assert metrics["sql"]["save_scan_page:history_span_read"]["calls"] == 1
        assert metrics["sql"]["save_scan_page:insert"]["calls"] == 1
        spans = list(session.scalars(select(AggregateHistorySpan).where(
            AggregateHistorySpan.aggregate_id == card.id)))
        assert len(spans) == cohorts
        assert {span.coordinates["partition"] for span in spans} == set(range(cohorts))
        assert all(span.first_index == span.last_index == span.next_index == 0 for span in spans)
        assert sum(1 + (span.coordinates["last_offset"] - span.coordinates["first_offset"])
            // cohorts for span in spans) == len(identities)
    # No online window is due. The next background history call must close
    # window zero; smaller UUIDs then extend the existing spans on the next scan.
    with database.session_factory() as session:
        result = build_aggregate_service(session, catalog).advance(card.id)
        assert (result["state"], result.get("origin")) == ("closed", "initial_history")
    # Let the remaining history half finish through real calls. The next
    # advance must be a scan, and every "previous" span below is read only
    # after these legitimate already-closed/history-waiting cursor advances.
    advance_until(context, card,
        lambda session, result: session.get(AggregateSchedule, card.schedule_id).turn == 0,
        scan_pages=1)
    with database.session_factory() as session:
        api = build_aggregate_service(session, catalog)
        saved = api.point_at(card.id, 0).model_dump()
        previous = {span.cohort_key: {
            "first_index": span.first_index, "last_index": span.last_index,
            "next_index": span.next_index, "load_start": span.load_start,
            "load_end": span.load_end, "coordinates": dict(span.coordinates)}
            for span in session.scalars(select(AggregateHistorySpan).where(
                AggregateHistorySpan.aggregate_id == card.id))}
    additions = [write_event(context, (anchor + timedelta(seconds=301)).isoformat(),
        offset=len(identities) + partition, event_id=UUID(int=900 + partition),
        generation="scan-baseline", partition=partition) for partition in range(cohorts)]
    with database.session_factory() as session:
        loaded = dict(session.execute(select(ParsedLog.kafka_partition, ParsedLog.backend_processed_at)
            .where(ParsedLog.id.in_(additions))).all())
        session.rollback()
        api = build_aggregate_service(session, catalog)
        metrics = {"rows": len(identities) + len(additions), "cohorts": cohorts,
            "case": "existing_history_merge"}
        try:
            with scan_query_profile(api, database.engine, monkeypatch, metrics):
                result = api.advance(card.id)
                api.record_worker_error(card.id, None)
        finally:
            with capsys.disabled():
                print("Event-time scan SQL baseline:", metrics, flush=True)
        assert result["state"] == "scanned" and result["records"] == len(identities) + len(additions)
        assert metrics["sql"]["save_scan_page:history_span_read"]["calls"] == 1
        assert metrics["sql"]["save_scan_page:update"]["calls"] == 1
        spans = list(session.scalars(select(AggregateHistorySpan).where(
            AggregateHistorySpan.aggregate_id == card.id)))
        assert {span.cohort_key for span in spans} == set(previous)
        for span in spans:
            before = previous[span.cohort_key]
            partition = span.coordinates["partition"]
            assert span.first_index == before["first_index"] == 0
            assert span.last_index == max(before["last_index"], 1)
            assert span.next_index == min(before["next_index"], 1)
            assert span.load_start == before["load_start"]
            assert span.load_end == max(before["load_end"], loaded[partition])
            assert span.coordinates == {**before["coordinates"],
                "last_offset": len(identities) + partition}
        assert api.point_at(card.id, 0).model_dump() == saved
        assert set(session.scalars(select(AggregateMembership.event_id).where(
            AggregateMembership.closure_id == saved["id"]))) == set(identities)
        assert session.scalar(select(func.count()).select_from(AggregateLateEvent).where(
            AggregateLateEvent.closure_id == saved["id"])) == 0
