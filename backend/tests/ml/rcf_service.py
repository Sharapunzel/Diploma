"""Real Linux/JVM + PostgreSQL detector transaction and lifecycle regressions."""

import copy
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.engine import make_url

from app.adapters.detector_lock import DetectorSessionLock
from app.adapters.model_runtime import ModelRuntime
from app.aggregate_composition import build_aggregate_service
from app.db import Database
from app.detection.aws_rcf import AwsModelFactory
from app.detection.contracts import DetectionError, StaleCandidate, digest
from app.detection.model_process import BoundedModelFactory
from app.detector_composition import build_detector_service as compose_detector_service
from app.detector_composition import build_worker_detector_service
from app.ecs import PackagedEcsCatalog
from app.models import (
    AggregateClosure,
    Detector,
    DetectorOutcome,
    DetectorWarmup,
    Finding,
    KafkaConnection,
    Source,
    WorkingWindowPoint,
)
from app.repositories.sqlalchemy.detectors import SqlAlchemyDetectorRepository
from app.schemas.aggregates import AggregatePreviewRequest, CreateAggregateRequest

OWNER = None
RUNTIME = None


@pytest.mark.parametrize("accelerated", [False, True])
def test_formula_output_real_capture_history_working_and_checkpoint(catalog, accelerated):
    from formula_history import create_formula, real_formula_history, wait_until

    from app.models import SourceInterruption

    with real_formula_history(catalog) as fixture:
        card, body, start = create_formula(fixture)
        seconds = 18 if accelerated else 32
        for second in range(seconds):
            wait_until(start + timedelta(seconds=second, milliseconds=100))
            fixture.write(1 + second % 3)
        end = start + timedelta(seconds=seconds)
        wait_until(end + timedelta(seconds=2))
        for index in range(seconds // 2):
            working = fixture.close_one(card)
            assert working.completeness == "complete", (working.completeness_reason, working.evidence_refs)
            with fixture.database.session_factory() as session:
                preview = fixture.aggregates(session).preview(body.model_copy(update={
                    "window_start_utc": start + timedelta(seconds=index*2), "time_basis": "event_time"}))
                assert [item.model_dump() for item in preview.features] == working.features
        with fixture.database.session_factory() as session:
            service = fixture.detectors(session)
            identity = service.create(card.id, UUID(int=1), "Formula",
                config={"sample_size": 8, "output_after": 8, "trees": 20},
                accelerated=accelerated, step=1 if accelerated else None)
            service.start(identity)
            service.begin_start(identity)
            result = service.prepare_warmup(identity)
            assert result["state"] == "prepared", result
            attempt = service.repository.attempt(result["attempt_id"])
            assert all(item["basis"] == "event_time" and item["completeness"] == "complete" for item in attempt["inputs"])
            assert all([f["key"] for f in item["features"]] == ["n"] for item in attempt["inputs"])
            assert all(item["features"][0]["value"] == 3*item["event_count"] for item in attempt["inputs"])
            assert service.warmup(identity) == {"state": "ready"}
            before = service.state(identity)["detector"]["checkpoint"]
            assert before["body"]["keys"] == ["n"]
        fixture.close_one(card)
        with fixture.database.session_factory() as session:
            service = fixture.detectors(session)
            assert service.advance_one(identity)["state"] == "evaluated"
            after = service.state(identity)["detector"]["checkpoint"]
            assert after["body"]["updates"] == before["body"]["updates"] + 1
        # Absence of an optional signed probe is not a ban on received-snapshot
        # evaluation. A positive stop fact is; it must not reset the forest.
        with fixture.database.session_factory.begin() as session:
            session.add(SourceInterruption(source_identity=fixture.source_id, kind="source_disabled",
                started_at=end + timedelta(seconds=2)))
        interrupted = fixture.close_one(card)
        assert interrupted.completeness == "interrupted"
        with fixture.database.session_factory() as session:
            service = fixture.detectors(session)
            assert service.advance_one(identity)["state"] == "skipped"
            assert service.state(identity)["detector"]["checkpoint"] == after


@pytest.fixture(scope="module", autouse=True)
def coordinator(database):
    global OWNER, RUNTIME
    lock = DetectorSessionLock(os.environ["TEST_DATABASE_URL"])
    pid = lock.acquire()
    assert pid is not None
    with database.session_factory.begin() as session:
        OWNER = SqlAlchemyDetectorRepository(session).acquire_owner(uuid4(), pid)
    RUNTIME = ModelRuntime()
    try:
        yield lock
    finally:
        RUNTIME.close()
        RUNTIME = None
        lock.close()
        OWNER = None


def build_detector_service(session, catalog):
    return compose_detector_service(session, catalog, OWNER,
        models=BoundedModelFactory(transport=RUNTIME))


def prepare(service, identity):
    # Explicit administrative authorization followed by coordinator acceptance.
    state = service.state(identity)["detector"]["execution_state"]
    if state not in {"requested", "starting", "running"}:
        service.start(identity)
        state = "requested"
    if state == "requested":
        service.begin_start(identity)
    return service.prepare_warmup(identity)


@pytest.fixture(scope="module")
def database():
    url = os.environ["TEST_DATABASE_URL"]
    assert (make_url(url).database or "").startswith("diploma_test_review_")
    environment = {**os.environ, "DATABASE_URL": url}
    environment.pop("DATABASE_URL_FILE", None)
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"],
                   env=environment, check=True)
    db = Database(url)
    yield db
    db.dispose()


@pytest.fixture(scope="module")
def catalog():
    return PackagedEcsCatalog.load()


@pytest.fixture
def setup(database, catalog, request):
    source_id, connection_id = uuid4(), uuid4()
    now = datetime.now(UTC).replace(microsecond=0)
    anchor = now - timedelta(seconds=600)
    with database.session_factory.begin() as session:
        session.add(KafkaConnection(id=connection_id, name=str(connection_id),
            bootstrap_servers=["kafka:9092"], cluster_identity=str(connection_id)))
        session.flush()
        session.add(Source(id=source_id, connection_id=connection_id, name=str(source_id),
            topic_name=str(source_id), kafka_topic_identity="synthetic", is_enabled=True))
    kind = getattr(request, "param", "count")
    duration = 10 if kind == "overlap" else 1
    feature = {"key": "n", "name": "Count", "op": "count"}
    if kind == "bytes":
        feature = {"key": "n", "name": "Bytes", "op": "sum", "field": "network.bytes"}
    body = AggregatePreviewRequest(source_id=source_id,
        aggregate_rule={"features": [feature]},
        schedule={"boundary": anchor, "timezone": "UTC", "duration_seconds": duration},
        window_start_utc=anchor, time_basis="event_time")
    with database.session_factory() as session:
        aggregate = build_aggregate_service(session, catalog)
        aggregate.repository.now = lambda: now - timedelta(seconds=120)
        checked = aggregate.validate(body)
        card = aggregate.create(CreateAggregateRequest(**body.model_dump(), name="Synthetic count",
            rule_hash=checked.rule_hash), UUID(int=1), str(uuid4()))
    return SimpleNamespace(card=card, anchor=anchor, cutoff=now - timedelta(seconds=120),
                           database=database, catalog=catalog, source_id=source_id,
                           connection_id=connection_id, kind=kind, duration=duration)


def processed_record(session, setup, index, at):
    from app.models import ProcessedKafkaRecord
    record = ProcessedKafkaRecord(id=uuid4(), connection_id=setup.connection_id,
        connection_identity=setup.connection_id, source_id=setup.source_id,
        kafka_topic_identity="synthetic", kafka_topic=str(setup.source_id),
        kafka_partition=0, kafka_offset=index, result_status="complete",
        backend_received_at=at, backend_processed_at=at)
    session.add(record)
    session.flush()
    return record.id


def point(setup, index, value, completeness="complete"):
    start = setup.anchor + timedelta(seconds=index * setup.duration)
    with setup.database.session_factory.begin() as session:
        row = WorkingWindowPoint(id=uuid4(), schedule_id=setup.card.schedule_id,
            window_index=index, start_utc=start, end_utc=start + timedelta(seconds=setup.duration),
            scheduled_at=start + timedelta(seconds=setup.duration), actual_started_at=start + timedelta(seconds=setup.duration),
            actual_completed_at=start + timedelta(seconds=setup.duration), event_count=0,
            event_time_min=None, event_time_max=None, completeness=completeness,
            completeness_reason="synthetic_fixture", evidence_refs={}, time_basis="event_time",
            features=[{"key": "n", "value": value, "reason": None, "valid_count": 1, "invalid_count": 0}])
        end = start + timedelta(seconds=setup.duration)
        session.add(AggregateClosure(origin_point_id=row.id, aggregate_id=setup.card.id,
            schedule_id=setup.card.schedule_id, source_identity=setup.source_id, window_index=index,
            start_utc=start, end_utc=end, time_basis="event_time", origin="online" if index >= setup.card.online_start_index else "initial_history",
            snapshot_reference_at=end, closed_at=end, close_not_before_at=end + timedelta(seconds=min(setup.duration,300)),
            payload_state="present", point_kind="working"))
        session.flush()
        session.add(row)
    return row.id


def freeze_attempt_clock(service, at):
    for name in ("read_context", "lock_context"):
        read = getattr(service.repository, name)
        def fixed(identity, read=read):
            return {**read(identity), "now": at}
        setattr(service.repository, name, fixed)


def prepared(setup, accelerated=False, timezone=None):
    with setup.database.session_factory() as session:
        if timezone:
            session.execute(select(func.set_config("TimeZone", timezone, False)))
        service = build_detector_service(session, setup.catalog)
        detector = service.create(setup.card.id, UUID(int=1), "Test RCF",
                                  config={"sample_size": 16, "output_after": 16, "trees": 20},
                                  accelerated=accelerated, step=1 if accelerated else None)
        freeze_attempt_clock(service, setup.cutoff)
        result = prepare(service, detector)
        assert result["state"] == "prepared", result
        assert service.warmup(detector) == {"state": "ready"}
        assert service.state(detector)["detector"]["accelerated_used"] is accelerated
    return detector


def history(setup, constant=False):
    for index in range(448, 480):
        point(setup, index, 2 if constant else 1 + index % 3)


def advance(setup, identity):
    with setup.database.session_factory() as session:
        return build_detector_service(session, setup.catalog).advance_one(identity)


def stored(setup, identity):
    with setup.database.session_factory() as session:
        return build_detector_service(session, setup.catalog).state(identity)["detector"]


@pytest.mark.parametrize("accelerated", [False, True])
def test_warmup_and_first_working_finding(setup, accelerated):
    history(setup)
    identity = prepared(setup, accelerated)
    state = stored(setup, identity)
    assert state["consumed_index"] is None and state["evaluation_start_index"] == 480
    with setup.database.session_factory() as session:
        assert not session.scalar(select(func.count()).select_from(Finding).where(Finding.detector_id == identity))
    point(setup, 480, 1000)
    result = advance(setup, identity)
    assert result["state"] == "evaluated" and result["finding"]
    finding = result["finding"]
    assert finding["exact_values"] == [1000] and finding["accelerated_used"] is accelerated
    assert finding["event_time_min"] is None and finding["event_time_max"] is None
    assert finding["attribution"] and finding["expected"]
    with setup.database.session_factory() as session:
        saved = session.scalar(select(Finding).where(Finding.detector_id == identity))
        assert not saved.late and saved.data["recovery"] == []
    checkpoint = stored(setup, identity)["checkpoint"]
    assert advance(setup, identity)["reason"] == "working_point_pending"
    assert stored(setup, identity)["checkpoint"] == checkpoint


@pytest.mark.parametrize("coverage,value", [("unknown", 0), ("interrupted", 0), ("complete", None)])
def test_skip_moves_only_consumed_cursor(setup, coverage, value):
    history(setup)
    identity = prepared(setup)
    before = stored(setup, identity)["checkpoint"]
    point(setup, 480, value, coverage)
    assert advance(setup, identity)["state"] == "skipped"
    after = stored(setup, identity)
    assert after["consumed_index"] == 480 and after["evaluated_index"] is None
    assert after["checkpoint"] == before
    with setup.database.session_factory() as session:
        build_detector_service(session, setup.catalog).stop(identity)
        protection = build_aggregate_service(session, setup.catalog).cleanup_preview(setup.card.id, 0)
        assert protection.minimum_keep == 0 and protection.can_cleanup


def uncomputed_point(setup, index):
    """Negative DTO/storage fixture; never a successful manufactured vector."""
    from uuid import uuid5

    from sqlalchemy import null

    from app.aggregation.event_time import close_deadline
    from app.aggregation.time_policy import execution_diagnostics, execution_policy

    start = setup.anchor + timedelta(seconds=index * setup.duration)
    end = start + timedelta(seconds=setup.duration)
    origin = "online" if index >= setup.card.online_start_index else "initial_history"
    began = close_deadline(end, setup.duration) if origin == "online" else end
    hard = execution_policy(setup.duration)["hard_budget_seconds"]
    completed = began + timedelta(seconds=hard)
    identity = uuid5(setup.card.schedule_id, "event-time-window:" + str(index))
    attempt = SimpleNamespace(attempt_id=uuid4(), hard_budget_seconds=hard, origin=origin,
        window_index=index, start_utc=start, end_utc=end, started_at=began, deadline_at=completed)
    execution = execution_diagnostics(attempt, completed, hard, "uncomputed", "finalization")
    with setup.database.session_factory.begin() as session:
        session.add(AggregateClosure(origin_point_id=identity, aggregate_id=setup.card.id,
            schedule_id=setup.card.schedule_id, source_identity=setup.source_id, window_index=index,
            start_utc=start, end_utc=end, time_basis="event_time", origin=origin,
            snapshot_reference_at=None, closed_at=completed, close_not_before_at=began,
            payload_state="present", point_kind="working", calculation_status="uncomputed",
            calculation_reason="execution_budget_exceeded", execution=execution))
        session.flush()
        session.add(WorkingWindowPoint(id=identity, schedule_id=setup.card.schedule_id,
            window_index=index, start_utc=start, end_utc=end, scheduled_at=end,
            actual_started_at=began, actual_completed_at=completed, event_count=None,
            event_time_min=None, event_time_max=None, completeness=None, completeness_reason=None,
            evidence_refs=null(), features=null(), time_basis="event_time",
            calculation_status="uncomputed", calculation_reason="execution_budget_exceeded"))
    return identity


def test_terminal_uncomputed_skips_without_loading_native_checkpoint(setup):
    history(setup)
    identity = prepared(setup)
    point(setup, 480, 1000)
    assert advance(setup, identity)["state"] == "evaluated"
    before = stored(setup, identity)
    assert before["evaluated_index"] == 480 and before["last_evaluated_projection"] is not None
    with setup.database.session_factory() as session:
        public_before = build_detector_service(session, setup.catalog).public_card(identity).last_evaluated.model_dump()
        findings_before = session.scalar(select(func.count()).select_from(Finding).where(Finding.detector_id == identity))
    refused = uncomputed_point(setup, 481)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        service.models.restore = lambda *_: pytest.fail("uncomputed restored the native forest")
        assert service.advance_one(identity) == {"state": "skipped", "reason": "execution_budget_exceeded",
            "point_id": refused, "finding": None}
    after = stored(setup, identity)
    assert after["consumed_index"] == 481
    assert after["evaluated_index"] == before["evaluated_index"]
    assert after["last_evaluated_projection"] == before["last_evaluated_projection"]
    assert after["checkpoint"] == before["checkpoint"]
    with setup.database.session_factory() as session:
        outcome = session.scalar(select(DetectorOutcome).where(DetectorOutcome.detector_id == identity,
            DetectorOutcome.working_point_id == refused))
        assert outcome.kind == "skipped" and outcome.reason == "execution_budget_exceeded"
        assert session.scalar(select(func.count()).select_from(Finding).where(Finding.detector_id == identity)) == findings_before
        public = build_detector_service(session, setup.catalog).public_card(identity)
        assert public.last_consumed.index == 481 and public.last_consumed.kind == "skipped"
        assert public.last_consumed.reason.code == "execution_budget_exceeded"
        declared = session.get(AggregateClosure, refused).execution
        assert public.last_consumed.aggregation_started_at == datetime.fromisoformat(declared["started_at"])
        assert public.last_evaluated.model_dump() == public_before
    # The following native model transition still uses the confirmed forest.
    point(setup, 482, 1000)
    assert advance(setup, identity)["state"] == "evaluated"
    assert stored(setup, identity)["checkpoint"]["body"]["updates"] == before["checkpoint"]["body"]["updates"] + 1


@pytest.mark.parametrize("setup", ["overlap"], indirect=True)
@pytest.mark.parametrize("accelerated", [False, True])
def test_both_warmups_reject_terminal_uncomputed_without_raw_reconstruction(setup, accelerated):
    for index in range(16, 48):
        if index != 43:
            point(setup, index, 1 + index % 3)
    uncomputed_point(setup, 43)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Refused history",
            config={"sample_size": 16, "output_after": 16, "trees": 20},
            accelerated=accelerated, step=1 if accelerated else None)
        freeze_attempt_clock(service, setup.cutoff)
        result = prepare(service, identity)
        assert result["state"] == "waiting" and result["reason"] == "warmup_window_uncomputed", result
        attempt = service.repository.attempt(result["attempt_id"])
        assert attempt["status"] == "waiting" and attempt["inputs"] == []
        state = service.state(identity)["detector"]
        assert state["checkpoint"] is None and state["consumed_index"] is None


@pytest.mark.parametrize("fault", ["before_commit", "lost_commit_ack"])
def test_failure_and_retry_match_one_sequential_training(setup, fault):
    history(setup)
    baseline, tested = prepared(setup), prepared(setup)
    point(setup, 480, 1000)
    expected = advance(setup, baseline)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        if fault == "before_commit":
            original = service.repository.save_transition
            def fail(*args):
                original(*args)
                raise RuntimeError("injected after checkpoint and finding flush")
            service.repository.save_transition = fail
        else:
            original = service.uow.commit
            def fail():
                original()
                raise RuntimeError("injected lost acknowledgement")
            service.uow.commit = fail
        with pytest.raises(RuntimeError, match="injected"):
            service.advance_one(tested)
    retry = advance(setup, tested)
    assert retry["state"] == ("evaluated" if fault == "before_commit" else "waiting")
    assert stored(setup, tested)["checkpoint"]["body"]["mapper"] == stored(setup, baseline)["checkpoint"]["body"]["mapper"]
    point(setup, 481, 2)
    assert advance(setup, tested)["finding"] == advance(setup, baseline)["finding"]
    with setup.database.session_factory() as session:
        outcomes = list(session.scalars(select(DetectorOutcome).where(DetectorOutcome.detector_id == tested)))
        findings = list(session.scalars(select(Finding).where(Finding.detector_id == tested)))
        assert len(outcomes) == 2 and len(findings) >= 1
    assert expected["finding"]


def test_delete_rejects_stale_candidate_and_preserves_history(setup):
    history(setup)
    identity = prepared(setup)
    point(setup, 480, 1000)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        original = service.models.restore
        def restore(*args):
            model = original(*args)
            process = model.process
            def race(values):
                result = process(values)
                with setup.database.session_factory() as deleting:
                    build_aggregate_service(deleting, setup.catalog).lifecycle(setup.card.id, "delete", UUID(int=1))
                return result
            model.process = race
            return model
        service.models.restore = restore
        with pytest.raises(StaleCandidate, match="aggregate_execution_fenced"):
            service.advance_one(identity)
    assert stored(setup, identity)["state"] == "stopped"
    assert stored(setup, identity)["consumed_index"] is None
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        with pytest.raises(DetectionError, match="aggregate_inactive"):
            service.create(setup.card.id, UUID(int=1), "Rejected")
        assert session.scalar(select(DetectorWarmup).where(DetectorWarmup.detector_id == identity)).inputs


def test_snapshot_corruption_never_resets_forest(setup):
    history(setup)
    identity = prepared(setup)
    detector = stored(setup, identity)
    factory = AwsModelFactory()
    for kind in ("checksum", "version", "config"):
        state = copy.deepcopy(detector["checkpoint"])
        if kind == "checksum":
            state["body"]["updates"] += 1
        if kind == "version":
            state["body"]["runtime"]["format"] = 999
            state["sha256"] = digest(state["body"])
        if kind == "config":
            state["body"]["keys"] = ["wrong"]
            state["sha256"] = digest(state["body"])
        with pytest.raises(DetectionError):
            factory.restore(state, detector["config"], detector["feature_keys"], detector["rule_hash"])


def test_corrupt_committed_checkpoint_stops_with_durable_diagnostic(setup):
    history(setup)
    identity = prepared(setup)
    with setup.database.session_factory.begin() as session:
        detector = session.get(Detector, identity)
        bad = copy.deepcopy(detector.checkpoint)
        bad["sha256"] = "0" * 64
        detector.checkpoint, detector.revision = bad, detector.revision + 1
    point(setup, 480, 1000)
    with pytest.raises(DetectionError, match="checkpoint_envelope"):
        advance(setup, identity)
    state = stored(setup, identity)
    assert state["state"] == "error" and "checkpoint" in state["reason"]
    assert state["consumed_index"] is None and state["checkpoint"] == bad
    with pytest.raises(DetectionError, match="execution_not_authorized"):
        advance(setup, identity)


def test_constant_history_waits(setup):
    history(setup, constant=True)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Constant",
                                  config={"sample_size": 16, "output_after": 16})
        freeze_attempt_clock(service, setup.cutoff)
        assert prepare(service, identity)["state"] == "prepared"
        assert service.warmup(identity) == {"state": "waiting", "reason": "constant_history"}


def test_two_sessions_discard_stale_candidate_without_double_training(setup):
    history(setup)
    identity, baseline = prepared(setup), prepared(setup)
    point(setup, 480, 1000)
    advance(setup, baseline)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        original = service.models.restore
        def restore(*args):
            model = original(*args)
            process = model.process
            def compete(values):
                result = process(values)
                assert advance(setup, identity)["state"] == "evaluated"
                return result
            model.process = compete
            return model
        service.models.restore = restore
        with pytest.raises(StaleCandidate):
            service.advance_one(identity)
    assert stored(setup, identity)["checkpoint"]["body"]["mapper"] == stored(setup, baseline)["checkpoint"]["body"]["mapper"]


def test_bounded_precision_collision_skips_without_training(setup):
    history(setup)
    identity = prepared(setup)
    point(setup, 480, 2**60)
    assert advance(setup, identity)["state"] == "evaluated"
    checkpoint = stored(setup, identity)["checkpoint"]
    point(setup, 481, 2**60 + 1)
    result = advance(setup, identity)
    assert result["state"] == "skipped" and "collision" in result["reason"]
    assert stored(setup, identity)["checkpoint"] == checkpoint
    assert stored(setup, identity)["evaluated_index"] == 480


def test_archive_gap_resume_and_final_delete(setup):
    from app.models import AggregateSchedule

    history(setup)
    identity = prepared(setup)
    point(setup, 480, 2)
    advance(setup, identity)
    before = stored(setup, identity)["checkpoint"]
    with setup.database.session_factory.begin() as session:
        session.get(AggregateSchedule, setup.card.schedule_id).next_index = 481
    with setup.database.session_factory() as session:
        aggregate = build_aggregate_service(session, setup.catalog)
        aggregate.lifecycle(setup.card.id, "archive", UUID(int=1))
        service = build_detector_service(session, setup.catalog)
        with pytest.raises(DetectionError, match="aggregate_inactive"):
            service.create(setup.card.id, UUID(int=1), "Rejected")
        assert service.advance_one(identity)["state"] == "waiting"
        card = aggregate.lifecycle(setup.card.id, "resume", UUID(int=1))
        assert card.skipped_ranges
    assert stored(setup, identity)["checkpoint"] == before
    point(setup, card.next_index, 2)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        original = service.repository.read_context
        service.repository.read_context = lambda value: {**original(value), "now": datetime.now(UTC) + timedelta(days=1)}
        assert service.advance_one(identity)["state"] == "evaluated"
    with setup.database.session_factory() as session:
        build_aggregate_service(session, setup.catalog).lifecycle(setup.card.id, "delete", UUID(int=1))
    assert stored(setup, identity)["state"] == "stopped"


def test_worker_discards_candidate_archived_during_real_model_and_resumes(setup, request):
    from threading import Event

    from app.models import AggregateSchedule
    from app.services.implementations.detector_worker import DetectorWorker

    history(setup)
    identity = prepared(setup)
    point(setup, 480, 1000)
    with setup.database.session_factory.begin() as session:
        session.get(AggregateSchedule, setup.card.schedule_id).next_index = 481
    before = stored(setup, identity)
    raced = []
    from app.adapters.model_runtime import ModelRuntime
    transport = ModelRuntime()
    request.addfinalizer(transport.close)
    def build(session, catalog, owner, cancel):
        service = build_worker_detector_service(session, catalog, owner, cancel, transport)
        original = service.models.restore
        def restore(*args):
            model = original(*args)
            process = model.process
            def archive_after_process(values):
                result = process(values)
                if not raced:
                    with setup.database.session_factory() as other:
                        build_aggregate_service(other, catalog).lifecycle(setup.card.id, "archive", UUID(int=1))
                    raced.append(True)
                return result
            model.process = archive_after_process
            return model
        service.models.restore = restore
        freeze_attempt_clock(service, datetime.now(UTC) + timedelta(days=1))
        return service
    worker = DetectorWorker(setup.database.session_factory, build, setup.catalog, Event(), None)
    worker.owner = OWNER
    assert worker._step(identity)["state"] == "stale" and raced
    after = stored(setup, identity)
    assert after["execution_state"] == "running" and after["state"] == "ready"
    assert after["failure_code"] is None and after["last_failure"] == before["last_failure"]
    assert after["checkpoint"] == before["checkpoint"] and after["consumed_index"] == before["consumed_index"]
    with setup.database.session_factory() as session:
        assert not session.scalar(select(func.count()).select_from(DetectorOutcome).where(DetectorOutcome.detector_id == identity))
        assert not session.scalar(select(func.count()).select_from(Finding).where(Finding.detector_id == identity))
        service = build_detector_service(session, setup.catalog)
        assert worker._step(identity)["state"] == "waiting"
        with pytest.raises(DetectionError, match="aggregate_inactive"):
            service.start(identity)
        card = build_aggregate_service(session, setup.catalog).lifecycle(setup.card.id, "resume", UUID(int=1))
        assert card.skipped_ranges
    assert worker._step(identity)["state"] == "evaluated"
    point(setup, card.next_index, 2)
    assert worker._step(identity)["state"] == "evaluated"
    final = stored(setup, identity)
    assert final["execution_state"] == "running" and final["consumed_index"] == card.next_index
    assert final["active_attempt_id"] == before["active_attempt_id"]


def test_unknown_hole_never_jumps_cursor(setup):
    history(setup)
    identity = prepared(setup)
    point(setup, 481, 1000)
    assert advance(setup, identity)["reason"] == "working_point_gap"
    with setup.database.session_factory() as session:
        assert build_detector_service(session, setup.catalog).state(identity)["diagnostics"]["code"] == "working_point_gap"
    assert stored(setup, identity)["consumed_index"] is None


def test_checkpoint_write_failure_rolls_back_all_outputs(setup):
    history(setup)
    identity = prepared(setup)
    point(setup, 480, 1000)
    before = stored(setup, identity)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        original = service.models.restore
        def restore(*args):
            model = original(*args)
            def failed_checkpoint(_boundary):
                raise DetectionError("injected_serialization_failure")
            model.checkpoint = failed_checkpoint
            return model
        service.models.restore = restore
        with pytest.raises(DetectionError, match="serialization_failure"):
            service.advance_one(identity)
    after = stored(setup, identity)
    assert after["checkpoint"] == before["checkpoint"] and after["consumed_index"] is None


def test_competing_warmup_attempt_cannot_confirm_old_candidate(setup):
    history(setup)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Warmup race",
            config={"sample_size": 16, "output_after": 16, "trees": 20})
        freeze_attempt_clock(service, setup.cutoff)
        prepare(service, identity)
        original_warmup = service.models.warmup
        def compete(*args):
            checkpoint = original_warmup(*args)
            with setup.database.session_factory() as competing:
                other = build_detector_service(competing, setup.catalog)
                freeze_attempt_clock(other, setup.cutoff)
                other.stop(identity)
                assert prepare(other, identity)["state"] == "prepared"
                assert other.warmup(identity)["state"] == "ready"
            return checkpoint
        service.models.warmup = compete
        with pytest.raises(StaleCandidate):
            service.warmup(identity)
    assert stored(setup, identity)["state"] == "ready"


def test_real_model_more_than_five_dimensions_and_constant_coordinate():
    from dataclasses import asdict

    from app.detection.contracts import ModelConfig
    config = asdict(ModelConfig(dimensions=7, trees=20, sample_size=16, output_after=16))
    rows = [[1 + i % 3 + j for j in range(6)] + [5] for i in range(32)]
    keys = [str(i) for i in range(7)]
    factory = AwsModelFactory()
    checkpoint = factory.warmup(config, keys, "synthetic", rows)
    assert checkpoint["body"]["affine"]["scale"][-1] == "1"
    model = factory.restore(checkpoint, config, keys, "synthetic")
    result = model.process([1000] * 6 + [5])
    assert result["grade"] > 0 and len(result["expected"]) == 7


def test_database_timezone_preserves_warmup_and_decisions(setup, monkeypatch):
    import time
    history(setup)
    previous = os.environ.get("TZ")
    try:
        monkeypatch.setenv("TZ", "Asia/Tokyo")
        time.tzset()
        first = prepared(setup, timezone="Asia/Tokyo")
        monkeypatch.setenv("TZ", "America/New_York")
        time.tzset()
        second = prepared(setup, timezone="America/New_York")
        assert stored(setup, first)["checkpoint"] == stored(setup, second)["checkpoint"]
        point(setup, 480, 1000)
        results = []
        for identity, timezone in [(first, "Asia/Tokyo"), (second, "America/New_York")]:
            monkeypatch.setenv("TZ", timezone)
            time.tzset()
            with setup.database.session_factory() as session:
                session.execute(select(func.set_config("TimeZone", timezone, False)))
                service = build_detector_service(session, setup.catalog)
                freeze_attempt_clock(service, setup.cutoff + timedelta(seconds=2.1))
                service.recover(identity)
                results.append(service.advance_one(identity))
                assert session.scalar(select(Finding.late).where(Finding.detector_id == identity))
        assert results[0] == results[1]
    finally:
        if previous is None:
            monkeypatch.delenv("TZ", raising=False)
        else:
            monkeypatch.setenv("TZ", previous)
        time.tzset()


@pytest.mark.parametrize("phase", ["prepare", "warmup"])
def test_delete_cancels_unconfirmed_warmup(setup, phase):
    history(setup)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Deletion race",
            config={"sample_size": 16, "output_after": 16, "trees": 20})
        freeze_attempt_clock(service, setup.cutoff)
        if phase == "warmup":
            prepare(service, identity)
            original = service.models.warmup
        else:
            original = service.history.materialize
        def delete_before_final_save(*args):
            result = original(*args)
            with setup.database.session_factory() as deleting:
                build_aggregate_service(deleting, setup.catalog).lifecycle(setup.card.id, "delete", UUID(int=1))
            return result
        if phase == "warmup":
            service.models.warmup = delete_before_final_save
        else:
            service.history.materialize = delete_before_final_save
        with pytest.raises(StaleCandidate, match="aggregate_execution_fenced"):
            (service.warmup(identity) if phase == "warmup" else prepare(service, identity))
    state = stored(setup, identity)
    assert state["state"] == "stopped" and state["checkpoint"] is None


def test_delete_mutex_prevents_concurrent_create(setup):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    started = Event()
    def create():
        with setup.database.session_factory() as creating:
            started.set()
            return build_detector_service(creating, setup.catalog).create(setup.card.id, UUID(int=1), "Rejected")
    with setup.database.session_factory() as deleting, ThreadPoolExecutor(max_workers=1) as pool:
        aggregate = build_aggregate_service(deleting, setup.catalog)
        aggregate._get(setup.card.id, lock=True)
        pending = pool.submit(create)
        assert started.wait(5)
        aggregate.lifecycle(setup.card.id, "delete", UUID(int=1))
        with pytest.raises(DetectionError, match="aggregate_inactive"):
            pending.result(timeout=10)


def test_missing_canonical_history_waits_without_training(setup):
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Unknown history",
            config={"sample_size":16,"output_after":16,"trees":20})
        result = prepare(service,identity)
        assert result["state"] == "waiting" and result["reason"] == "insufficient_history"
        assert stored(setup,identity)["checkpoint"] is None


def test_preparation_locks_schedule_before_detector_without_source_barrier(setup):
    history(setup)
    with setup.database.session_factory() as session:
        service=build_detector_service(session, setup.catalog)
        identity=service.create(setup.card.id,UUID(int=1),"Lock order",config={"sample_size":16,"output_after":16,"trees":20})
        service.start(identity);service.begin_start(identity)
        trace=[]
        row=service.repository._row
        def traced(model, identity, lock=False):
            result=row(model,identity,lock)
            if lock: trace.append(model.__tablename__)
            return result
        service.repository._row=traced
        service.windows.lock_source=lambda *_:pytest.fail("canonical warmup requested a source barrier")
        assert prepare(service,identity)["state"] == "prepared"
        assert trace.index("aggregate_schedules") < trace.index("detectors")


def test_schedule_lock_wait_is_bounded_and_does_not_commit_attempt(setup):
    import time

    from sqlalchemy.exc import DBAPIError

    from app.models import AggregateSchedule
    history(setup)
    with setup.database.session_factory() as session:
        service=build_detector_service(session,setup.catalog)
        identity=service.create(setup.card.id,UUID(int=1),"Bounded lock",config={"sample_size":16,"output_after":16,"trees":20})
        service.start(identity);service.begin_start(identity)
        before=service.state(identity)["detector"]
    with setup.database.session_factory() as blocking, setup.database.session_factory() as session:
        blocking.scalar(select(AggregateSchedule).where(AggregateSchedule.id==setup.card.schedule_id).with_for_update())
        service=build_detector_service(session,setup.catalog)
        started=time.monotonic()
        with pytest.raises(DBAPIError): service.prepare_warmup(identity)
        assert 4 <= time.monotonic()-started <12
    state=stored(setup,identity)
    assert state["revision"]==before["revision"] and state["active_attempt_id"] is None


@pytest.mark.parametrize("phase", ["fit", "collision", "convert"])
def test_preparation_deadline_stops_numeric_work_without_committing_batch(setup, monkeypatch, phase):
    from app.detection import numeric
    from app.services.implementations import detector_history

    originals = [[2**60 + i] for i in range(512)]
    for i, values in enumerate(originals,start=48):
        point(setup,i,values[0])
    clock = {"now": 0, "exact": 0, "convert": 0, "expired_at": None}
    exact = numeric.exact
    convert = numeric.Affine.convert
    def expire():
        clock["now"], clock["expired_at"] = 31, clock["exact"]
    def counted_exact(*args):
        clock["exact"] += 1
        if phase == "fit" and clock["exact"] == 600:
            expire()
        return exact(*args)
    def counted_convert(self, *args, **kwargs):
        clock["convert"] += 1
        if phase == "convert" and clock["convert"] == 513:
            expire()
        result = convert(self, *args, **kwargs)
        if phase == "collision" and clock["convert"] == 1:
            expire()
        return result
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Numeric deadline")
        freeze_attempt_clock(service, setup.anchor + timedelta(seconds=560))
        with monkeypatch.context() as controlled:
            controlled.setattr(numeric, "exact", counted_exact)
            controlled.setattr(numeric.Affine, "convert", counted_convert)
            controlled.setattr(detector_history.time, "monotonic", lambda: clock["now"])
            result = prepare(service, identity)
        assert clock["expired_at"] is not None
        assert clock["exact"] - clock["expired_at"] <= 1
        assert clock["exact"] <= {"fit": 601, "collision": 1026, "convert": 1537}[phase]
        assert result["state"] == "waiting"
        assert result["reason"] == "resource_budget:preparation_seconds"
        state = service.state(identity)
        assert state["detector"]["state"] == "waiting" and state["detector"]["checkpoint"] is None
        assert state["warmup"]["status"] == "waiting" and state["warmup"]["inputs"] == []
        assert not session.scalar(select(func.count()).select_from(Finding).where(Finding.detector_id == identity))
        assert not session.scalar(select(func.count()).select_from(DetectorOutcome).where(DetectorOutcome.detector_id == identity))
        actual = list(session.scalars(select(WorkingWindowPoint.features).where(
            WorkingWindowPoint.schedule_id == setup.card.schedule_id).order_by(WorkingWindowPoint.window_index)))
        assert [[features[0]["value"]] for features in actual] == originals


@pytest.mark.parametrize("setup", ["count", "bytes", "empty"], indirect=True)
def test_real_event_time_aggregate_preserves_finding_facts(setup):
    from app.models import AggregateSchedule, ParsedLog, ProcessedKafkaRecord
    from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
    history(setup)
    identity=prepared(setup)
    start=setup.anchor+timedelta(seconds=480)
    occurred=start+timedelta(microseconds=100000)
    count=0 if setup.kind=="empty" else 1 if setup.kind=="bytes" else 100
    with setup.database.session_factory.begin() as session:
        session.get(AggregateSchedule,setup.card.schedule_id).next_index=480
        writer=SqlAlchemyProcessingRepository(session)
        for i in range(count):
            record=ProcessedKafkaRecord(id=uuid4(),connection_id=setup.connection_id,connection_identity=setup.connection_id,
                source_id=setup.source_id,kafka_topic_identity="synthetic",kafka_topic=str(setup.source_id),kafka_partition=0,
                kafka_offset=i,result_status="complete",backend_received_at=start,backend_processed_at=start)
            event=ParsedLog(id=uuid4(),source_id=setup.source_id,connection_id=setup.connection_id,normalizer_version=1,
                normalizer_name="Synthetic",source_name=setup.kind,connection_name="Synthetic",kafka_topic=str(setup.source_id),
                kafka_partition=0,kafka_offset=i,deduplication_key=str(uuid4()),raw="synthetic",
                ecs_data={"@timestamp":occurred.isoformat(),"network":{"bytes":1000}},
                normalization_status="complete",normalization_diagnostics=[],fluent_bit_collected_at=start,
                backend_received_at=start,backend_processed_at=start)
            assert writer.store_event(record,event)
    with setup.database.session_factory() as session:
        service=build_aggregate_service(session,setup.catalog)
        result=service.advance(setup.card.id)
        assert (result["state"],result.get("origin"))==("closed","online")
        working=service.point_at(setup.card.id,480)
        assert result["point_id"]==working.id
        assert working.completeness=="complete"
        assert working.features[0].value==(0 if setup.kind=="empty" else 1000 if setup.kind=="bytes" else 100)
    result=advance(setup,identity)
    assert result["state"]=="evaluated"
    if setup.kind!="empty":
        assert result["finding"]
        assert result["finding"]["source_id"]==str(setup.source_id)
        assert result["finding"]["event_time_min"]==occurred.isoformat()
        assert result["finding"]["time_basis"]=="event_time" and result["finding"]["start"]==start.isoformat()


def overlapping_history(setup, accelerated=True, fault=None):
    from app.aggregation.event_time import warmup_indices
    from app.models import KafkaCheckpoint, ParsedLog, ProcessedKafkaRecord, SourceInterruption
    from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
    indices = warmup_indices(47, 10, 1 if accelerated else 10, 16, 16, accelerated)
    start = setup.anchor + timedelta(seconds=indices["base_first"]*10)
    end = setup.cutoff
    events = []
    with setup.database.session_factory.begin() as session:
        session.execute(select(func.set_config("TimeZone", "America/New_York", False)))
        writer = SqlAlchemyProcessingRepository(session)
        for second in range(int((end-start).total_seconds())):
            when = start + timedelta(seconds=second)
            for _ in range(1 + second % 3):
                index = len(events)
                record = ProcessedKafkaRecord(id=uuid4(), connection_id=setup.connection_id,
                    connection_identity=setup.connection_id, source_id=setup.source_id,
                    kafka_topic_identity="synthetic", kafka_topic=str(setup.source_id), kafka_partition=0,
                    kafka_offset=index, result_status="complete", backend_received_at=when, backend_processed_at=when)
                event = ParsedLog(id=uuid4(), source_id=setup.source_id, connection_id=setup.connection_id,
                    normalizer_version=1, normalizer_name="ECS", source_name="overlap", connection_name="Synthetic",
                    kafka_topic=str(setup.source_id), kafka_partition=0, kafka_offset=index,
                    deduplication_key=str(uuid4()), raw="synthetic", ecs_data={"@timestamp": when.isoformat()},
                    normalization_status="complete", normalization_diagnostics=[], fluent_bit_collected_at=when,
                    backend_received_at=when, backend_processed_at=when)
                assert writer.store_event(record, event)
                events.append(when)
        if fault in {"backlog","retention"}:
            session.add(KafkaCheckpoint(source_identity=setup.source_id,topic_identity="synthetic",worker_instance=uuid4(),
                observed_at=end-timedelta(seconds=1),partitions={"0":{"low":len(events)+1 if fault=="retention" else 0,
                    "high":len(events),"committed":len(events)-1 if fault=="backlog" else len(events)}}))
        elif fault:
            session.add(SourceInterruption(source_identity=setup.source_id, kind="processing_stopped",
                started_at=start-timedelta(seconds=1), ended_at=end+timedelta(seconds=1)))
    for _ in range(3*(48-indices["base_first"])+9):
        with setup.database.session_factory() as session:
            aggregate = build_aggregate_service(session, setup.catalog)
            aggregate.repository.now = lambda: setup.cutoff
            aggregate.advance(setup.card.id)
    first = setup.anchor + timedelta(seconds=indices["starts"][0])
    plan = {"cutoff": end.isoformat(), "first_start": first.isoformat(), "count": indices["count"],
        "step": 1 if accelerated else 10, "duration": 10,
        "coverage_seconds": 10+(indices["count"]-1)*(1 if accelerated else 10), "evaluation_start_index":48}
    with setup.database.session_factory() as session:
        bases = session.execute(select(AggregateClosure.origin_point_id, AggregateClosure.window_index)
            .where(AggregateClosure.schedule_id == setup.card.schedule_id,
                AggregateClosure.window_index >= indices["base_first"], AggregateClosure.window_index <= indices["base_last"],
                AggregateClosure.payload_state == "present").order_by(AggregateClosure.window_index)).all()
    assert [row.window_index for row in bases] == list(range(indices["base_first"], indices["base_last"] + 1))
    plan["base_closures"] = [{"origin_point_id": str(row.origin_point_id), "window_index": row.window_index} for row in bases]
    windows = [(first+timedelta(seconds=i*plan["step"]),first+timedelta(seconds=i*plan["step"]+10)) for i in range(plan["count"])]
    return plan, windows, events, uuid4()


@pytest.mark.parametrize("setup", ["overlap"], indirect=True)
@pytest.mark.parametrize("accelerated", [False, True])
def test_real_checkpoint_and_saved_inputs_survive_automatic_source_gc_restart(setup, accelerated, monkeypatch):
    """Core storage/restart proof; elapsed-time worker/load is a separate live gate."""
    from sqlalchemy.exc import DBAPIError

    from app.aggregation.event_time import OperationBudget
    from app.detection.protection import checkpoint_boundary
    from app.models import (
        AggregateLateEvent,
        AggregateMembership,
        AggregateRetentionState,
        ParsedLog,
    )

    _plan, _windows, events, _worker = overlapping_history(setup, accelerated)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Real checkpoint GC",
            config={"sample_size": 16, "output_after": 16, "trees": 20},
            accelerated=accelerated, step=1 if accelerated else None)
        freeze_attempt_clock(service, setup.cutoff)
        ready = prepare(service, identity)
        assert ready["state"] == "prepared", ready
        prepared_revision = service.state(identity)["detector"]["revision"]
        assert service.warmup(identity) == {"state": "ready"}
        published = service.state(identity)["detector"]
        assert published["revision"] == prepared_revision + 1
        assert published["state"] == "ready" and published["checkpoint"] is not None
        assert service.repository.attempt(published["active_attempt_id"])["status"] == "ready"
        pre = published["checkpoint"]["body"]["mapper"]["preprocessorStates"][0]
        print({"checkpoint_preprocessor_shapes": {key:
            len(pre[key]) if isinstance(pre[key], list) else type(pre[key]).__name__
            for key in ("weights", "lastShingledPoint", "lastShingledInput", "previousTimeStamps",
                "deviationStates", "dataQualityStates", "timeStampDeviationStates")},
            "dimensions": published["config"]["dimensions"], "accelerated": accelerated})
        validation_started = time.monotonic()
        assert checkpoint_boundary(published, OperationBudget(.85).check) == 48
        validation_seconds = time.monotonic() - validation_started
        print({"checkpoint_validation_seconds": validation_seconds, "accelerated": accelerated})
        assert validation_seconds < .85
        service.stop(identity)
        model = service.state(identity)["detector"]
        checkpoint = copy.deepcopy(model["checkpoint"])
        saved = copy.deepcopy(service.repository.attempt(model["active_attempt_id"]))
        bases = [UUID(row["origin_point_id"]) for row in saved["plan"]["base_closures"]]
        members = set(session.execute(select(AggregateMembership.closure_id, AggregateMembership.event_id)
            .where(AggregateMembership.closure_id.in_(bases))).all())
    # One distant canonical closure is a controlled storage fixture, not a
    # fabricated run of 600 online evaluations. It makes the saved checkpoint's
    # unread suffix exceed B and leaves the original batch eligible for GC.
    unread = point(setup, 1000, 50, completeness="unknown")
    with setup.database.session_factory() as session:
        aggregate = build_aggregate_service(session, setup.catalog)
        commit, first = aggregate.uow.commit, [True]
        def lose_ack():
            commit()
            if first[0]:
                first[0] = False
                raise DBAPIError(None, None, RuntimeError("lost acknowledgement"))
        monkeypatch.setattr(aggregate.uow, "commit", lose_ack)
        assert aggregate.maintain(setup.card.id)["pending"]
    for _ in range(len(bases) * 5 + 8):
        with setup.database.session_factory() as session:
            result = build_aggregate_service(session, setup.catalog).maintain(setup.card.id)
            assert result["status"] != "error", result
            state = session.get(AggregateRetentionState, setup.card.id)
            if state.auto_cleared_total == len(bases) and state.pending_closure_id is None:
                break
    with setup.database.session_factory() as session:
        aggregate = build_aggregate_service(session, setup.catalog)
        summary = aggregate.card(setup.card.id).auto_cleanup
        assert summary.reserve_windows == 512 and summary.protected_from_index == 48
        assert summary.keep_from_index == 48 and summary.auto_cleared_total == len(bases)
        assert summary.history_revision == len(bases)
        assert session.get(WorkingWindowPoint, unread) is not None
        assert all(session.get(WorkingWindowPoint, origin) is None for origin in bases)
        assert set(session.execute(select(AggregateMembership.closure_id, AggregateMembership.event_id)
            .where(AggregateMembership.closure_id.in_(bases))).all()) == members
        assert session.scalar(select(func.count()).select_from(ParsedLog).where(ParsedLog.source_id == setup.source_id)) == len(events)
        service = build_detector_service(session, setup.catalog)
        current = service.state(identity)["detector"]
        assert current["execution_state"] == "stopped" and current["checkpoint"] == checkpoint
        assert service.repository.attempt(current["active_attempt_id"]) == saved
        assert current["consumed_index"] is current["evaluated_index"] is None
        # A fresh service uses the real mapper/runtime to restore the unchanged
        # committed model after source payloads and heavy manifests disappeared.
        service.start(identity)
        service.begin_start(identity)
        assert service.resume(identity) == {"state": "running"}
        assert service.state(identity)["detector"]["checkpoint"] == checkpoint
        assert service.repository.attempt(saved["id"]) == saved
    for _ in range(6):
        with setup.database.session_factory() as session:
            aggregate = build_aggregate_service(session, setup.catalog)
            aggregate.repository.now = lambda: setup.cutoff
            aggregate.advance(setup.card.id)
    with setup.database.session_factory() as session:
        assert all(session.get(WorkingWindowPoint, origin) is None for origin in bases)
        assert session.scalar(select(func.count()).select_from(AggregateLateEvent).where(AggregateLateEvent.closure_id.in_(bases))) == 0






@pytest.mark.parametrize("setup", ["overlap"], indirect=True)
@pytest.mark.parametrize("arrival", ["before_attempt", "after_attempt", "missing"])
def test_ordinary_history_never_repairs_unknown_canonical_batch_with_new_evidence(setup, arrival):
    from app.models import KafkaCheckpoint
    plan, _, _, worker = overlapping_history(setup, accelerated=False, fault="stop")
    with setup.database.session_factory() as session:
        service=build_detector_service(session,setup.catalog)
        identity=service.create(setup.card.id,UUID(int=1),"No fresh fallback",config={"sample_size":16,"output_after":16,"trees":20})
        service.history.aggregates.event_statistics=lambda *_:pytest.fail("ordinary history recalculated raw")
        result=prepare(service,identity)
        assert result["state"]=="waiting" and result["reason"]=="coverage:interrupted"
        attempt=service.repository.attempt(result["attempt_id"])
        assert attempt["plan"]==plan and all(i["completeness"]=="interrupted" for i in attempt["inputs"])
        before=copy.deepcopy(attempt["inputs"])
    if arrival!="missing":
        with setup.database.session_factory.begin() as session:
            session.add(KafkaCheckpoint(source_identity=setup.source_id, topic_identity="synthetic",worker_instance=worker,
                partitions={"0":{"low":0,"high":0,"committed":0}}))
    with setup.database.session_factory() as session:
        assert build_detector_service(session,setup.catalog).repository.attempt(attempt["id"])["inputs"]==before
    assert stored(setup,identity)["checkpoint"] is None


@pytest.mark.parametrize("setup", ["overlap"], indirect=True)
@pytest.mark.parametrize("accelerated", [False, True])
def test_history_optional_checkpoint_does_not_replace_canonical_cutoff(setup, accelerated):
    from app.models import AvailabilityMarker, KafkaCheckpoint
    plan,_,events,worker=overlapping_history(setup,accelerated)
    with setup.database.session_factory() as session:
        service=build_detector_service(session,setup.catalog)
        identity=service.create(setup.card.id,UUID(int=1),"Fixed cutoff",config={"sample_size":16,"output_after":16,"trees":20},accelerated=accelerated,step=1 if accelerated else None)
        result=prepare(service,identity)
        assert result["state"]=="prepared",result
        attempt=service.repository.attempt(result["attempt_id"])
        assert attempt["plan"]==plan and attempt["cutoff"]==setup.cutoff
        assert session.scalar(select(func.count()).select_from(AvailabilityMarker).where(AvailabilityMarker.source_identity==setup.source_id,AvailabilityMarker.kind=="barrier"))==0
        before=copy.deepcopy(attempt)
    with setup.database.session_factory.begin() as session:
        session.add(KafkaCheckpoint(source_identity=setup.source_id,topic_identity="synthetic",worker_instance=worker,partitions={"0":{"low":0,"high":len(events),"committed":len(events)}}))
    with setup.database.session_factory() as session:
        assert build_detector_service(session,setup.catalog).repository.attempt(attempt["id"])==before


def test_history_refreshes_remaining_sql_budget_between_working_reads(setup):
    from sqlalchemy import event

    history(setup)
    observed = []
    def inspect_read(_connection, cursor, statement, _parameters, _context, _many):
        if "logs.working_window_points" not in statement or not statement.lstrip().upper().startswith("SELECT"):
            return
        cursor.execute("SHOW statement_timeout")
        value = cursor.fetchone()[0]
        milliseconds = float(value[:-2]) if value.endswith("ms") else float(value[:-1]) * 1000
        observed.append(milliseconds)
        if len(observed) == 1:
            cursor.execute("SELECT pg_sleep(0.08)")
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Per statement budget",
            config={"sample_size": 16, "output_after": 16, "trees": 20})
        freeze_attempt_clock(service, setup.cutoff)
        event.listen(setup.database.engine, "before_cursor_execute", inspect_read)
        try:
            assert prepare(service, identity)["state"] == "prepared"
        finally:
            event.remove(setup.database.engine, "before_cursor_execute", inspect_read)
        assert len(observed) == 2
        assert observed[1] <= observed[0] - 50


@pytest.mark.parametrize("setup", ["overlap"], indirect=True)
@pytest.mark.parametrize("mutation", ["delete", "ecs", "diagnostics"])
def test_overlap_missing_or_changed_saved_raw_is_rejected_before_training(setup, mutation):
    from app.models import ParsedLog
    overlapping_history(setup)
    with setup.database.session_factory.begin() as session:
        row=session.scalar(select(ParsedLog).where(ParsedLog.source_id==setup.source_id))
        if mutation == "delete":
            session.delete(row)
        elif mutation == "ecs":
            row.ecs_data = {**row.ecs_data, "network": {"bytes": 2**60 + 1}}
        else:
            row.normalization_diagnostics = [{"code": "synthetic_changed_diagnostic"}]
    with setup.database.session_factory() as session:
        service=build_detector_service(session,setup.catalog)
        identity=service.create(setup.card.id,UUID(int=1),"Missing member",config={"sample_size":16,"output_after":16,"trees":20},accelerated=True,step=1)
        result=prepare(service,identity)
        assert result["state"]=="waiting" and result["reason"]=="warmup_membership_source_unavailable",result
        attempt=service.repository.attempt(result["attempt_id"])
        assert attempt["inputs"]==[] and stored(setup,identity)["checkpoint"] is None


@pytest.mark.parametrize("setup", ["overlap"], indirect=True)
@pytest.mark.parametrize("blocked", [False, True])
def test_history_read_deadline_has_no_partial_inputs_or_automatic_retry(setup, blocked, monkeypatch):
    import time

    from sqlalchemy import text
    overlapping_history(setup)
    with setup.database.session_factory() as session, setup.database.session_factory() as other:
        service=build_detector_service(session,setup.catalog)
        identity=service.create(setup.card.id,UUID(int=1),"Bounded snapshot",config={"sample_size":16,"output_after":16,"trees":20},accelerated=True,step=1)
        materialize=service.history.materialize
        def limited(context,attempt,deadline):
            if blocked: other.execute(text("LOCK TABLE logs.aggregate_memberships IN ACCESS EXCLUSIVE MODE"))
            try: return materialize(context,attempt,time.monotonic()+(.15 if blocked else .000001))
            finally: other.rollback()
        monkeypatch.setattr(service.history,"materialize",limited)
        result=prepare(service,identity)
        assert result["reason"]=="resource_budget:preparation_seconds",result
        state=service.state(identity)
        assert state["detector"]["execution_state"]=="failed" and state["detector"]["failure_code"]=="resource_limit"
        assert state["detector"]["checkpoint"] is None and state["detector"]["consumed_index"] is None
        assert state["warmup"]["inputs"]==[] and state["warmup"]["status"]=="waiting"
        service.recover(identity)
        assert service.state(identity)["detector"]==state["detector"] and identity not in service.candidates()


@pytest.mark.parametrize("setup", ["overlap"], indirect=True)
@pytest.mark.parametrize("phase", ["readiness", "snapshot"])
@pytest.mark.parametrize("action", ["stop", "delete", "owner"])
def test_history_readiness_and_snapshot_are_fenced_before_write(setup, phase, action, monkeypatch):
    plan, _, _events, _worker = overlapping_history(setup)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Read fencing",
            config={"sample_size": 16, "output_after": 16, "trees": 20}, accelerated=True, step=1)
        freeze_attempt_clock(service, setup.cutoff)
        original = service.history.plan if phase == "readiness" else service.history.materialize
        raced = []
        def mutation(*args):
            result = original(*args)
            if not raced:
                with setup.database.session_factory() as other:
                    if action == "stop":
                        build_detector_service(other, setup.catalog).stop(identity)
                    elif action == "delete":
                        build_aggregate_service(other, setup.catalog).lifecycle(setup.card.id, "delete", UUID(int=1))
                    else:
                        # An independently committed revision invalidates the
                        # final owner check without releasing the test's lock.
                        from app.models import DetectorCoordinator
                        row = other.get(DetectorCoordinator, 1)
                        row.generation += 1
                        other.commit()
                raced.append(True)
            return result
        monkeypatch.setattr(service.history, "plan" if phase == "readiness" else "materialize", mutation)
        try:
            with pytest.raises(DetectionError):
                prepare(service, identity)
            state = stored(setup, identity)
            assert state["checkpoint"] is None and state["consumed_index"] is None
            with setup.database.session_factory() as read:
                attempt = read.scalar(select(DetectorWarmup).where(DetectorWarmup.detector_id == identity))
                assert attempt is None if phase == "readiness" else attempt.plan == plan and attempt.inputs == []
        finally:
            if action == "owner":
                from app.models import DetectorCoordinator
                with setup.database.session_factory.begin() as restore:
                    restore.get(DetectorCoordinator, 1).generation = OWNER.generation


@pytest.mark.parametrize("setup", ["overlap"], indirect=True)
@pytest.mark.parametrize("fault,reason", [
    ("late_attestation", "input_coverage_unavailable"),
    ("attestation_gap", "input_coverage_gap"),
    ("heartbeat", "consumer_coverage_gap"),
    ("backlog", "kafka_backlog_or_retention"),
    ("retention", "kafka_backlog_or_retention"),
    ("interrupted", "known_source_or_processing_stop"),
])
def test_history_saved_quality_is_not_promoted_by_optional_new_reports(setup,fault,reason):
    plan,_,_,_=overlapping_history(setup, fault=fault if fault in {"backlog","retention","interrupted"} else None)
    with setup.database.session_factory() as session:
        service=build_detector_service(session,setup.catalog)
        identity=service.create(setup.card.id,UUID(int=1),"Saved quality",config={"sample_size":16,"output_after":16,"trees":20},accelerated=True,step=1)
        result=prepare(service,identity)
        assert result["state"]==("waiting" if fault in {"backlog","retention","interrupted"} else "prepared"),result
        attempt=service.repository.attempt(result["attempt_id"])
        assert attempt["plan"]==plan
        if result["state"]=="waiting":
            assert attempt["inputs"]==[] and stored(setup,identity)["checkpoint"] is None
        else:
            assert all(i["completeness"]=="complete" for i in attempt["inputs"])


@pytest.mark.parametrize("setup", ["overlap"], indirect=True)
@pytest.mark.parametrize("mutation", ["delete", "add"])
def test_overlapping_batch_uses_one_snapshot_and_survives_event_delete(setup, mutation):
    from sqlalchemy import delete

    from app.models import AvailabilityMarker, ParsedLog
    plan, windows, events, _worker = overlapping_history(setup)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Overlap",
            config={"sample_size": 16, "output_after": 16, "trees": 20}, accelerated=True, step=1)
        freeze_attempt_clock(service, setup.cutoff)
        calculate = service.history.aggregates.overlap_statistics
        removed = []
        def delete_during_batch(*args):
            result = calculate(*args)
            if not removed:
                with setup.database.session_factory.begin() as deleting:
                    if mutation == "delete":
                        deleting.execute(delete(ParsedLog).where(ParsedLog.source_id == setup.source_id))
                    else:
                        when = windows[len(windows) // 2][0]
                        record_id = processed_record(deleting, setup, len(events), when)
                        event = ParsedLog(id=uuid4(), source_id=setup.source_id, connection_id=setup.connection_id, normalizer_version=1,
                            processed_record_id=record_id, normalizer_name="ECS", source_name="overlap",
                            connection_name="Synthetic", kafka_topic=str(setup.source_id), kafka_partition=0,
                            kafka_offset=len(events), deduplication_key=str(uuid4()), raw="synthetic",
                            ecs_data={"@timestamp": when.isoformat()}, normalization_status="complete",
                            normalization_diagnostics=[], fluent_bit_collected_at=when,
                            backend_received_at=when, backend_processed_at=when)
                        deleting.add(event)
                        deleting.add(AvailabilityMarker(source_identity=setup.source_id, kind="event",
                            event_id=event.id, availability_at=when))
                removed.append(True)
            return result
        service.history.aggregates.overlap_statistics = delete_during_batch
        result = prepare(service, identity)
        assert result["state"] == "prepared", result
        attempt = service.repository.attempt(result["attempt_id"])
        assert [item["features"][0]["value"] for item in attempt["inputs"]] == [
            sum(left <= when < right for when in events) for left, right in windows]
        assert service.warmup(identity) == {"state": "ready"}
        assert stored(setup, identity)["accelerated_used"]
        summary = service.state(identity)["warmup"]["summary"]
        assert summary["eligible_windows"] == plan["count"] == 71
        assert summary["eligible_coverage_seconds"] == plan["coverage_seconds"] == 80


def test_created_detector_requires_request_and_fenced_owner(setup):
    history(setup)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Explicit start")
        assert service.state(identity)["detector"]["execution_state"] == "created"
        for operation in (service.prepare_warmup, service.warmup, service.advance_one):
            with pytest.raises(DetectionError, match="execution_not_authorized"):
                operation(identity)
        unowned = compose_detector_service(session, setup.catalog)
        with pytest.raises(DetectionError, match="managed_worker_required"):
            unowned.prepare_warmup(identity)
        first = service.start(identity)["detector"]
        second = service.start(identity)["detector"]
        assert first["start_request_id"] == second["start_request_id"]
        assert first["revision"] == second["revision"]
        assert service.state(identity).get("warmup") is None
        service.stop(identity)
        before = service.state(identity)["detector"]
        assert service.stop(identity)["detector"] == before


@pytest.mark.parametrize("phase", ["prepare", "warmup", "advance"])
def test_manual_stop_fences_every_candidate_and_its_diagnostic(setup, phase):
    history(setup)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Stop race",
            config={"sample_size": 16, "output_after": 16, "trees": 20})
        freeze_attempt_clock(service, setup.cutoff)
        service.start(identity)
        service.begin_start(identity)
        if phase != "prepare":
            service.prepare_warmup(identity)
        if phase == "advance":
            service.warmup(identity)
            point(setup, 480, 1000)
            freeze_attempt_clock(service, setup.cutoff + timedelta(seconds=2))
        expected_context = service.state(identity)
        before = expected_context["detector"]
        def stop():
            with setup.database.session_factory() as other:
                build_detector_service(other, setup.catalog).stop(identity)
        if phase == "prepare":
            original = service.history.materialize
            def race(*args):
                result = original(*args)
                stop()
                return result
            service.history.materialize = race
            operation = service.prepare_warmup
        elif phase == "warmup":
            original = service.models.warmup
            def race(*args):
                result = original(*args)
                stop()
                return result
            service.models.warmup = race
            operation = service.warmup
        else:
            original = service.models.restore
            def race(*args):
                model = original(*args)
                process = model.process
                def process_race(values):
                    result = process(values)
                    stop()
                    return result
                model.process = process_race
                return model
            service.models.restore = race
            operation = service.advance_one
        with pytest.raises(StaleCandidate):
            operation(identity)
        after = service.state(identity)["detector"]
        assert after["execution_state"] == "stopped" and after["failure_code"] is None
        assert after["checkpoint"] == before["checkpoint"]
        assert after["consumed_index"] == before["consumed_index"]
        with pytest.raises(StaleCandidate):
            service.fail(identity, "model_process_failed", expected_context)
        assert service.state(identity)["detector"] == after


def test_failed_or_interrupted_initial_start_needs_manual_retry(setup):
    history(setup, constant=True)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Failed start",
            config={"sample_size": 16, "output_after": 16, "trees": 20})
        freeze_attempt_clock(service, setup.cutoff)
        prepare(service, identity)
        service.warmup(identity)
        failed = service.state(identity)["detector"]
        assert failed["execution_state"] == "failed" and failed["failure_code"] == "history_quality"
        assert failed["last_failure"]["code"] == "history_quality"
        assert failed["last_failure"]["start_request_id"] == str(failed["start_request_id"])
        service.recover(identity)
        assert service.state(identity)["detector"] == failed
        with pytest.raises(DetectionError, match="execution_not_authorized"):
            service.prepare_warmup(identity)
        service.start(identity)
        service.begin_start(identity)
        request = service.state(identity)["detector"]["start_request_id"]
        assert service.state(identity)["detector"]["last_failure"] == failed["last_failure"]
        service.recover(identity)
        state = service.state(identity)["detector"]
        assert state["execution_state"] == "failed" and state["failure_code"] == "start_interrupted"
        assert state["start_request_id"] == request
        service.start(identity)
        assert service.state(identity)["detector"]["start_request_id"] != request


@pytest.mark.parametrize("late_materialization", [False, True])
@pytest.mark.parametrize("reason", ["crash_recovery", "manual_resume"])
def test_short_recovery_and_manual_backlog_keep_late_until_materialization(setup, reason, late_materialization):
    history(setup)
    identity = prepared(setup)
    # close deadline cutoff+2 has passed; the next deadline cutoff+3 has not.
    recovered_at = setup.cutoff + timedelta(seconds=2.1)
    if not late_materialization:
        point(setup, 480, 1000)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        freeze_attempt_clock(service, recovered_at)
        if reason == "manual_resume":
            service.stop(identity)
            service.start(identity)
            service.begin_start(identity)
            service.resume(identity)
        else:
            service.recover(identity)
            service.recover(identity)  # repeated takeover must retain the range
        ranges = service.state(identity)["recovery"]
        assert ranges and all(r["first_index"] == r["last_index"] == 480 for r in ranges)
        assert all(r["reason"] == reason for r in ranges)
        before = service.state(identity)["detector"]
        if late_materialization:
            assert service.advance_one(identity)["state"] == "waiting"
            point(setup, 480, 1000)
        assert service.advance_one(identity)["state"] == "evaluated"
        after = service.state(identity)["detector"]
        assert after["active_attempt_id"] == before["active_attempt_id"]
        assert after["checkpoint"]["body"]["updates"] == before["checkpoint"]["body"]["updates"] + 1
    with setup.database.session_factory() as session:
        finding = session.scalar(select(Finding).where(Finding.detector_id == identity))
        assert finding.late and finding.data["recovery"][0]["reason"] == reason


def test_standby_takeover_fences_old_candidate_and_preserves_other_states(setup, coordinator):
    global OWNER
    history(setup)
    identity = prepared(setup)
    point(setup, 480, 1000)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        expected = service.repository.read_context(identity)
        service.uow.rollback()
        standby = DetectorSessionLock(os.environ["TEST_DATABASE_URL"])
        assert standby.acquire() is None
        old = OWNER
        coordinator.close()
        pid = coordinator.acquire()
        with setup.database.session_factory.begin() as other:
            OWNER = SqlAlchemyDetectorRepository(other).acquire_owner(uuid4(), pid)
        assert OWNER.generation > old.generation
        with pytest.raises(StaleCandidate, match="ownership_lost"):
            service._lock(expected)
        service.uow.rollback()
        new = build_detector_service(session, setup.catalog)
        new.recover(identity)
        assert new.advance_one(identity)["state"] == "evaluated"
        new.stop(identity)
        stopped = new.state(identity)["detector"]
        new.recover(identity)
        assert new.state(identity)["detector"] == stopped


def test_physical_owner_loss_after_flush_rejects_commit(setup, coordinator):
    global OWNER
    from sqlalchemy.exc import DBAPIError
    history(setup)
    identity = prepared(setup)
    point(setup, 480, 1000)
    before = stored(setup, identity)
    try:
        with setup.database.session_factory() as session:
            service = build_detector_service(session, setup.catalog)
            commit = service.uow.commit
            def lose_connection_before_commit():
                coordinator.close()
                commit()
            service.uow.commit = lose_connection_before_commit
            with pytest.raises(DBAPIError, match="ownership lost"):
                service.advance_one(identity)
        assert stored(setup, identity)["checkpoint"] == before["checkpoint"]
        assert stored(setup, identity)["consumed_index"] is None
    finally:
        pid = coordinator.acquire()
        with setup.database.session_factory.begin() as session:
            OWNER = SqlAlchemyDetectorRepository(session).acquire_owner(uuid4(), pid)


def test_manual_resume_consumes_entire_backlog_without_rewarming(setup):
    history(setup)
    identity = prepared(setup)
    point(setup, 480, 2)
    advance(setup, identity)
    before = stored(setup, identity)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        service.stop(identity)
        point(setup, 481, 0, "unknown")
        point(setup, 482, None)
        point(setup, 483, 1000)
        service.start(identity)
        service.begin_start(identity)
        with pytest.raises(DetectionError, match="execution_not_authorized"):
            service.advance_one(identity)
        service.resume(identity)
        assert [service.advance_one(identity)["state"] for _ in range(3)] == ["skipped", "skipped", "evaluated"]
        after = service.state(identity)["detector"]
        assert after["consumed_index"] == after["evaluated_index"] == 483
        assert after["active_attempt_id"] == before["active_attempt_id"]
        assert after["checkpoint"]["body"]["updates"] == before["checkpoint"]["body"]["updates"] + 1


def test_added_history_never_retries_failed_start_without_command(setup):
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Missing history",
            config={"sample_size": 16, "output_after": 16, "trees": 20})
        freeze_attempt_clock(service, setup.anchor + timedelta(seconds=1))
        result = prepare(service, identity)
        assert result["state"] == "waiting"
        failed = service.state(identity)["detector"]
        assert failed["execution_state"] == "failed"
        history(setup)
        service.recover(identity)
        assert service.state(identity)["detector"] == failed
        assert identity not in service.candidates()
        freeze_attempt_clock(service, setup.cutoff)
        assert prepare(service, identity)["state"] == "prepared"
        assert service.warmup(identity)["state"] == "ready"


@pytest.mark.parametrize("failure", ["operator_cost", "total_memory", "statement_timeout", "invalid_rule"])
def test_confirmed_history_failure_finishes_committed_attempt(setup, monkeypatch, failure):
    from app.aggregation.rules import RuleError

    history(setup)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Budget failure",
            config={"sample_size": 16, "output_after": 16, "trees": 20})
        freeze_attempt_clock(service, setup.cutoff)
        def fail_materialization(*_):
            if failure == "statement_timeout":
                # Real PostgreSQL abort: the failure must be saved in a new,
                # short transaction after rolling back the failed snapshot.
                session.execute(select(func.set_config("statement_timeout", "20", True)))
                session.execute(select(func.pg_sleep(.1)))
            code = "invalid_rule" if failure == "invalid_rule" else "resource_budget_exceeded"
            raise RuleError(code, failure, "arbitrary private diagnostic")
        with monkeypatch.context() as controlled:
            controlled.setattr(service.history, "materialize", fail_materialization)
            assert prepare(service, identity)["state"] == "waiting"
        state = service.state(identity)
        failed = state["detector"]
        factor = "preparation_seconds" if failure == "statement_timeout" else failure
        assert failed["execution_state"] == "failed"
        assert failed["failure_code"] == ("history_quality" if failure == "invalid_rule" else "resource_limit")
        assert failed["last_failure"]["details"] == ({} if failure == "invalid_rule" else {"factor": factor})
        assert "private" not in str(state)
        assert failed["checkpoint"] is None and failed["consumed_index"] is None
        assert state["warmup"]["status"] == "waiting" and state["warmup"]["inputs"] == []
        service.recover(identity)
        assert service.state(identity)["detector"] == failed
        assert identity not in service.candidates()
        old_attempt = failed["active_attempt_id"]
        assert prepare(service, identity)["state"] == "prepared"
        assert service.state(identity)["detector"]["active_attempt_id"] != old_attempt
        assert service.warmup(identity)["state"] == "ready"


def test_bounded_selection_reaches_models_after_first_page(setup):
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identities = []
        for _ in range(5):
            identity = service.create(setup.card.id, UUID(int=1), "Independent")
            service.start(identity)
            identities.append(identity)
        observed, after = [], None
        while True:
            page = service.candidates(after, 2)
            if not page:
                break
            observed.extend(page)
            after = page[-1]
        assert set(identities) <= set(observed)
        assert observed == sorted(set(observed))


def test_archive_between_request_and_acceptance_fails_once_with_reason(setup):
    from threading import Event

    from app.services.implementations.detector_worker import DetectorWorker
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Archive race")
        service.start(identity)
        build_aggregate_service(session, setup.catalog).lifecycle(setup.card.id, "archive", UUID(int=1))
    worker = DetectorWorker(setup.database.session_factory, build_worker_detector_service, setup.catalog, Event(), None)
    worker.owner = OWNER
    assert worker._step(identity)["state"] == "failed"
    state = stored(setup, identity)
    assert state["execution_state"] == "failed" and state["failure_code"] == "aggregate_inactive"
    assert state["active_attempt_id"] is None
