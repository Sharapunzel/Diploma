"""Real Linux/JVM + PostgreSQL detector transaction and lifecycle regressions."""

import copy
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.engine import make_url

from app.aggregate_composition import build_aggregate_service
from app.db import Database
from app.detection.aws_rcf import AwsModelFactory
from app.detection.contracts import DetectionError, StaleCandidate, digest
from app.detector_cli import build_detector_service
from app.ecs import PackagedEcsCatalog
from app.models import (
    Detector,
    DetectorOutcome,
    DetectorWarmup,
    Finding,
    KafkaConnection,
    Source,
    WorkingWindowPoint,
)
from app.schemas.aggregates import AggregatePreviewRequest, CreateAggregateRequest


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
        window_start_utc=anchor, time_basis="backend_processed_at_historical")
    with database.session_factory() as session:
        aggregate = build_aggregate_service(session, catalog)
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
            completeness_reason="synthetic_fixture", evidence_refs={},
            features=[{"key": "n", "value": value, "reason": None, "valid_count": 1, "invalid_count": 0}])
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
        result = service.prepare_warmup(detector)
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
        with pytest.raises(DetectionError, match="aggregate_inactive"):
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
        if kind == "checksum": state["body"]["updates"] += 1
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
    assert advance(setup, identity)["state"] == "waiting"


def test_constant_history_waits(setup):
    history(setup, constant=True)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Constant",
                                  config={"sample_size": 16, "output_after": 16})
        freeze_attempt_clock(service, setup.cutoff)
        assert service.prepare_warmup(identity)["state"] == "prepared"
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


def test_unknown_hole_never_jumps_cursor(setup):
    history(setup)
    identity = prepared(setup)
    point(setup, 481, 1000)
    assert advance(setup, identity)["reason"] == "working_point_gap"
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
        service.prepare_warmup(identity)
        original_warmup = service.models.warmup
        def compete(*args):
            checkpoint = original_warmup(*args)
            with setup.database.session_factory() as competing:
                other = build_detector_service(competing, setup.catalog)
                freeze_attempt_clock(other, setup.cutoff)
                assert other.prepare_warmup(identity)["state"] == "prepared"
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


def test_database_timezone_preserves_warmup_and_decisions(setup):
    history(setup)
    first = prepared(setup, timezone="Asia/Tokyo")
    second = prepared(setup, timezone="America/New_York")
    assert stored(setup, first)["checkpoint"] == stored(setup, second)["checkpoint"]
    point(setup, 480, 1000)
    results = []
    for identity, timezone in [(first, "Asia/Tokyo"), (second, "America/New_York")]:
        with setup.database.session_factory() as session:
            session.execute(select(func.set_config("TimeZone", timezone, False)))
            results.append(build_detector_service(session, setup.catalog).advance_one(identity))
    assert results[0] == results[1]


@pytest.mark.parametrize("phase", ["prepare", "warmup"])
def test_delete_cancels_unconfirmed_warmup(setup, phase):
    history(setup)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Deletion race",
            config={"sample_size": 16, "output_after": 16, "trees": 20})
        freeze_attempt_clock(service, setup.cutoff)
        if phase == "warmup":
            service.prepare_warmup(identity)
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
        with pytest.raises(DetectionError, match="aggregate_inactive"):
            (service.warmup if phase == "warmup" else service.prepare_warmup)(identity)
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


def test_legacy_or_unknown_history_waits_without_training(setup):
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Unknown history",
            config={"sample_size": 16, "output_after": 16, "trees": 20})
        freeze_attempt_clock(service, setup.cutoff)
        service.history.aggregates.progress = lambda: SimpleNamespace(
            availability_verified_from_at=None, legacy_cutover_at=setup.cutoff)
        result = service.prepare_warmup(identity)
        assert result["state"] == "waiting" and result["reason"] == "legacy_coverage_unproven"
        assert stored(setup, identity)["checkpoint"] is None


def test_preparation_locks_source_before_detector(setup):
    history(setup)
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Lock order",
            config={"sample_size": 16, "output_after": 16, "trees": 20})
        freeze_attempt_clock(service, setup.cutoff)
        trace = []
        aggregate_context = service.repository.aggregate_context
        lock_source = service.windows.lock_source
        row = service.repository._row
        def aggregate(*args, **kwargs):
            result = aggregate_context(*args, **kwargs)
            if kwargs.get("lock", args[1] if len(args) > 1 else False):
                trace.append("schedule")
            return result
        def source(identity):
            result = lock_source(identity)
            trace.append("source")
            return result
        def detector(model, identity, lock=False):
            result = row(model, identity, lock)
            if model is Detector and lock:
                trace.append("detector")
            return result
        service.repository.aggregate_context = aggregate
        service.windows.lock_source = source
        service.repository._row = detector
        assert service.prepare_warmup(identity)["state"] == "prepared"
        assert trace[0] == "schedule" and trace.index("source") < trace.index("detector")


def test_source_lock_wait_is_bounded_and_does_not_commit_attempt(setup):
    import time

    from sqlalchemy.exc import DBAPIError
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Bounded lock",
            config={"sample_size": 16, "output_after": 16, "trees": 20})
    with setup.database.session_factory() as blocking, setup.database.session_factory() as session:
        blocking.scalar(select(Source).where(Source.id == setup.source_id).with_for_update())
        service = build_detector_service(session, setup.catalog)
        freeze_attempt_clock(service, setup.cutoff)
        started = time.monotonic()
        with pytest.raises(DBAPIError):
            service.prepare_warmup(identity)
        assert 4 <= time.monotonic() - started < 12
    state = stored(setup, identity)
    assert state["revision"] == 0 and state["active_attempt_id"] is None


@pytest.mark.parametrize("phase", ["fit", "collision", "convert"])
def test_preparation_deadline_stops_numeric_work_without_committing_batch(setup, monkeypatch, phase):
    from app.detection import numeric
    from app.services.implementations import detector_history

    originals = [[2**60 + i] for i in range(512)]
    with setup.database.session_factory.begin() as session:
        for i, values in enumerate(originals, start=48):
            start = setup.anchor + timedelta(seconds=i)
            end = start + timedelta(seconds=1)
            session.add(WorkingWindowPoint(id=uuid4(), schedule_id=setup.card.schedule_id,
                window_index=i, start_utc=start, end_utc=end, scheduled_at=end,
                actual_started_at=end, actual_completed_at=end, event_count=0,
                event_time_min=None, event_time_max=None, completeness="complete",
                completeness_reason="synthetic_fixture", evidence_refs={},
                features=[{"key": "n", "value": values[0], "reason": None,
                           "valid_count": 1, "invalid_count": 0}]))
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
            result = service.prepare_warmup(identity)
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
def test_real_aggregate_and_saved_signed_evidence(setup):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    from app.models import (
        AggregateSchedule,
        AvailabilityMarker,
        InputProbe,
        KafkaCheckpoint,
        ParsedLog,
    )
    from app.repositories.sqlalchemy.input_attestations import SqlAlchemyInputAttestationRepository
    from app.services.implementations.input_attestations import (
        canonical_payload,
        ingest_attestation,
    )
    from app.services.implementations.working_windows import WindowNotReady

    history(setup)
    identity = prepared(setup)
    start, end = setup.cutoff, setup.cutoff + timedelta(seconds=1)
    probe_id, generation, worker = uuid4(), uuid4(), uuid4()
    private = Ed25519PrivateKey.generate()
    count = 0 if setup.kind == "empty" else 1 if setup.kind == "bytes" else 100
    payload = {"id": str(uuid4()), "probe_id": str(probe_id), "source_id": str(setup.source_id),
        "input_generation": str(generation), "sequence": 1, "start": start.isoformat(),
        "end": end.isoformat(), "state": "covered", "topic_identity": "synthetic",
        "start_positions": {"0": 0}, "end_positions": {"0": count}}
    occurred = start - timedelta(days=20)
    with setup.database.session_factory.begin() as session:
        session.get(AggregateSchedule, setup.card.schedule_id).next_index = 480
        session.add(InputProbe(id=probe_id, source_identity=setup.source_id, input_generation=generation,
            public_key=private.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)))
        for i in range(count):
            record_id = processed_record(session, setup, i, start)
            event = ParsedLog(id=uuid4(), source_id=setup.source_id, normalizer_version=1,
                processed_record_id=record_id,
                normalizer_name="Synthetic ECS", source_name=setup.kind, connection_name="Synthetic",
                kafka_topic=str(setup.source_id), kafka_partition=0, kafka_offset=i, deduplication_key=str(uuid4()),
                raw="synthetic", ecs_data=({"@timestamp": occurred.isoformat(),
                    "network": {"bytes": 1000}, "source": {"ip": "192.0.2.1"},
                    "event": {"category": ["network"]}} if setup.kind == "bytes" else
                    {"@timestamp": occurred.isoformat(), "user": {"name": "alice"},
                     "event": {"category": ["authentication"], "outcome": "failure"}}),
                normalization_status="complete", normalization_diagnostics=[],
                fluent_bit_collected_at=start, backend_received_at=start, backend_processed_at=start)
            session.add(event)
            session.add(AvailabilityMarker(source_identity=setup.source_id, kind="event",
                                           event_id=event.id, availability_at=start))
    with setup.database.session_factory.begin() as session:
        ingest_attestation(SqlAlchemyInputAttestationRepository(session), payload,
                           private.sign(canonical_payload(payload)))
        for at in [start - timedelta(seconds=1), end + timedelta(seconds=1)]:
            position = 0 if at < start else count
            session.add(KafkaCheckpoint(source_identity=setup.source_id, topic_identity="synthetic",
                worker_instance=worker, observed_at=at, partitions={"0": {"low": 0, "high": position, "committed": position}}))
    def aggregate(session):
        result = build_aggregate_service(session, setup.catalog)
        result.capture = SimpleNamespace(status=lambda: SimpleNamespace(ready=True))
        return result
    with setup.database.session_factory() as session, pytest.raises(WindowNotReady, match="barrier_not_materialized"):
        aggregate(session).advance(setup.card.id)
    with setup.database.session_factory.begin() as session:
        for marker in session.scalars(select(AvailabilityMarker).where(
                AvailabilityMarker.source_identity == setup.source_id, AvailabilityMarker.kind == "barrier")):
            marker.availability_at = datetime.now(UTC)
    with setup.database.session_factory.begin() as session:
        session.add(KafkaCheckpoint(source_identity=setup.source_id, topic_identity="synthetic",
            worker_instance=worker, partitions={"0": {"low": 0, "high": count, "committed": count}}))
    with setup.database.session_factory() as session:
        working = aggregate(session).advance(setup.card.id)
        assert working.completeness == "complete" and working.evidence_refs["attestations"]
        assert working.features[0]["value"] == (0 if setup.kind == "empty" else 1000 if setup.kind == "bytes" else 100)
    result = advance(setup, identity)
    assert result["state"] == "evaluated"
    if setup.kind == "empty":
        assert stored(setup, identity)["checkpoint"]["body"]["updates"] == 33
        return
    assert result["finding"]
    assert result["finding"]["source_id"] == str(setup.source_id)
    assert result["finding"]["event_time_min"] == occurred.isoformat()
    assert result["finding"]["start"] == start.isoformat()


@pytest.mark.parametrize("setup", ["overlap"], indirect=True)
def test_overlapping_batch_uses_one_snapshot_and_survives_event_delete(setup):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from sqlalchemy import delete

    from app.detection.history import plan_windows, warmup_plan
    from app.models import (
        AvailabilityMarker,
        InputAttestation,
        InputProbe,
        KafkaCheckpoint,
        ParsedLog,
        ReplicationProgress,
    )
    from app.services.implementations.input_attestations import canonical_payload

    plan = warmup_plan(setup.anchor, 10, setup.cutoff, 32, 1)
    windows = list(plan_windows(plan))
    start, end = windows[0][0], windows[-1][1]
    probe_id, generation, worker, report_id = uuid4(), uuid4(), uuid4(), uuid4()
    private = Ed25519PrivateKey.generate()
    count = sum(1 + second % 3 for second in range(int((end-start).total_seconds())))
    report = {"id": str(report_id), "probe_id": str(probe_id), "source_id": str(setup.source_id),
        "input_generation": str(generation), "sequence": 1, "start": start.isoformat(),
        "end": end.isoformat(), "state": "covered", "topic_identity": "synthetic",
        "start_positions": {"0": 0}, "end_positions": {"0": count}}
    at = datetime.now(UTC)
    events = []
    with setup.database.session_factory.begin() as session:
        progress = session.get(ReplicationProgress, 1)
        if progress is None:
            session.add(ReplicationProgress(id=1, durable_lsn=0,
                availability_verified_from_at=setup.anchor))
        else:
            progress.availability_verified_from_at = min(progress.availability_verified_from_at or setup.anchor, setup.anchor)
        session.add(AvailabilityMarker(source_identity=setup.source_id, kind="barrier",
            created_at=at - timedelta(seconds=1), availability_at=at - timedelta(seconds=1)))
        session.add(InputProbe(id=probe_id, source_identity=setup.source_id, input_generation=generation,
            public_key=private.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)))
        session.flush()
        session.add(InputAttestation(id=report_id, probe_id=probe_id, source_identity=setup.source_id,
            input_generation=generation, sequence=1, start_utc=start, end_utc=end, state="covered",
            topic_identity="synthetic", start_positions={"0": 0}, end_positions={"0": count},
            signed_payload=report, signature=private.sign(canonical_payload(report)),
            received_at=at - timedelta(seconds=2)))
        for second in range(int((end-start).total_seconds())):
            when = start + timedelta(seconds=second)
            for _ in range(1 + second % 3):
                index = len(events)
                record_id = processed_record(session, setup, index, when)
                event = ParsedLog(id=uuid4(), source_id=setup.source_id, normalizer_version=1,
                    processed_record_id=record_id,
                    normalizer_name="ECS", source_name="overlap", connection_name="Synthetic",
                    kafka_topic=str(setup.source_id), kafka_partition=0, kafka_offset=index,
                    deduplication_key=str(uuid4()), raw="synthetic",
                    ecs_data={"@timestamp": when.isoformat()}, normalization_status="complete",
                    normalization_diagnostics=[], fluent_bit_collected_at=when,
                    backend_received_at=when, backend_processed_at=when)
                events.append(when)
                session.add(event)
                session.add(AvailabilityMarker(source_identity=setup.source_id, kind="event",
                    event_id=event.id, availability_at=when))
        when = start - timedelta(seconds=1)
        while when <= end + timedelta(seconds=5):
            position = sum(event_time <= when for event_time in events)
            session.add(KafkaCheckpoint(source_identity=setup.source_id, topic_identity="synthetic",
                worker_instance=worker, observed_at=when, partitions={"0": {"low": 0, "high": position, "committed": position}}))
            when += timedelta(seconds=5)
        session.add(KafkaCheckpoint(source_identity=setup.source_id, topic_identity="synthetic",
            worker_instance=worker, partitions={"0": {"low": 0, "high": count, "committed": count}}))
    with setup.database.session_factory() as session:
        service = build_detector_service(session, setup.catalog)
        identity = service.create(setup.card.id, UUID(int=1), "Overlap",
            config={"sample_size": 16, "output_after": 16, "trees": 20}, accelerated=True, step=1)
        freeze_attempt_clock(service, setup.cutoff)
        calculate = service.history.aggregates.calculate
        removed = []
        def delete_during_batch(*args):
            result = calculate(*args)
            if not removed:
                with setup.database.session_factory.begin() as deleting:
                    deleting.execute(delete(ParsedLog).where(ParsedLog.source_id == setup.source_id))
                removed.append(True)
            return result
        service.history.aggregates.calculate = delete_during_batch
        result = service.prepare_warmup(identity)
        assert result["state"] == "prepared", result
        attempt = service.repository.attempt(result["attempt_id"])
        assert [item["features"][0]["value"] for item in attempt["inputs"]] == [
            sum(left <= when < right for when in events) for left, right in windows]
        assert service.warmup(identity) == {"state": "ready"}
        assert stored(setup, identity)["accelerated_used"]
        summary = service.state(identity)["warmup"]["summary"]
        assert summary["eligible_windows"] == plan["count"] == 71
        assert summary["eligible_coverage_seconds"] == plan["coverage_seconds"] == 80
