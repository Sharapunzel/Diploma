"""Synthetic processor/probe for detector browser profiles, executed in ML container.

Uses actual commits, signed interval evidence and WAL capture. No fabricated
availability, saved complete vectors or capture-boundary changes. The owning
profile removes the container/database and checks its inventory in finally.
"""
import json
import os
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

STATUS = Path(os.environ.get("BROWSER_FIXTURE_STATUS", "/tmp/browser-detector-status.json"))
capture_at = None


class FixtureCaptureTimeout(RuntimeError):
    """Real source barriers stopped advancing within the fixture's budget."""


def publish_status(state, phase, sequence=0, error=None, sqlstate=None):
    if not isinstance(sqlstate, str) or len(sqlstate) != 5 or not sqlstate.isascii() or not sqlstate.isalnum():
        sqlstate = None
    payload = {"state": state, "phase": phase, "sequence": sequence,
               "pid": os.getpid(), "updated": time.monotonic(), "error": error,
               "capture_at": capture_at, "sqlstate": sqlstate}
    temporary = STATUS.with_suffix(".next")
    temporary.write_text(json.dumps(payload), encoding="utf-8")
    temporary.replace(STATUS)


def read_status(path=STATUS):
    """Only bounded lifecycle facts, never logs, credentials or event payloads."""
    try:
        status = json.loads(path.read_text(encoding="utf-8"))
        pid = status["pid"]
        if not isinstance(pid, int) or pid < 1:
            raise ValueError("invalid pid")
        if status["state"] != "running":
            return {"ready": False, "reason": "fixture_" + status["state"],
                    "phase": status["phase"], "error": status["error"],
                    "sqlstate": status.get("sqlstate")}
        os.kill(pid, 0)
        if not 0 <= time.monotonic() - status["updated"] <= 10:
            return {"ready": False, "reason": "fixture_progress_stale", "phase": status["phase"],
                    "sequence": status["sequence"]}
        if status.get("capture_at") is None:
            return {"ready": False, "reason": "fixture_capture_starting", "phase": status["phase"],
                    "sequence": status["sequence"]}
        if status.get("capture_at") is not None and not 0 <= time.monotonic() - status["capture_at"] <= 10:
            return {"ready": False, "reason": "fixture_capture_stale", "phase": status["phase"]}
        return {"ready": True, "reason": "ready", "phase": status["phase"],
                "sequence": status["sequence"]}
    except ProcessLookupError:
        return {"ready": False, "reason": "fixture_process_stopped"}
    except (OSError, ValueError, KeyError, TypeError):
        return {"ready": False, "reason": "fixture_status_unavailable"}


def checked_status():
    # Standard-library-only probe: no repeated ORM/app import or new DB pool.
    # The writer publishes capture_at only after its real event and barrier
    # markers have actually been materialized by the decoder.
    return read_status()


def bounded_database_url(raw):
    """Bounds only this synthetic writer's connections, never the database."""
    from sqlalchemy.engine import make_url

    url = make_url(raw)
    options = " ".join(filter(None, [url.query.get("options"),
        "-c lock_timeout=1000 -c statement_timeout=3000"]))
    return url.update_query_dict({"connect_timeout": "5", "options": options}).render_as_string(hide_password=False)


@contextmanager
def working_closure_gate(database):
    """Publish fixture evidence without blocking event-time snapshot readers.

    The real writer/probe commits and WAL still run independently. Historical
    readiness must succeed without this gate (covered by service DB tests).
    Event-time quality reads a received snapshot, not a later signed report.
    ACCESS EXCLUSIVE formerly blocked those RR reads and itself timed out when
    a live overlap warmup retained ACCESS SHARE. SHARE ROW EXCLUSIVE keeps
    evidence writers serialized while allowing genuine snapshot reads.
    The transaction releases the table lock on success and every exception;
    no lifecycle/schedule/detector lock is acquired here.
    """
    from sqlalchemy import text

    with database.session_factory.begin() as proof:
        proof.execute(text("SET LOCAL lock_timeout='1000ms'"))
        proof.execute(text("SET LOCAL statement_timeout='3000ms'"))
        proof.execute(text("LOCK TABLE logs.source_interruptions IN SHARE ROW EXCLUSIVE MODE"))
        yield proof


def prepare_auto_history(database, catalog, actor, connection_id):
    """Seed only the live auto scenario, through the real atomic writer."""
    from app.aggregate_composition import build_aggregate_service
    from app.models import AvailabilityMarker, ParsedLog, ProcessedKafkaRecord, Source
    from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
    from app.schemas.aggregates import AggregatePreviewRequest, CreateAggregateRequest
    from sqlalchemy import select

    identity, duration, windows = uuid4(), 2, 520
    bootstrap = datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=60)
    anchor = bootstrap - timedelta(seconds=duration * windows)
    with database.session_factory.begin() as session:
        session.add(Source(id=identity, connection_id=connection_id,
            name="История автоматической очистки браузера", topic_name=str(identity),
            kafka_topic_identity=str(identity), is_enabled=True))
    offset = 0
    for start in range(0, windows, 40):
        with database.session_factory.begin() as session:
            source = session.get(Source, identity)
            writer = SqlAlchemyProcessingRepository(session)
            for index in range(start, min(start + 40, windows)):
                timestamp = anchor + timedelta(seconds=index * duration)
                for _ in range(1 + index % 3):
                    now, record_id = datetime.now(UTC), uuid4()
                    record = ProcessedKafkaRecord(id=record_id, connection_id=connection_id,
                        connection_identity=connection_id, source_id=identity,
                        kafka_topic_identity=str(identity), kafka_topic=str(identity),
                        kafka_partition=0, kafka_offset=offset, result_status="complete",
                        backend_received_at=now, backend_processed_at=now)
                    event = ParsedLog(source_id=identity, connection_id=connection_id,
                        normalizer_version=1, normalizer_name="Synthetic auto history",
                        source_name=source.name, connection_name="Synthetic input", kafka_topic=str(identity),
                        kafka_partition=0, kafka_offset=offset, deduplication_key=str(record_id),
                        raw="synthetic auto history", ecs_data={"@timestamp": timestamp.isoformat()},
                        fluent_bit_collected_at=now, backend_received_at=now, backend_processed_at=now)
                    assert writer.store_event(record, event)
                    offset += 1
        publish_status("running", "auto_history_batch_committed", min(start + 40, windows))
        deadline = time.monotonic() + 2
        with database.session_factory() as session:
            while session.scalar(select(AvailabilityMarker.id).where(
                    AvailabilityMarker.source_identity == identity,
                    AvailabilityMarker.availability_at.is_(None)).limit(1)):
                assert time.monotonic() < deadline, "Auto history markers did not materialize"
                session.rollback()
                time.sleep(.01)
        publish_status("running", "auto_history_batch_materialized", min(start + 40, windows))
    with database.session_factory() as session:
        service = build_aggregate_service(session, catalog)
        # This is the same scoped logical clock composed for this test target
        # in both real workers. It fixes a contiguous history/online boundary;
        # SQL, writer commits, classification and budgets remain real.
        assert datetime.now(UTC) < bootstrap, "Auto seed exceeded its existing 60s startup budget"
        physical_now = service.repository.now
        add_state = service.repository.add_retention_state
        service.repository.add_retention_state = lambda aggregate, _now: add_state(aggregate, physical_now())
        service.repository.now = lambda: bootstrap
        body = AggregatePreviewRequest(source_id=identity, aggregate_rule={"features": [
            {"key": "events", "name": "Число событий", "op": "count"}]},
            schedule={"boundary": anchor, "timezone": "UTC", "duration_seconds": duration},
            window_start_utc=anchor, time_basis="event_time")
        checked = service.validate(body)
        aggregate = service.create(CreateAggregateRequest(**body.model_dump(),
            name="Автоматическая история браузера", rule_hash=checked.rule_hash), actor, str(uuid4()))
    publish_status("running", "auto_aggregate_created")
    return {"aggregate": str(aggregate.id), "source": str(identity), "anchor": anchor.isoformat(),
        "seed_windows": windows, "offset": offset, "clock_at": bootstrap.isoformat()}


def run():
    global capture_at
    # These milestones follow completed work. There is no timer heartbeat that
    # could keep a stalled import/setup healthy; every phase retains the 10s
    # progress bound and the profile retains its overall 60s preparation budget.
    from sqlalchemy import func, select, text
    from sqlalchemy.engine import make_url
    publish_status("running", "sqlalchemy_imported")
    from app.adapters.wal_bootstrap import read_setting
    from app.db import Database
    publish_status("running", "database_components_imported")
    from app.models import (
        AvailabilityMarker,
        InputProbe,
        KafkaCheckpoint,
        KafkaConnection,
        ParsedLog,
        ProcessedKafkaRecord,
        Source,
        User,
    )
    publish_status("running", "models_imported")
    from app.aggregate_composition import build_aggregate_service
    publish_status("running", "aggregate_services_imported")
    from app.ecs import PackagedEcsCatalog
    from app.repositories.sqlalchemy.input_attestations import (
        SqlAlchemyInputAttestationRepository,
    )
    from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
    from app.schemas.aggregates import AggregatePreviewRequest, CreateAggregateRequest
    from app.services.implementations.input_attestations import (
        canonical_payload,
        ingest_attestation,
    )
    publish_status("running", "processing_components_imported")
    from app.core.security import hash_password
    publish_status("running", "security_components_imported")
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    publish_status("running", "crypto_components_imported")

    url = bounded_database_url(read_setting("DATABASE_URL"))
    assert make_url(url).database == "diploma_test"
    db = Database(url)
    publish_status("running", "database_configured")
    source_id, connection_id, actor, probe_id, worker = (uuid4() for _ in range(5))
    private = Ed25519PrivateKey.generate()
    admin_hash = hash_password(os.environ["BROWSER_PASSWORD"])
    publish_status("running", "admin_password_hashed")
    guest_hash = hash_password(os.environ["BROWSER_PASSWORD"])
    publish_status("running", "guest_password_hashed")
    with db.session_factory.begin() as s:
        s.add(User(id=actor, username=os.environ["BROWSER_USERNAME"], password_hash=admin_hash, display_name="Browser detector admin", role_id=UUID("00000000-0000-4000-8000-000000000001")))
        s.add(User(username=os.environ["BROWSER_GUEST"], password_hash=guest_hash, display_name="Browser guest", role_id=UUID("00000000-0000-4000-8000-000000000002")))
        s.add(KafkaConnection(id=connection_id, name=str(connection_id), bootstrap_servers=["kafka:9092"], cluster_identity=str(connection_id)))
        s.flush()
        s.add(Source(id=source_id, connection_id=connection_id, name="Синтетический источник браузерной проверки", topic_name=str(source_id), kafka_topic_identity=str(source_id), is_enabled=True))
        s.flush()
        s.add(InputProbe(id=probe_id, source_identity=source_id, input_generation=uuid4(), public_key=private.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)))
    publish_status("running", "source_committed")
    catalog = PackagedEcsCatalog.load()
    publish_status("running", "catalog_loaded")
    auto_history = prepare_auto_history(db, catalog, actor, connection_id) if os.environ.get("BROWSER_AUTO_HISTORY") == "1" else None
    anchor = datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=2)
    with db.session_factory() as s:
        service = build_aggregate_service(s, catalog)
        body = AggregatePreviewRequest(source_id=source_id, aggregate_rule={"features":[{"key":"events","name":"Число событий","op":"count"}]}, schedule={"boundary":anchor,"timezone":"UTC","duration_seconds":2}, window_start_utc=anchor-timedelta(seconds=2), time_basis="event_time")
        # Creation needs a completed preview window. Anchor may precede current
        # time; wait for the first real interval rather than moving app clocks.
    publish_status("running", "initial_rule_prepared")
    while datetime.now(UTC) < anchor:
        time.sleep(.02)
    offset, sequence = 0, 0
    aggregate_id = None
    deadline = time.monotonic() + 1800
    try:
        while time.monotonic() < deadline:
            publish_status("running", "writing", sequence)
            start = anchor + timedelta(seconds=sequence)
            end = start + timedelta(seconds=1)
            while datetime.now(UTC) < start:
                time.sleep(.01)
            first = offset
            with db.session_factory.begin() as s:
                source = s.scalar(select(Source).where(Source.id == source_id).with_for_update(read=True))
                for _ in range(1 + sequence % 3):
                    at, rid = datetime.now(UTC), uuid4()
                    record = ProcessedKafkaRecord(id=rid, connection_id=connection_id, connection_identity=connection_id, source_id=source_id, kafka_topic_identity=str(source_id), kafka_topic=str(source_id), kafka_partition=0, kafka_offset=offset, result_status="complete", backend_received_at=at, backend_processed_at=at)
                    event = ParsedLog(source_id=source_id, connection_id=connection_id, normalizer_version=1, normalizer_name="Synthetic processor", source_name=source.name, connection_name="Synthetic input", kafka_topic=str(source_id), kafka_partition=0, kafka_offset=offset, deduplication_key=str(rid), raw="synthetic", ecs_data={"@timestamp":at.isoformat()}, fluent_bit_collected_at=at, backend_received_at=at, backend_processed_at=at)
                    assert SqlAlchemyProcessingRepository(s).store_event(record, event)
                    offset += 1
                if auto_history:
                    identity = UUID(auto_history["source"])
                    auto_source = s.get(Source, identity)
                    logical = auto_history["clock_at"]
                    clock_path = os.environ.get("AGGREGATE_TEST_CLOCK")
                    if clock_path:
                        clock_body = json.loads(Path(clock_path).read_text(encoding="utf-8"))
                        logical = clock_body["aggregates"].get(auto_history["aggregate"], logical)
                    for _ in range(1 + sequence % 3):
                        at, rid = datetime.now(UTC), uuid4()
                        record = ProcessedKafkaRecord(id=rid, connection_id=connection_id,
                            connection_identity=connection_id, source_id=identity,
                            kafka_topic_identity=str(identity), kafka_topic=str(identity), kafka_partition=0,
                            kafka_offset=auto_history["offset"], result_status="complete",
                            backend_received_at=at, backend_processed_at=at)
                        event = ParsedLog(source_id=identity, connection_id=connection_id,
                            normalizer_version=1, normalizer_name="Synthetic auto stream", source_name=auto_source.name,
                            connection_name="Synthetic input", kafka_topic=str(identity), kafka_partition=0,
                            kafka_offset=auto_history["offset"], deduplication_key=str(rid), raw="synthetic auto stream",
                            ecs_data={"@timestamp": logical}, fluent_bit_collected_at=at,
                            backend_received_at=at, backend_processed_at=at)
                        assert SqlAlchemyProcessingRepository(s).store_event(record, event)
                        auto_history["offset"] += 1
            # Retain real signed evidence and checkpoints. The gate is compatible
            # with event-time RR readers; it never assigns the quality result or
            # delays their snapshot until a future attestation is visible.
            publish_status("running", "gating_snapshot", sequence)
            with working_closure_gate(db) as proof:
                publish_status("running", "attesting", sequence)
                with db.session_factory.begin() as s:
                    s.execute(text("SET LOCAL lock_timeout='1000ms'"))
                    s.execute(text("SET LOCAL statement_timeout='3000ms'"))
                    s.scalar(select(Source).where(Source.id == source_id).with_for_update())
                    while datetime.now(UTC) < end:
                        s.add(KafkaCheckpoint(source_identity=source_id, topic_identity=str(source_id), worker_instance=worker, observed_at=s.scalar(select(func.clock_timestamp())), partitions={"0":{"low":0,"high":offset,"committed":offset}}))
                        time.sleep(.02)
                    probe = s.get(InputProbe, probe_id)
                    payload = {"id":str(uuid4()),"probe_id":str(probe_id),"source_id":str(source_id),"input_generation":str(probe.input_generation),"sequence":sequence,"start":start.isoformat(),"end":end.isoformat(),"state":"covered","topic_identity":str(source_id),"start_positions":{"0":first},"end_positions":{"0":offset}}
                    ingest_attestation(SqlAlchemyInputAttestationRepository(s), payload, private.sign(canonical_payload(payload)))
                # Give source-lock waiters time to commit their genuine barriers,
                # then wait for every committed barrier to pass through WAL.
                # A marker from another schedule is not proof for a pending one.
                time.sleep(.25)
                barrier_deadline = time.monotonic() + 2
                publish_status("running", "materializing_markers", sequence)
                captured_sources = [source_id, UUID(auto_history["source"])] if auto_history else [source_id]
                while proof.scalar(select(AvailabilityMarker.id).where(AvailabilityMarker.source_identity.in_(captured_sources), AvailabilityMarker.availability_at.is_(None)).limit(1)):
                    if time.monotonic() >= barrier_deadline:
                        raise FixtureCaptureTimeout("Real browser source markers did not materialize")
                    time.sleep(.01)
                capture_at = time.monotonic()
                proof.add(KafkaCheckpoint(source_identity=source_id, topic_identity=str(source_id), worker_instance=worker, observed_at=proof.scalar(select(func.clock_timestamp())), partitions={"0":{"low":0,"high":offset,"committed":offset}}))
            sequence += 1
            publish_status("running", "interval_committed", sequence)
            if aggregate_id is None and sequence >= 2:
                with db.session_factory() as s:
                    service = build_aggregate_service(s, catalog)
                    body = AggregatePreviewRequest(source_id=source_id, aggregate_rule=body.aggregate_rule, schedule=body.schedule, window_start_utc=anchor, time_basis="event_time")
                    checked = service.validate(body)
                    aggregate_id = str(service.create(CreateAggregateRequest(**body.model_dump(), name="Браузерный агрегат", rule_hash=checked.rule_hash), actor, str(uuid4())).id)
            if sequence == 22:
                Path("/tmp/detector-browser-fixture.json").write_text(json.dumps({"aggregate":aggregate_id,
                    "source":str(source_id),"anchor":anchor.isoformat(),
                    **({"auto_cleanup": {key: value for key, value in auto_history.items() if key != "offset"}}
                       if auto_history else {})}), encoding="utf-8")
    finally:
        db.dispose()


if __name__ == "__main__":
    import sys
    if sys.argv[1:] == ["--status"]:
        print(json.dumps(checked_status()), flush=True)
    else:
        publish_status("running", "starting")
        try:
            run()
        except BaseException as error:
            previous = read_status()
            publish_status("failed", previous.get("phase", "stopped"),
                           previous.get("sequence", 0), error=type(error).__name__,
                           sqlstate=getattr(getattr(error, "orig", None), "sqlstate", None))
            raise
        else:
            publish_status("failed", "budget_exhausted")
