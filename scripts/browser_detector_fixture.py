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


def publish_status(state, phase, sequence=0, error=None):
    payload = {"state": state, "phase": phase, "sequence": sequence,
               "pid": os.getpid(), "updated": time.monotonic(), "error": error,
               "capture_at": capture_at}
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
                    "phase": status["phase"], "error": status["error"]}
        os.kill(pid, 0)
        if not 0 <= time.monotonic() - status["updated"] <= 10:
            return {"ready": False, "reason": "fixture_progress_stale", "phase": status["phase"]}
        if status.get("capture_at") is None:
            return {"ready": False, "reason": "fixture_capture_starting", "phase": status["phase"]}
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


@contextmanager
def working_closure_gate(database):
    """Coordinate the *working* point's one-shot evidence read in this fixture.

    The real writer/probe commits and WAL still run independently. Historical
    readiness must succeed without this gate (covered by service DB tests).
    The transaction releases the table lock on success and every exception;
    no lifecycle/schedule/detector lock is acquired here.
    """
    from sqlalchemy import text

    with database.session_factory.begin() as proof:
        proof.execute(text("SET LOCAL lock_timeout='1000ms'"))
        proof.execute(text("SET LOCAL statement_timeout='3000ms'"))
        proof.execute(text("LOCK TABLE logs.source_interruptions IN ACCESS EXCLUSIVE MODE"))
        yield proof


def run():
    global capture_at
    from app.adapters.wal_bootstrap import read_setting
    from app.aggregate_composition import build_aggregate_service
    from app.core.security import hash_password
    from app.db import Database
    from app.ecs import PackagedEcsCatalog
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
    from app.repositories.sqlalchemy.input_attestations import (
        SqlAlchemyInputAttestationRepository,
    )
    from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
    from app.schemas.aggregates import AggregatePreviewRequest, CreateAggregateRequest
    from app.services.implementations.input_attestations import (
        canonical_payload,
        ingest_attestation,
    )
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from sqlalchemy import func, select, text
    from sqlalchemy.engine import make_url

    url = read_setting("DATABASE_URL")
    assert make_url(url).database == "diploma_test"
    db = Database(url)
    source_id, connection_id, actor, probe_id, worker = (uuid4() for _ in range(5))
    private = Ed25519PrivateKey.generate()
    anchor = datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=2)
    with db.session_factory.begin() as s:
        s.add(User(id=actor, username=os.environ["BROWSER_USERNAME"], password_hash=hash_password(os.environ["BROWSER_PASSWORD"]), display_name="Browser detector admin", role_id=UUID("00000000-0000-4000-8000-000000000001")))
        s.add(User(username=os.environ["BROWSER_GUEST"], password_hash=hash_password(os.environ["BROWSER_PASSWORD"]), display_name="Browser guest", role_id=UUID("00000000-0000-4000-8000-000000000002")))
        s.add(KafkaConnection(id=connection_id, name=str(connection_id), bootstrap_servers=["kafka:9092"], cluster_identity=str(connection_id)))
        s.flush()
        s.add(Source(id=source_id, connection_id=connection_id, name="Синтетический источник браузерной проверки", topic_name=str(source_id), kafka_topic_identity=str(source_id), is_enabled=True))
        s.flush()
        s.add(InputProbe(id=probe_id, source_identity=source_id, input_generation=uuid4(), public_key=private.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)))
    catalog = PackagedEcsCatalog.load()
    with db.session_factory() as s:
        service = build_aggregate_service(s, catalog)
        body = AggregatePreviewRequest(source_id=source_id, aggregate_rule={"features":[{"key":"events","name":"Число событий","op":"count"}]}, schedule={"boundary":anchor,"timezone":"UTC","duration_seconds":2}, window_start_utc=anchor-timedelta(seconds=2), time_basis="backend_processed_at_historical")
        # Creation needs a completed preview window. Anchor may precede current
        # time; wait for the first real interval rather than moving app clocks.
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
                    event = ParsedLog(source_id=source_id, connection_id=connection_id, normalizer_version=1, normalizer_name="Synthetic processor", source_name=source.name, connection_name="Synthetic input", kafka_topic=str(source_id), kafka_partition=0, kafka_offset=offset, deduplication_key=str(rid), raw="synthetic", ecs_data={"@timestamp":"2000-01-01T00:00:00Z"}, fluent_bit_collected_at=at, backend_received_at=at, backend_processed_at=at)
                    assert SqlAlchemyProcessingRepository(s).store_event(record, event)
                    offset += 1
            # Synchronize only the arrival of real test evidence, as in the
            # permanent runtime scenario: reports commit before source-locked
            # barriers; a fresh checkpoint follows actual WAL materialization.
            # No schedule, time, completeness result or capture state is changed.
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
                while proof.scalar(select(AvailabilityMarker.id).where(AvailabilityMarker.source_identity == source_id, AvailabilityMarker.availability_at.is_(None)).limit(1)):
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
                    body = AggregatePreviewRequest(source_id=source_id, aggregate_rule=body.aggregate_rule, schedule=body.schedule, window_start_utc=anchor, time_basis="availability_at")
                    checked = service.validate(body)
                    aggregate_id = str(service.create(CreateAggregateRequest(**body.model_dump(), name="Браузерный агрегат", rule_hash=checked.rule_hash), actor, str(uuid4())).id)
            if sequence == 22:
                Path("/tmp/detector-browser-fixture.json").write_text(json.dumps({"aggregate":aggregate_id,"source":str(source_id),"anchor":anchor.isoformat()}), encoding="utf-8")
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
                           previous.get("sequence", 0), error=type(error).__name__)
            raise
        else:
            publish_status("failed", "budget_exhausted")
