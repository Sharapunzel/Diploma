"""Permanent Compose scenario: prepared history, real WAL/aggregate/RCF workers.

Invoked only by scripts/agent_test_profiles.py inside the isolated ML service.
History consists of explicitly synthetic saved complete working snapshots.
Live points use signed coverage and actual pgoutput materialization.
"""
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4, uuid5

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url

from app.adapters.wal_bootstrap import read_setting
from app.aggregate_composition import build_aggregate_service
from app.db import Database
from app.detector_composition import build_detector_service
from app.ecs import PackagedEcsCatalog
from app.models import (
    AggregateSchedule,
    AvailabilityMarker,
    DetectorOutcome,
    Finding,
    InputProbe,
    KafkaCheckpoint,
    KafkaConnection,
    ParsedLog,
    ProcessedKafkaRecord,
    Source,
    WorkingWindowPoint,
)
from app.repositories.sqlalchemy.input_attestations import SqlAlchemyInputAttestationRepository
from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository
from app.schemas.aggregates import AggregatePreviewRequest, CreateAggregateRequest
from app.services.implementations.input_attestations import canonical_payload, ingest_attestation


def wait_for(check, timeout=40):
    deadline = time.monotonic() + timeout
    while True:
        value = check()
        if value:
            return value
        assert time.monotonic() < deadline, "detector runtime deadline"
        time.sleep(.05)


def run():
    url = read_setting("DATABASE_URL")
    assert make_url(url).database == "diploma_test", "isolated profile only"
    db, catalog = Database(url), PackagedEcsCatalog.load()
    sources, aggregates, detectors, probes, keys, workers = [], [], [], [], [], []
    anchor = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=300)
    connection = uuid4()
    try:
        with db.session_factory.begin() as session:
            session.add(KafkaConnection(id=connection, name=str(connection), bootstrap_servers=["kafka:9092"], cluster_identity=str(connection)))
            session.flush()
            for name in ("authentication", "web"):
                source_id = uuid4()
                source = Source(id=source_id, connection_id=connection, name=name + str(uuid4()),
                    topic_name=str(uuid4()), kafka_topic_identity=str(source_id), is_enabled=True)
                session.add(source)
                sources.append(source.id)
        for i, source in enumerate(sources):
            feature = {"key": "n", "name": "Events" if i == 0 else "Bytes", "op": "count" if i == 0 else "sum"}
            if i:
                feature["field"] = "network.bytes"
            body = AggregatePreviewRequest(source_id=source, aggregate_rule={"features": [feature]},
                schedule={"boundary": anchor, "timezone": "UTC", "duration_seconds": 5},
                window_start_utc=anchor, time_basis="backend_processed_at_historical")
            with db.session_factory() as session:
                service = build_aggregate_service(session, catalog)
                checked = service.validate(body)
                card = service.create(CreateAggregateRequest(**body.model_dump(), name="Detector runtime",
                    rule_hash=checked.rule_hash), UUID(int=1), str(uuid4()))
                aggregates.append(card)
            # Long history is prepared without physically waiting its age. The
            # accelerated flag uses its legal step=duration boundary here; real
            # overlapping batches have independent ML/DB regression coverage.
            with db.session_factory.begin() as session:
                for index in range(60):
                    left = anchor + timedelta(seconds=5 * index)
                    session.add(WorkingWindowPoint(id=uuid4(), schedule_id=card.schedule_id,
                        window_index=index, start_utc=left, end_utc=left + timedelta(seconds=5),
                        scheduled_at=left + timedelta(seconds=5), actual_started_at=left + timedelta(seconds=5),
                        actual_completed_at=left + timedelta(seconds=5), event_count=1,
                        event_time_min=None, event_time_max=None, completeness="complete",
                        completeness_reason="synthetic_prepared_history", evidence_refs={},
                        features=[{"key": "n", "value": 1 + index % 3, "reason": None, "valid_count": 1, "invalid_count": 0}]))
                session.get(AggregateSchedule, card.schedule_id).next_index = 60
                private, probe = Ed25519PrivateKey.generate(), uuid4()
                session.add(InputProbe(id=probe, source_identity=source, input_generation=uuid4(),
                    public_key=private.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)))
                keys.append(private)
                probes.append(probe)
                workers.append(uuid4())
            with db.session_factory() as session:
                service = build_detector_service(session, catalog)
                identity = service.create(card.id, UUID(int=1), "Runtime RCF",
                    config={"sample_size": 16, "output_after": 16, "trees": 20}, accelerated=bool(i), step=5 if i else None)
                assert service.state(identity)["detector"]["execution_state"] == "created"
                service.start(identity)
                detectors.append(identity)
        def ready():
            with db.session_factory() as session:
                states = [build_detector_service(session, catalog).state(identity)["detector"] for identity in detectors]
                assert not any(row["execution_state"] == "failed" for row in states), [
                    (row["execution_state"], row["failure_code"], row["reason"]) for row in states]
                return all(row["execution_state"] == "running" for row in states)
        wait_for(ready)
        # Choose a future whole window after startup; previously skipped unknown
        # windows are retained outcomes and do not train the model.
        now = datetime.now(UTC)
        first = int((now - anchor).total_seconds() // 5) + 1
        offsets = [0, 0]
        delays = []
        for sequence, value in enumerate((2, 100), start=1):
            start = anchor + timedelta(seconds=5 * (first + sequence - 1))
            end = start + timedelta(seconds=5)
            with db.session_factory.begin() as session:
                for i, source_id in enumerate(sources):
                    session.add(KafkaCheckpoint(source_identity=source_id, topic_identity=str(source_id),
                        worker_instance=workers[i], partitions={"0": {"low": 0, "high": offsets[i], "committed": offsets[i]}}))
            wait_for(lambda start=start: datetime.now(UTC) >= start, timeout=10)
            def caught_up(sequence=sequence):
                with db.session_factory() as session:
                    indices = session.scalars(select(AggregateSchedule.next_index).where(
                        AggregateSchedule.id.in_([card.schedule_id for card in aggregates]))).all()
                    return all(index >= first + sequence - 1 for index in indices)
            wait_for(caught_up, timeout=3)
            with db.session_factory.begin() as session:
                for i, source_id in enumerate(sources):
                    source = session.get(Source, source_id)
                    for _ in range(value if i == 0 else 1):
                        at, rid = datetime.now(UTC), uuid4()
                        record = ProcessedKafkaRecord(id=rid, connection_id=connection, connection_identity=connection,
                            source_id=source_id, kafka_topic_identity=str(source_id), kafka_topic=source.topic_name,
                            kafka_partition=0, kafka_offset=offsets[i], result_status="complete",
                            backend_received_at=at, backend_processed_at=at)
                        event = ParsedLog(id=uuid4(), source_id=source_id, connection_id=connection,
                            normalizer_version=1, normalizer_name="Synthetic ECS", source_name=source.name,
                            connection_name="runtime", kafka_topic=source.topic_name, kafka_partition=0,
                            kafka_offset=offsets[i], deduplication_key=str(rid), raw="synthetic",
                            ecs_data={"@timestamp": "2000-01-01T00:00:00Z", "event": {"category": ["authentication" if i == 0 else "web"]},
                                      "network": {"bytes": value}}, fluent_bit_collected_at=at,
                            backend_received_at=at, backend_processed_at=at)
                        assert SqlAlchemyProcessingRepository(session).store_event(record, event)
                        offsets[i] += 1
            # Control the synthetic evidence arrival without changing quality
            # rules: hold the first coverage read until post-barrier consumer
            # evidence is committed. This test-only gate never holds a schedule
            # or detector lock and is released before any RCF evaluation.
            with db.session_factory.begin() as proof:
                proof.execute(text("SET LOCAL lock_timeout='1000ms'"))
                proof.execute(text("SET LOCAL statement_timeout='3000ms'"))
                proof.execute(text("LOCK TABLE logs.source_interruptions IN ACCESS EXCLUSIVE MODE"))
                # Hold Source across the closing boundary, so the completed signed
                # report commits before the aggregate's real WAL barrier.
                with db.session_factory.begin() as session:
                    session.scalars(select(Source).where(Source.id.in_(sources)).order_by(Source.id).with_for_update()).all()
                    while datetime.now(UTC) < end:
                        at = datetime.now(UTC)
                        for i, source_id in enumerate(sources):
                            session.add(KafkaCheckpoint(source_identity=source_id, topic_identity=str(source_id),
                                worker_instance=workers[i], observed_at=at,
                                partitions={"0": {"low": 0, "high": offsets[i], "committed": offsets[i]}}))
                        # Evidence rows are committed with the report, before barrier.
                        time.sleep(.1)
                    for i, source_id in enumerate(sources):
                        probe = session.get(InputProbe, probes[i])
                        payload = {"id": str(uuid4()), "probe_id": str(probe.id), "source_id": str(source_id),
                            "input_generation": str(probe.input_generation), "sequence": sequence,
                            "start": start.isoformat(), "end": end.isoformat(), "state": "covered", "topic_identity": str(source_id),
                            "start_positions": {"0": offsets[i] - (value if i == 0 else 1)}, "end_positions": {"0": offsets[i]}}
                        ingest_attestation(SqlAlchemyInputAttestationRepository(session), payload, keys[i].sign(canonical_payload(payload)))
                barriers = [uuid5(card.schedule_id, f"working-window:{first + sequence - 1}") for card in aggregates]
                deadline = time.monotonic() + 2
                while len(proof.scalars(select(AvailabilityMarker.id).where(
                        AvailabilityMarker.id.in_(barriers), AvailabilityMarker.availability_at.is_not(None))).all()) != 2:
                    assert time.monotonic() < deadline, "Real WAL barriers did not materialize"
                    time.sleep(.01)
                at = proof.scalar(select(func.clock_timestamp()))
                for i, source_id in enumerate(sources):
                    proof.add(KafkaCheckpoint(source_identity=source_id, topic_identity=str(source_id),
                        worker_instance=workers[i], observed_at=at,
                        partitions={"0": {"low": 0, "high": offsets[i], "committed": offsets[i]}}))
            def evaluated(sequence=sequence):
                with db.session_factory.begin() as session:
                    at = session.scalar(select(func.clock_timestamp()))
                    for i, source_id in enumerate(sources):
                        session.add(KafkaCheckpoint(source_identity=source_id, topic_identity=str(source_id),
                            worker_instance=workers[i], observed_at=at, partitions={"0": {"low": 0, "high": offsets[i], "committed": offsets[i]}}))
                    rows = list(session.scalars(select(DetectorOutcome).where(DetectorOutcome.detector_id.in_(detectors),
                        DetectorOutcome.window_index == first + sequence - 1)))
                    return (rows, session.scalar(select(func.clock_timestamp()))) if len(rows) == 2 else None
            results, visible_at = wait_for(evaluated, timeout=10)
            visible_delay = (visible_at - end).total_seconds()
            assert visible_delay < 5, visible_delay
            delays.append(visible_delay)
            for result in results:
                with db.session_factory() as session:
                    saved_point = session.get(WorkingWindowPoint, result.working_point_id)
                    assert result.kind == "evaluated", (result.reason, saved_point.completeness_reason, saved_point.evidence_refs)
                    assert saved_point.features[0]["value"] == value
                grade = result.data["result"]["grade"]
                assert grade == 0 if sequence == 1 else grade > 0
                delay = (datetime.fromisoformat(result.data["actual_completed_at"]) - end).total_seconds()
                assert delay < 5, delay
        with db.session_factory() as session:
            findings = list(session.scalars(select(Finding).where(Finding.detector_id.in_(detectors))))
            assert len(findings) >= 2
            assert all(item.data["expected"] and item.data["attribution"] for item in findings)
            for identity in detectors:
                service = build_detector_service(session, catalog)
                service.stop(identity)
        result = {"detectors": [str(x) for x in detectors], "max_completion_seconds": max(delays),
                  "findings": len(findings), "windows_seconds": 5}
        Path("/tmp/detector-runtime-result.json").write_text(json.dumps(result), encoding="utf-8")
        print(json.dumps(result), flush=True)
    finally:
        with db.session_factory.begin() as session:
            for identity in sources:
                source = session.get(Source, identity)
                if source is not None:
                    source.is_enabled = False
        db.dispose()


if __name__ == "__main__":
    run()
