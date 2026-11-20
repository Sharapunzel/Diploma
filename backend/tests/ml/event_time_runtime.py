"""Synthetic atomic writer/readback for the permanent Compose runtime smoke.

Invoked only inside that profile's containers. It never writes point quality,
model inputs, checkpoints or classifier results.
"""

import json
import os
import sys
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.engine import make_url

from app.config import settings
from app.core.security import hash_password
from app.db import Database
from app.detection.contracts import digest
from app.models import (
    Aggregate,
    AggregateCleanupOperation,
    AggregateClosure,
    AggregateSchedule,
    AggregateSnapshot,
    AvailabilityMarker,
    Detector,
    Finding,
    KafkaConnection,
    ParsedLog,
    ProcessedKafkaRecord,
    Source,
    User,
    WorkingWindowPoint,
)
from app.repositories.sqlalchemy.detectors import SqlAlchemyDetectorRepository
from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository


def write(session, source, offset, at, count, kind):
    now = datetime.now(UTC)
    for number in range(count):
        identity = uuid4()
        ecs = {"@timestamp": at.isoformat(), "event": {"category": [kind], "outcome": "failure"}}
        if kind == "authentication":
            ecs["user"] = {"name": "synthetic", "id": "smoke"}
        else:
            ecs.update(http={"request": {"method": "GET"}}, network={"bytes": 10 + offset + number})
        record = ProcessedKafkaRecord(id=uuid4(), connection_id=source.connection_id,
            connection_identity=source.connection_id, kafka_topic_identity=str(source.id), source_id=source.id,
            kafka_topic=source.topic_name, kafka_partition=0, kafka_offset=offset + number,
            result_status="complete", backend_received_at=now, backend_processed_at=now)
        event = ParsedLog(id=identity, source_id=source.id, connection_id=source.connection_id,
            normalizer_version=1, normalizer_name="synthetic-runtime", source_name=source.name,
            connection_name="synthetic-runtime", kafka_topic=source.topic_name, kafka_partition=0,
            kafka_offset=offset + number, deduplication_key=str(identity), fluent_bit_collected_at=now,
            backend_received_at=now, backend_processed_at=now, raw="synthetic runtime record",
            ecs_data=ecs, normalization_status="complete", normalization_diagnostics=[])
        assert SqlAlchemyProcessingRepository(session).store_event(record, event)
    return offset + count


def setup(database):
    anchor = (datetime.now(UTC) - timedelta(days=1)).replace(microsecond=0)
    with database.session_factory.begin() as session:
        session.add(User(username=os.environ["EVENT_TIME_USER"],
            password_hash=hash_password(os.environ["EVENT_TIME_PASSWORD"]), display_name="Runtime smoke",
            role_id=UUID("00000000-0000-4000-8000-000000000001")))
        connection = KafkaConnection(id=uuid4(), name=str(uuid4()),
            bootstrap_servers=["kafka:9092"], cluster_identity="event-time-runtime")
        session.add(connection)
        session.flush()
        sources, offsets = [], []
        for kind in ("authentication", "web"):
            source = Source(id=uuid4(), name=kind + " " + str(uuid4()), connection_id=connection.id,
                topic_name=str(uuid4()), is_enabled=True)
            session.add(source)
            session.flush()
            offset = 0
            for index in range(16):
                offset = write(session, source, offset, anchor + timedelta(seconds=index * 5), 1 + index % 3, kind)
            sources.append(str(source.id))
            offsets.append(offset)
    return {"sources": sources, "offsets": offsets, "anchor": anchor.isoformat()}


def browser_auto_facts(database, payload):
    """Read physical payload facts; never advance or alter a point."""
    from sqlalchemy import text

    identity = UUID(payload["aggregate"])
    with database.session_factory() as session:
        session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        session.execute(text("SET LOCAL statement_timeout='3000ms'"))
        canonical_count = session.scalar(select(func.count()).select_from(AggregateClosure).where(
            AggregateClosure.aggregate_id == identity, AggregateClosure.window_index.between(0, 519),
            AggregateClosure.origin == "initial_history", AggregateClosure.time_basis == "event_time"))
        if payload.get("readiness_only"):
            return {"canonical_history_count": canonical_count}
        from app.aggregate_composition import build_aggregate_service
        from app.core.errors import DomainError
        from app.ecs import PackagedEcsCatalog

        service = build_aggregate_service(session, PackagedEcsCatalog.load())
        closure = session.scalar(select(AggregateClosure).where(AggregateClosure.aggregate_id == identity,
            AggregateClosure.window_index == 0))
        if closure is None:
            return {"ready": False, "canonical_history_count": canonical_count}
        point_id = closure.origin_point_id
        facts = {"old_point_id": str(point_id), "old_window_index": 0,
            "canonical_history_count": canonical_count,
            "payload_state": closure.payload_state,
            "old_payloads": session.scalar(select(func.count()).select_from(WorkingWindowPoint).where(
                WorkingWindowPoint.id == point_id)),
            "old_snapshots": session.scalar(select(func.count()).select_from(AggregateSnapshot).where(
                AggregateSnapshot.closure_id == point_id))}
        if payload.get("verify"):
            assert str(point_id) == payload["old_point_id"]
            assert facts["payload_state"] == "cleared" and facts["old_payloads"] == facts["old_snapshots"] == 0, facts
            from app.models import DetectorOutcome, DetectorRecovery, DetectorWarmup

            ordinary_id = session.scalar(select(Detector.id).where(
                Detector.aggregate_id == identity, Detector.accelerated.is_(False)))
            recovery = session.execute(select(DetectorRecovery.first_index, DetectorRecovery.last_index).where(
                DetectorRecovery.detector_id == ordinary_id, DetectorRecovery.reason == "manual_resume"
                ).order_by(DetectorRecovery.started_at.desc(), DetectorRecovery.id.desc()).limit(1)).one()
            assert recovery.last_index - recovery.first_index + 1 == 520
            proofs = []
            for accelerated in (False, True):
                model = session.execute(select(Detector.id, Detector.consumed_index, Detector.evaluated_index,
                    Detector.checkpoint.is_not(None).label("checkpoint_present"), Detector.active_attempt_id,
                    Detector.accelerated_used, Detector.execution_state).where(Detector.aggregate_id == identity,
                        Detector.accelerated.is_(accelerated))).one()
                assert model.checkpoint_present and model.accelerated_used == accelerated and model.execution_state == "running"
                attempt = session.execute(select(DetectorWarmup.status, DetectorWarmup.inputs_hash,
                    func.jsonb_array_length(DetectorWarmup.inputs).label("inputs_count")).where(
                        DetectorWarmup.id == model.active_attempt_id)).one()
                assert attempt.status == "ready" and attempt.inputs_count == 512 and len(attempt.inputs_hash) == 64
                proof = {"detector_id": str(model.id), "accelerated": accelerated,
                    "checkpoint_present": True, "saved_inputs": attempt.inputs_count}
                outcomes = session.execute(select(func.count(), func.count(func.distinct(DetectorOutcome.window_index)),
                    func.count().filter(DetectorOutcome.kind == "evaluated"),
                    func.min(DetectorOutcome.window_index), func.max(DetectorOutcome.window_index)).where(
                        DetectorOutcome.detector_id == model.id,
                        DetectorOutcome.window_index.between(recovery.first_index, recovery.last_index))).one()
                assert tuple(outcomes[:3]) == (520, 520, 520), outcomes
                assert tuple(outcomes[3:]) == (recovery.first_index, recovery.last_index), outcomes
                assert model.consumed_index == model.evaluated_index == recovery.last_index
                proof.update(unread_from=recovery.first_index, unread_to=recovery.last_index,
                    evaluated_without_gaps=outcomes[0])
                proofs.append(proof)
            facts["models"] = proofs
            return facts
        try:
            point = service._saved_point(service.repository.get(identity), closure)
        except DomainError as error:
            raise AssertionError("Old auto point was not observed before deletion") from error
        assert facts["old_payloads"] == facts["old_snapshots"] == 1 and point.payload_state == "present"
        return {"ready": True, **facts, "old_point": point.model_dump(mode="json")}


def main():
    assert make_url(settings.database_url).database == "diploma_test", "Runtime requires the disposable profile DB"
    database = Database(settings.database_url)
    try:
        action = sys.argv[1]
        payload = json.loads(os.environ.get("EVENT_TIME_PAYLOAD", "{}"))
        if action == "setup":
            result = setup(database)
        elif action == "browser_auto_facts":
            result = browser_auto_facts(database, payload)
        elif action == "write":
            with database.session_factory.begin() as session:
                offsets = []
                for index, source_id in enumerate(payload["sources"]):
                    source = session.get(Source, UUID(source_id))
                    at = datetime.fromisoformat(payload["timestamp"]) if "timestamp" in payload else datetime.now(UTC)
                    offsets.append(write(session, source, payload["offsets"][index], at,
                        payload["count"], "authentication" if index == 0 else "web"))
            result = {"offsets": offsets}
        elif action == "timings":
            with database.session_factory() as session:
                repository = SqlAlchemyDetectorRepository(session)
                result = {identity: repository.public_result(UUID(identity), payload["index"])
                    for identity in payload["detectors"]}
        elif action == "inspect":
            with database.session_factory() as session:
                aggregates = [UUID(x) for x in payload["aggregates"]]
                detectors = list(session.scalars(select(Detector).where(Detector.aggregate_id.in_(aggregates))))
                findings = list(session.scalars(select(Finding).where(Finding.detector_id.in_([d.id for d in detectors]))))
                schedules = session.execute(select(Aggregate.id, AggregateSchedule).join(
                    AggregateSchedule, AggregateSchedule.id == Aggregate.schedule_id).where(Aggregate.id.in_(aggregates))).all()
                cleanups = list(session.scalars(select(AggregateCleanupOperation).where(AggregateCleanupOperation.aggregate_id.in_(aggregates))))
                result = {"checkpoints": {str(d.id): digest(d.checkpoint) if d.checkpoint else None for d in detectors},
                    "schedules": {str(aggregate_id): {key: getattr(schedule, key) for key in (
                        "id", "anchor_utc", "bootstrap_at", "online_start_index", "next_index", "paused",
                        "execution_generation", "active_cleanup_id")} for aggregate_id, schedule in schedules},
                    "cleanup_operations": {str(op.id): {key: getattr(op, key) for key in (
                        "status", "requested_keep", "reference_tail", "cutoff", "planned_delete", "deleted_points",
                        "progress", "completed_at", "safe_error")} for op in cleanups},
                    "closure_identities": {str(a): digest({str(c.origin_point_id): [c.window_index, c.time_basis,
                        c.origin, c.payload_state] for c in session.scalars(select(AggregateClosure).where(
                            AggregateClosure.aggregate_id == a))}) for a in aggregates},
                    "findings": {str(f.id): f.data for f in findings},
                    "payloads": {str(a): session.scalar(select(func.count()).select_from(AggregateClosure).where(
                        AggregateClosure.aggregate_id == a, AggregateClosure.payload_state == "present")) for a in aggregates},
                    "capture_pending": session.scalar(select(func.count()).select_from(AvailabilityMarker).where(
                        AvailabilityMarker.source_identity.in_(select(Aggregate.source_identity).where(Aggregate.id.in_(aggregates))),
                        AvailabilityMarker.availability_at.is_(None)))}
        else:
            raise ValueError("unknown runtime action")
        print(json.dumps(result, default=str, sort_keys=True))
    finally:
        database.dispose()


if __name__ == "__main__":
    main()
