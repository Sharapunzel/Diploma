"""Real atomic input writer and bounded readback for controlled load profiles."""

import json
import os
import sys
from datetime import UTC, datetime, timedelta
from time import monotonic
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select, text
from sqlalchemy.engine import make_url

from app.adapters.wal_bootstrap import read_setting
from app.core.security import hash_password
from app.db import Database
from app.models import (
    AggregateClosure,
    AggregateHistorySpan,
    AggregateMembership,
    AggregateRetentionState,
    AggregateSchedule,
    AggregateSnapshot,
    AvailabilityMarker,
    KafkaConnection,
    ParsedLog,
    ProcessedKafkaRecord,
    Source,
    User,
    WorkingWindowPoint,
)
from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository


def setup(database, payload):
    bootstrap, anchor = load_timeline(datetime.now(UTC))
    with database.session_factory.begin() as session:
        actor = User(username=os.environ["AGGREGATE_LOAD_USER"],
            password_hash=hash_password(os.environ["AGGREGATE_LOAD_PASSWORD"]), display_name="Controlled aggregate load",
            role_id=UUID("00000000-0000-4000-8000-000000000001"))
        connection_id = uuid4()
        connection = KafkaConnection(id=connection_id, name=str(uuid4()), bootstrap_servers=["kafka:9092"],
            cluster_identity="controlled-aggregate-load:" + str(connection_id))
        session.add_all([actor, connection])
        session.flush()
        sources = []
        for index in range(payload["sources"]):
            source = Source(id=uuid4(), connection_id=connection.id, name="Controlled load " + str(index),
                topic_name=str(uuid4()), is_enabled=True)
            source.kafka_topic_identity = str(source.id)
            session.add(source)
            sources.append(str(source.id))
    return {"sources": sources, "actor": str(actor.id), "anchor": anchor.isoformat(),
        "clock_at": bootstrap.isoformat(), "history_windows": 520, "heavy_history_rows": 10001}


def load_timeline(started):
    bootstrap = started.replace(microsecond=0) + timedelta(seconds=1200)
    return bootstrap, bootstrap - timedelta(seconds=520 * 300)


def create(database, payload):
    from app.aggregate_composition import build_aggregate_service
    from app.ecs import PackagedEcsCatalog
    from app.schemas.aggregates import (
        AggregatePreviewRequest,
        CreateAggregateRequest,
        ValidateRequest,
    )

    bootstrap, anchor = datetime.fromisoformat(payload["clock_at"]), datetime.fromisoformat(payload["anchor"])
    assert anchor + timedelta(seconds=520 * 300) == bootstrap
    shift = payload.get("anchor_shift", 0)
    assert type(shift) is int and -2 <= shift <= 0
    started = monotonic()
    with database.session_factory() as session:
        actor = session.get(User, UUID(payload["actor"]))
        assert actor is not None and actor.username == os.environ["AGGREGATE_LOAD_USER"]
        assert actor.role_id == UUID("00000000-0000-4000-8000-000000000001")
        service = build_aggregate_service(session, PackagedEcsCatalog.load())
        physical_now = service.repository.now
        assert 0 < (bootstrap - physical_now()).total_seconds() <= 1200
        bootstrap += timedelta(seconds=shift)
        anchor += timedelta(seconds=shift)
        # Window time is frozen only for this explicit target. Technical policy
        # scheduling remains on the original physical DB clock, including its
        # unchanged 60s interval/UUID phase. No state or point is inserted here.
        add_state = service.repository.add_retention_state
        service.repository.add_retention_state = lambda aggregate, _now: add_state(aggregate, physical_now())
        service.repository.now = lambda: bootstrap
        body = ValidateRequest(source_id=payload["source"], aggregate_rule=payload["rule"],
            schedule={"boundary": anchor, "timezone": "UTC", "duration_seconds": 300})
        checked = service.validate(body)
        preview = AggregatePreviewRequest(**body.model_dump(), window_start_utc=anchor, time_basis="event_time")
        service.preview(preview)
        card = service.create(CreateAggregateRequest(**preview.model_dump(), name=payload["name"],
            rule_hash=checked.rule_hash), actor.id, str(uuid4()))
        assert card.online_start_index == 520
        return {"card": card.model_dump(mode="json"), "seconds": monotonic() - started}


def write(database, payload):
    count = payload["count"]
    if type(count) is not int or not 0 < count <= 1000:
        raise ValueError("writer batches are bounded at 1000 real records")
    started = monotonic()
    source_id, offset = UUID(payload["source"]), payload["offset"]
    timestamps = payload.get("timestamps")
    at = datetime.fromisoformat(payload["timestamp"]) if timestamps is None else None
    with database.session_factory.begin() as session:
        session.execute(text("SET LOCAL statement_timeout='10000ms'"))
        session.execute(text("SET LOCAL lock_timeout='1000ms'"))
        source = session.get(Source, source_id)
        writer = SqlAlchemyProcessingRepository(session)
        for number in range(count):
            timestamp = datetime.fromisoformat(timestamps[number]) if timestamps is not None else at
            now, identity, coordinate = datetime.now(UTC), uuid4(), offset + number
            record = ProcessedKafkaRecord(id=uuid4(), connection_id=source.connection_id,
                connection_identity=source.connection_id, source_id=source_id,
                kafka_topic_identity=str(source_id), kafka_topic=source.topic_name, kafka_partition=0,
                kafka_offset=coordinate, result_status="complete", backend_received_at=now, backend_processed_at=now)
            event = ParsedLog(id=identity, source_id=source_id, connection_id=source.connection_id,
                normalizer_version=1, normalizer_name="Controlled load", source_name=source.name,
                connection_name="Controlled load", kafka_topic=source.topic_name, kafka_partition=0,
                kafka_offset=coordinate, deduplication_key=str(identity), raw="synthetic controlled load",
                ecs_data={"@timestamp": timestamp.isoformat(), "event": {"category": ["authentication", "web"],
                    "outcome": "failure" if coordinate % 4 else "success", "action": "logon"},
                    "process": {"pid": 10 + coordinate % 97}, "network": {"bytes": 10 + coordinate},
                    "source": {"bytes": 20 + coordinate * 3}, "destination": {"bytes": 10 + coordinate},
                    "user": {"name": "synthetic"}, "tags": ["controlled", "load"]},
                fluent_bit_collected_at=now, backend_received_at=now, backend_processed_at=now)
            assert writer.store_event(record, event)
    return {"offset": offset + count, "rows": count, "seconds": monotonic() - started}


def write_history(database, payload):
    anchor = datetime.fromisoformat(payload["anchor"])
    timestamps = [(anchor + timedelta(seconds=index * 300)).isoformat()
        for index in range(520) if index != 8 for _ in range(1 + index % 3)]
    result, seconds = {"offset": payload["offset"]}, 0.0
    for start in range(0, len(timestamps), 1000):
        batch = timestamps[start:start + 1000]
        result = write(database, {**payload, "offset": result["offset"], "count": len(batch), "timestamps": batch})
        seconds += result["seconds"]
    return {"offset": result["offset"], "rows": len(timestamps), "seconds": seconds}


def _loaded_cleared_at(closure):
    """Optional metadata from loaded state only; never invoke an ORM descriptor."""
    try:
        value = vars(closure).get("cleared_at") if closure is not None else None
        if isinstance(value, datetime) and value.tzinfo is not None:
            return value.astimezone(UTC).isoformat()
    except Exception:  # noqa: BLE001 - optional diagnostic never changes inspect success
        pass
    return None


def inspect(database, payload):
    identities = [UUID(identity) for identity in payload["aggregates"]]
    with database.session_factory() as session:
        session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        session.execute(text("SET LOCAL statement_timeout='3000ms'"))
        session.execute(text("SET LOCAL lock_timeout='1000ms'"))
        result = {}
        for identity in identities:
            counts = session.execute(select(
                func.count().filter(AggregateClosure.origin == "initial_history"),
                func.count().filter(AggregateClosure.calculation_status == "uncomputed"))
                .where(AggregateClosure.aggregate_id == identity)).one()
            state = session.get(AggregateRetentionState, identity)
            closure = session.scalar(select(AggregateClosure).where(AggregateClosure.aggregate_id == identity,
                AggregateClosure.window_index == 8))
            point_id = closure.origin_point_id if closure else None
            schedule = session.scalar(select(AggregateSchedule).join(AggregateClosure,
                AggregateClosure.schedule_id == AggregateSchedule.id).where(
                    AggregateClosure.aggregate_id == identity).limit(1))
            result[str(identity)] = {
                "next_index": schedule.next_index if schedule else None,
                "last_closed_index": session.scalar(select(func.max(AggregateClosure.window_index)).where(
                    AggregateClosure.aggregate_id == identity)),
                "old_point_id": str(point_id) if point_id else None,
                "old_payload_state": closure.payload_state if closure else None,
                "clear_reason": closure.clear_reason if closure else None,
                "old_cleared_at": _loaded_cleared_at(closure),
                "execution_generation": schedule.execution_generation if schedule else None,
                "active_cleanup_id": str(schedule.active_cleanup_id) if schedule and schedule.active_cleanup_id else None,
                "initial_history_closed": counts[0],
                "terminal_uncomputed": counts[1],
                "history_remaining": session.scalar(select(func.coalesce(func.sum(func.greatest(
                    AggregateHistorySpan.last_index - AggregateHistorySpan.next_index + 1, 0)), 0)).where(
                        AggregateHistorySpan.aggregate_id == identity)),
                "old_payloads": session.scalar(select(func.count()).select_from(WorkingWindowPoint).where(
                    WorkingWindowPoint.id == point_id)) if point_id else 0,
                "old_snapshots": session.scalar(select(func.count()).select_from(AggregateSnapshot).where(
                    AggregateSnapshot.closure_id == point_id)) if point_id else 0,
                "membership_pairs": session.scalar(select(func.count()).select_from(AggregateMembership).where(
                    AggregateMembership.closure_id == point_id)) if point_id else 0,
                "membership_details": session.scalar(select(func.count()).select_from(AggregateMembership).where(
                    AggregateMembership.closure_id == point_id,
                    or_(AggregateMembership.fingerprint.is_not(None),
                        AggregateMembership.event_time.is_not(None)))) if point_id else 0,
                "auto_cleared_total": state.auto_cleared_total if state else None,
                "next_check_at": state.next_check_at if state else None,
                "pending_closure_id": str(state.pending_closure_id) if state and state.pending_closure_id else None,
                "pending_phase": state.pending_phase if state else None,
                "status": state.status if state else "missing",
                "reason": state.reason if state else "retention_state_missing",
            }
        connections = session.execute(text("SELECT application_name,count(*) FROM pg_stat_activity "
            "WHERE datname=current_database() GROUP BY application_name")).all()
        pending = session.scalar(select(func.count()).select_from(AvailabilityMarker).where(
            AvailabilityMarker.source_identity.in_([UUID(source) for source in payload["sources"]]),
            AvailabilityMarker.availability_at.is_(None)))
        return {"aggregates": result, "capture_pending": pending,
            "connections": {name: count for name, count in connections}, "observed_at": datetime.now(UTC).isoformat()}


def main():
    raw = read_setting("DATABASE_URL")
    url = make_url(raw)
    if url.database != "diploma_test":
        raise ValueError("controlled load requires the isolated profile database")
    database = Database(url.update_query_dict({"connect_timeout": "5"}).render_as_string(hide_password=False))
    try:
        payload = json.loads(os.environ.get("AGGREGATE_LOAD_PAYLOAD", "{}"))
        action = sys.argv[1]
        if action == "setup":
            result = setup(database, payload)
        elif action == "create":
            result = create(database, payload)
        elif action == "write":
            result = write(database, payload)
        elif action == "history":
            result = write_history(database, payload)
        elif action == "inspect":
            result = inspect(database, payload)
        else:
            raise ValueError("unknown controlled load action")
        print(json.dumps(result, default=str, sort_keys=True))
    finally:
        database.dispose()


if __name__ == "__main__":
    main()
