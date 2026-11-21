"""Synthetic read-side data for PostgreSQL and browser tests."""

import json
import os
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from app.core.security import hash_password
from app.db import Database
from app.models import (
    AggregateSchedule,
    KafkaConnection,
    ParsedLog,
    ProcessedKafkaRecord,
    Source,
    User,
)
from app.models.aggregate_history import AggregateClosure
from app.models.aggregates import Aggregate
from app.models.detectors import Detector, Finding
from app.repositories.sqlalchemy.processing import SqlAlchemyProcessingRepository

ADMIN = UUID("00000000-0000-4000-8000-000000000001")
GUEST = UUID("00000000-0000-4000-8000-000000000002")
SOURCE_NAME = "zz-findings-original"
DETECTOR_NAME = "Сохранённый детектор"


def seed_findings(session: Session, actor: UUID, *, extra_sources: int = 0):
    now = datetime.now(UTC)
    anchor = (now - timedelta(minutes=10)).replace(second=0, microsecond=0)
    lower = anchor + timedelta(microseconds=123456)
    upper = lower + timedelta(seconds=1)
    source, connection, schedule, aggregate = (uuid4() for _ in range(4))
    detector_ids = [uuid4(), uuid4()]

    session.add(
        KafkaConnection(id=connection, name="findings-test", bootstrap_servers=["127.0.0.1:1"])
    )
    session.flush()
    session.add(
        Source(
            id=source,
            connection_id=connection,
            name=SOURCE_NAME,
            topic_name="findings-test",
            kafka_topic_identity=str(source),
        )
    )
    for index in range(extra_sources):
        session.add(
            Source(
                connection_id=connection,
                name=f"0-findings-decoy-{index:02}",
                topic_name=f"findings-decoy-{index:02}",
            )
        )
    session.add(
        AggregateSchedule(
            id=schedule,
            source_identity=source,
            anchor_utc=anchor,
            duration_seconds=60,
            semantic_basis="event_time",
            paused=True,
        )
    )
    session.flush()
    session.add(
        Aggregate(
            id=aggregate,
            schedule_id=schedule,
            name="Текущее имя агрегата",
            source_identity=source,
            source_name=SOURCE_NAME,
            source_type="kafka",
            rule={"private": True},
            rule_hash="a" * 64,
            original_boundary=anchor.isoformat(),
            timezone="UTC",
            created_at=anchor,
            created_by=actor,
            state="archived",
            request_key=str(aggregate),
            request_hash="a" * 64,
            semantic_basis="event_time",
            execution_mode="current",
        )
    )
    session.flush()
    session.add_all(
        Detector(
            id=identity,
            aggregate_id=aggregate,
            name=f"Текущий детектор {index}",
            created_by=actor,
            created_at=anchor,
            updated_at=now,
            config={"private": True},
            config_hash="b" * 64,
            feature_keys=["a", "b", "c"],
            feature_names=["А", "Б", "В"],
            rule_hash="a" * 64,
            state="stopped",
            step_seconds=60,
            checkpoint={"private": True},
        )
        for index, identity in enumerate(detector_ids)
    )
    session.flush()

    saved = {}
    cases = [
        ("older", -1, 1, 0.3, 0.9),
        ("primary", 0, 0, 0.8, 0.9),
        ("single", 0, 0, 0.8, 0.7),
        ("legacy", 60, 1, 0.4, 0.6),
        ("after", 120, 1, 0.6, 0.8),
        ("max", 180, 0, 0.9, 0.9),
    ]
    if extra_sources:
        cases.extend(
            (f"week-{index}", -days * 86400 + index * 60, index % 2, 0.5, 0.8)
            for index, days in enumerate((6, 5, 5, 3, 3, 3, 1, 1))
        )
    for index, (key, start_delta, detector, grade, confidence) in enumerate(cases):
        start = anchor + timedelta(seconds=start_delta)
        end = start + timedelta(seconds=60)
        point, finding = uuid4(), uuid4()
        time_basis = "availability_at" if key == "legacy" else "event_time"
        session.add(
            AggregateClosure(
                origin_point_id=point,
                aggregate_id=aggregate,
                schedule_id=schedule,
                source_identity=source,
                window_index=index,
                start_utc=start,
                end_utc=end,
                time_basis=time_basis,
                origin="online",
                snapshot_reference_at=end,
                closed_at=end,
                close_not_before_at=end,
                payload_state="cleared",
                point_kind="working",
                cleared_at=now,
                clear_reason="manual",
            )
        )
        session.flush()
        snapshot = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "detector_name": DETECTOR_NAME,
            "grade": grade,
            "confidence": confidence,
            "aggregate_name": "Сохранённый агрегат",
            "source_id": str(source),
            "source_name": SOURCE_NAME,
            "feature_keys": ["a", "b", "c"],
            "exact_values": ["1152921504606846977", "0.000000000000000001", "3"],
            "expected": ["1152921504606846976.75", "0.1", "0"],
            "expected_origin": "aws_rcf",
            "feature_names": ["Счётчик", "Плотность", "Без вклада"],
            "attribution": [3, 1, 0],
            "event_count": 2,
            "event_time_min": lower.isoformat(),
            "event_time_max": upper.isoformat(),
            "time_basis": time_basis,
            "accelerated_used": key == "primary",
            "aggregation_started_at": end.isoformat(),
            "aggregation_completed_at": end.isoformat(),
            "evaluation_started_at": end.isoformat(),
            "evaluation_completed_at": end.isoformat(),
            "model_config": {"private": True},
            "aggregate_rule": {"private": True},
            "checkpoint": {"private": True},
            "quality": {"snapshot": {"private": True}},
        }
        if key == "legacy":
            for field in (
                "time_basis",
                "event_time_min",
                "event_time_max",
                "event_count",
                "evaluation_started_at",
                "attribution",
                "expected_origin",
            ):
                del snapshot[field]
            snapshot["expected"] = ["1"]
        elif key == "single":
            snapshot.update(event_count=1, event_time_max=lower.isoformat())
        elif key == "max":
            snapshot.update(event_time_max=datetime.max.replace(tzinfo=UTC).isoformat())
        elif key.startswith("week-"):
            snapshot.update(event_count=0, event_time_min=None, event_time_max=None)
        session.add(
            Finding(
                id=finding,
                detector_id=detector_ids[detector],
                working_point_id=point,
                created_at=now,
                late=key == "primary",
                data=snapshot,
            )
        )
        saved[key] = str(finding)

    events = (
        (lower.isoformat(), "findings-min", []),
        (
            (upper + timedelta(seconds=30)).replace(tzinfo=None).isoformat() + "+00:00:30",
            "findings-max",
            [],
        ),
        (
            (lower + timedelta(microseconds=1)).isoformat(),
            "findings-late",
            [{"code": "event_time_fallback"}],
        ),
        ((upper + timedelta(microseconds=1)).isoformat(), "findings-outside", []),
    )
    writer = SqlAlchemyProcessingRepository(session)
    for index, (event_time, message, diagnostics) in enumerate(events):
        record = ProcessedKafkaRecord(
            id=uuid4(),
            connection_id=connection,
            connection_identity=connection,
            source_id=source,
            kafka_topic_identity=str(source),
            kafka_topic="findings-test",
            kafka_partition=0,
            kafka_offset=index,
            result_status="complete",
            backend_received_at=now,
            backend_processed_at=now,
        )
        written = writer.store_event(
            record,
            ParsedLog(
                source_id=source,
                connection_id=connection,
                normalizer_name="findings-test",
                normalizer_version=1,
                source_name=SOURCE_NAME,
                connection_name="findings-test",
                kafka_topic="findings-test",
                kafka_partition=0,
                kafka_offset=index,
                deduplication_key=f"{source}:{index}",
                fluent_bit_collected_at=now,
                backend_received_at=now,
                backend_processed_at=now,
                normalization_diagnostics=diagnostics,
                raw="private raw fixture",
                ecs_data={"@timestamp": event_time, "message": message},
            ),
        )
        assert written
    return {
        "source": str(source),
        "source_name": SOURCE_NAME,
        "detector_name": DETECTOR_NAME,
        "detectors": [str(identity) for identity in detector_ids],
        "findings": saved,
        "start": anchor.isoformat(),
        "stop": (anchor + timedelta(seconds=120)).isoformat(),
        "from": lower.isoformat().replace("+00:00", "Z"),
        "to": (upper + timedelta(microseconds=1)).isoformat().replace("+00:00", "Z"),
    }


def main():
    from app.config import settings

    if make_url(settings.database_url).database != "diploma_test":
        raise RuntimeError("Findings browser fixtures require the isolated profile database")
    database = Database(settings.database_url)
    try:
        with database.session_factory.begin() as session:
            actor = session.scalar(
                select(User.id).where(User.username == os.environ["BROWSER_USERNAME"])
            )
            if actor is None:
                raise RuntimeError("Missing browser admin")
            guest = "findings-browser-guest"
            session.add(
                User(
                    username=guest,
                    display_name="Findings guest",
                    role_id=GUEST,
                    password_hash=hash_password(os.environ["BROWSER_PASSWORD"]),
                )
            )
            config = seed_findings(session, actor, extra_sources=51)
        print(json.dumps({**config, "guest": guest}))
    finally:
        database.dispose()


if __name__ == "__main__":
    main()
