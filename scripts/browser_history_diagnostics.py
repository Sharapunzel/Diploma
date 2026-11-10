"""Bounded read-only history provenance for the owned detector browser database."""
import json
from datetime import datetime
from uuid import uuid5


def snapshot(database):
    from app.models import (
        Aggregate,
        AvailabilityMarker,
        Detector,
        DetectorWarmup,
        InputAttestation,
        WorkingWindowPoint,
    )
    from sqlalchemy import func, select, text

    with database.session_factory.begin() as session:
        session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        session.execute(text("SET LOCAL statement_timeout='2000ms'"))
        observed_at = session.scalar(select(func.clock_timestamp()))
        facts = []
        for attempt in session.scalars(select(DetectorWarmup).order_by(
                DetectorWarmup.created_at.desc()).limit(12)):
            detector = session.get(Detector, attempt.detector_id)
            aggregate = session.get(Aggregate, detector.aggregate_id)

            def barrier(identity):
                row = session.get(AvailabilityMarker, identity)
                return ({"id": str(row.id), "created": row.created_at,
                         "materialized": row.availability_at is not None,
                         "availability": row.availability_at} if row else None)

            details = []
            points = list(session.scalars(select(WorkingWindowPoint).where(
                WorkingWindowPoint.schedule_id == aggregate.schedule_id,
                WorkingWindowPoint.start_utc.in_([
                    datetime.fromisoformat(item["start"]) for item in attempt.inputs]))))
            by_start = {point.start_utc: point for point in points}
            for item in attempt.inputs:
                start, end = datetime.fromisoformat(item["start"]), datetime.fromisoformat(item["end"])
                point = by_start.get(start)
                if item["completeness"] == "complete" and (point is None or point.completeness == "complete"):
                    continue
                if len(details) == 3:
                    break
                reports = session.scalars(select(InputAttestation).where(
                    InputAttestation.source_identity == aggregate.source_identity,
                    InputAttestation.start_utc < end, InputAttestation.end_utc > start
                ).order_by(InputAttestation.start_utc, InputAttestation.sequence).limit(8))
                details.append({"start": start, "end": end, "point_id": item.get("point_id"),
                    "basis": item["basis"], "completeness": item["completeness"], "reason": item["reason"],
                    "working": ({"id": str(point.id), "completeness": point.completeness,
                        "reason": point.completeness_reason, "completed": point.actual_completed_at,
                        "barrier": barrier(uuid5(point.schedule_id, f"working-window:{point.window_index}"))}
                        if point else None),
                    "reports": [{"id": str(row.id), "sequence": row.sequence, "start": row.start_utc,
                        "end": row.end_utc, "received": row.received_at} for row in reports]})
            facts.append({"observed_at": observed_at, "attempt": str(attempt.id), "detector": str(detector.id),
                "execution": detector.execution_state, "failure_code": detector.failure_code,
                "status": attempt.status, "reason": attempt.reason, "cutoff": attempt.cutoff,
                "created": attempt.created_at, "barrier": barrier(uuid5(attempt.id, "warmup")),
                "working_inputs": sum(bool(item.get("point_id")) for item in attempt.inputs),
                "historical_inputs": sum(not item.get("point_id") for item in attempt.inputs),
                "windows": details})
        return facts


if __name__ == "__main__":
    from app.adapters.wal_bootstrap import read_setting
    from app.db import Database
    from sqlalchemy.engine import make_url

    url = read_setting("DATABASE_URL")
    assert make_url(url).database == "diploma_test"
    db = Database(url)
    try:
        print(json.dumps(snapshot(db), default=str, ensure_ascii=True))
    finally:
        db.dispose()
