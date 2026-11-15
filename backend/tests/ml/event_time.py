"""Official AWS RCF on immutable event-time inputs and compacted history."""

import copy
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.adapters.detector_lock import DetectorSessionLock
from app.adapters.model_runtime import ModelRuntime
from app.aggregate_composition import build_aggregate_service
from app.detection.model_process import BoundedModelFactory
from app.detector_composition import build_detector_service
from app.models import Finding
from test_event_time_pipeline import (
    create,
    turns,
    write_event,
)

pytest_plugins = ["test_event_time_pipeline"]


@pytest.mark.parametrize("accelerated", [False, True])
def test_official_warmup_finding_restore_and_physical_cleanup(context, accelerated, request):
    database, catalog, _, _ = context
    runtime = ModelRuntime()
    request.addfinalizer(runtime.close)
    models = BoundedModelFactory(transport=runtime)
    anchor = (datetime.now(UTC) - timedelta(days=1)).replace(microsecond=0)
    offset = 0
    for index in range(16):
        for second in range(1 + index % 3):
            write_event(context, (anchor + timedelta(seconds=index * 10 + second)).isoformat(), offset=offset)
            offset += 1
    card, _ = create(context, anchor, 10)
    turns(context, card.id, 60)
    lock = DetectorSessionLock(os.environ["TEST_DATABASE_URL"])
    try:
        pid = lock.acquire()
        assert pid is not None
        with database.session_factory() as session:
            owner = build_detector_service(session, catalog).acquire_owner(uuid4(), pid)
            service = build_detector_service(session, catalog, owner, models=models)
            identity = service.create(card.id, UUID(int=1), "AWS event time",
                config={"sample_size": 8, "output_after": 8, "trees": 20},
                accelerated=accelerated, step=5 if accelerated else None)
            service.start(identity)
            service.begin_start(identity)
            prepared = service.prepare_warmup(identity)
            assert prepared["state"] == "prepared", prepared
            assert service.warmup(identity) == {"state": "ready"}
            before = service.state(identity)["detector"]["checkpoint"]
        first = anchor + timedelta(seconds=card.online_start_index * 10)
        for index, count in enumerate([2, 1, 100, 400]):
            for number in range(count):
                write_event(context, (first + timedelta(seconds=index * 10, microseconds=number)).isoformat(), offset=offset)
                offset += 1
        clock = first + timedelta(seconds=60)
        for _ in range(24):
            with database.session_factory() as session:
                aggregates = build_aggregate_service(session, catalog)
                aggregates.repository.now = lambda: clock
                aggregates.advance(card.id)
        for _ in range(4):
            with database.session_factory() as session:
                service = build_detector_service(session, catalog, owner, models=models)
                # Controlled time tests long-window recovery without sleeping.
                read = service.repository.read_context
                service.repository.read_context = lambda identity, read=read: {**read(identity), "now": clock}
                result = service.advance_one(identity)
                assert result["state"] == "evaluated", result
        with database.session_factory() as session:
            service = build_detector_service(session, catalog, owner, models=models)
            after = service.state(identity)["detector"]["checkpoint"]
            assert before != after
            service.stop(identity)
            findings = list(session.scalars(select(Finding).where(Finding.detector_id == identity)))
            assert findings, "Official classifier must create the controlled anomaly"
            snapshots = {f.id: copy.deepcopy(f.data) for f in findings}
            for finding in findings:
                assert finding.data["exact_values"] and finding.data["expected"]
                assert finding.data["time_basis"] == "event_time"
                assert finding.data["accelerated_used"] is accelerated
                assert finding.data["evaluation_completed_at"]
            aggregates = build_aggregate_service(session, catalog)
            plan = aggregates.cleanup_preview(card.id, 0)
            receipt = aggregates.aggregate_command(card.id, UUID(int=1), "compact", "cleanup", plan.minimum_keep)
        turns(context, card.id, 100)
        with database.session_factory() as session:
            aggregates = build_aggregate_service(session, catalog)
            assert aggregates.cleanup_operation(card.id, receipt.cleanup_operation_id).status == "succeeded"
            for finding_id, expected in snapshots.items():
                assert session.get(Finding, finding_id).data == expected
            service = build_detector_service(session, catalog, owner, models=models)
            assert service.state(identity)["detector"]["checkpoint"] == after
            assert service.public_card(identity).last_evaluated is not None
            service.start(identity)
            service.begin_start(identity)
            assert service.resume(identity)["state"] == "running"
    finally:
        lock.close()
