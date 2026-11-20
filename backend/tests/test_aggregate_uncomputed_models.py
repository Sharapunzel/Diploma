"""Terminal computation refusals never enter the model or warmup input path.

Run directly with pytest; these contract/barrier cases require no database/JVM.
The DB/native companion cases live in ml/rcf_service.py.
"""

from contextlib import nullcontext
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from app.aggregation.event_time import OperationBudget
from app.detection.contracts import DetectionError, StaleCandidate
from app.detection.execution import ExecutionOwner
from app.repositories.sqlalchemy.aggregate_overlap import SqlAlchemyAggregateOverlapQueries
from app.services.implementations.detector_history import input_snapshot
from app.services.implementations.detectors import DetectorService


def working_case():
    anchor = datetime(2026, 11, 17, tzinfo=UTC)
    checkpoint = {"body": {"updates": 70}, "unchanged": "checkpoint"}
    context = {"now": anchor + timedelta(seconds=100),
        "aggregate": {"state": "active", "execution_mode": "current", "skipped_ranges": []},
        "schedule": {"id": uuid4(), "anchor_utc": anchor, "duration_seconds": 5,
            "execution_generation": 1, "paused": False, "active_cleanup_id": None},
        "detector": {"id": uuid4(), "execution_state": "running", "state": "ready",
            "checkpoint": checkpoint, "consumed_index": 10, "evaluation_start_index": 10,
            "revision": 4, "config_hash": "immutable", "execution_generation": 1,
            "start_request_id": None, "active_attempt_id": None}}
    point = {"id": uuid4(), "window_index": 11,
        "start_utc": anchor + timedelta(seconds=55), "end_utc": anchor + timedelta(seconds=60),
        "calculation_status": "uncomputed", "calculation_reason": "execution_budget_exceeded",
        "event_count": None, "features": None}
    repository, models, uow = Mock(), Mock(), Mock()
    repository.read_context.return_value = context
    repository.lock_context.return_value = deepcopy(context)
    repository.next_working_point.return_value = point
    uow.read_snapshot.side_effect = nullcontext
    owner = ExecutionOwner(uuid4(), 1, 123)
    api = DetectorService(repository, None, None, None, uow, models, owner)
    return api, context, point


def test_working_uncomputed_is_consumed_without_restoring_or_mutating_the_model():
    api, context, point = working_case()
    checkpoint = deepcopy(context["detector"]["checkpoint"])
    result = api.advance_one(context["detector"]["id"])
    assert result == {"state": "skipped", "reason": "execution_budget_exceeded",
        "point_id": point["id"], "finding": None}
    assert api.models.mock_calls == []
    _, saved_point, saved_checkpoint, outcome, finding = api.repository.save_transition.call_args.args
    assert saved_point is point and saved_checkpoint == checkpoint
    assert outcome["kind"] == "skipped" and outcome["reason"] == "execution_budget_exceeded"
    assert outcome["data"]["features"] is None and outcome["data"]["result"] is None
    assert finding is None and context["detector"]["checkpoint"] == checkpoint
    api.uow.commit.assert_called_once()
    assert api.transition_timings["model_seconds"] == 0


@pytest.mark.parametrize("cancel", ["pause", "generation", "archive"])
def test_working_uncomputed_still_obeys_the_final_lifecycle_fence(cancel):
    api, context, _ = working_case()
    current = api.repository.lock_context.return_value
    if cancel == "pause":
        current["schedule"]["paused"] = True
    elif cancel == "generation":
        current["schedule"]["execution_generation"] += 1
    else:
        current["aggregate"]["state"] = "archived"
    with pytest.raises(StaleCandidate):
        api.advance_one(context["detector"]["id"])
    api.repository.save_transition.assert_not_called()
    api.uow.commit.assert_not_called()
    assert api.models.mock_calls == []


def test_ordinary_warmup_refuses_uncomputed_before_reading_numeric_fields():
    with pytest.raises(DetectionError, match="warmup_window_uncomputed"):
        input_snapshot({"calculation_status": "uncomputed"})


def test_overlap_refuses_uncomputed_basis_before_reading_raw_membership():
    repository = SqlAlchemyAggregateOverlapQueries()
    repository.session, repository.read_budget = Mock(), Mock()
    repository.session.scalars.return_value = [
        SimpleNamespace(payload_state="present", calculation_status="uncomputed")]
    with pytest.raises(DetectionError, match="warmup_window_uncomputed"):
        repository.overlap_statistics(uuid4(), datetime.now(UTC), datetime.now(UTC),
            [10], None, OperationBudget(1))
    repository.session.scalars.assert_called_once()
    repository.session.execute.assert_not_called()
