"""Clock, provenance and protection boundaries without elapsed-time sleeps."""

from datetime import UTC, datetime, timedelta

import pytest
from app.aggregation.event_time import (
    EventTimeBudgetError,
    OperationBudget,
    close_deadline,
    event_timestamp,
    index_at,
    online_start,
    suffix_size,
    warmup_indices,
)
from app.schemas.aggregates import CleanupRequest, CreateAggregateRequest
from pydantic import ValidationError


@pytest.mark.parametrize("duration", [1, 5, 299, 300, 301, 604800])
def test_absolute_deadline_and_bootstrap_containing_window(duration):
    anchor = datetime(2026, 10, 23, 14, 25, tzinfo=UTC)
    end = anchor + timedelta(seconds=duration)
    assert close_deadline(end, duration) == end + timedelta(seconds=min(duration, 300))
    assert online_start(anchor, duration, anchor - timedelta(days=1)) == 0
    assert online_start(anchor, duration, end - timedelta(microseconds=1)) == 0
    assert online_start(anchor, duration, end) == 1
    assert index_at(anchor, duration, end) == 1


@pytest.mark.parametrize("raw,proven,diagnostics,reason", [
    ("2026-01-01T00:00:00Z", False, [], "event_time_unknown_provenance"),
    ("2026-01-01T00:00:00Z", True, [{"code": "event_time_fallback"}], "event_time_fallback"),
    (None, True, [], "event_time_missing"), ("broken", True, [], "event_time_invalid"),
    (True, True, [], "event_time_invalid"), ([], True, [], "event_time_invalid"),
    ("0001-01-01T00:00:00+01:00", True, [], "event_time_invalid"),
    ("9999-12-31T23:59:59.999999Z", True, [], "event_time_future"),
])
def test_timestamp_exclusions_are_not_reconstructed(raw, proven, diagnostics, reason):
    assert event_timestamp({"@timestamp": raw}, diagnostics, proven,
        datetime(2026, 11, 14, tzinfo=UTC)) == (None, reason)


def test_future_is_relative_to_one_snapshot_and_zone_does_not_change_membership():
    reference = datetime(2026, 10, 23, 14, 30, tzinfo=UTC)
    at, reason = event_timestamp({"@timestamp": "2026-10-23T19:25:00+05:00"}, [], True, reference)
    assert reason is None and at == reference - timedelta(minutes=5)
    assert event_timestamp({"@timestamp": reference.isoformat()}, [], True, reference)[1] is None
    assert event_timestamp({"@timestamp": (reference + timedelta(microseconds=1)).isoformat()}, [], True, reference)[1] == "event_time_future"


def test_budget_remainder_is_shared_by_all_queries():
    clock = [100.0]
    budget = OperationBudget(10, clock=lambda: clock[0])
    clock[0] += 6
    assert budget.milliseconds() == 4000
    clock[0] += 4
    with pytest.raises(EventTimeBudgetError):
        budget.check()


def test_protection_uses_actual_suffix_and_ordinary_has_no_eight_window_minimum():
    assert suffix_size(range(1000), 900) == 100
    assert max(suffix_size(range(1000), i) for i in (900, 750)) == 250
    assert suffix_size([1, 4, 12, 20], 12) == 2
    ordinary = warmup_indices(3, 300, 300, 2, 2, False)
    assert ordinary["complete"] and len(ordinary["starts"]) == 4
    assert ordinary["starts"][-1] + 300 - ordinary["starts"][0] == 1200
    accelerated = warmup_indices(30, 300, 60, 8, 8, True)
    assert accelerated["count"] >= 16
    assert accelerated["starts"][-1] + 300 - accelerated["starts"][0] >= 2400


@pytest.mark.parametrize("value", [True, False, -1, 1.0, "1", None, 9223372036854775808])
def test_cleanup_body_is_strict_int64(value):
    with pytest.raises(ValidationError):
        CleanupRequest(keep_last=value)


def test_replay_transport_keeps_ast_guards():
    with pytest.raises(ValidationError):
        CreateAggregateRequest(source_id="00000000-0000-4000-8000-000000000001",
            aggregate_rule={"features": [{"key": "x", "name": "x", "op": "formula", "expr": {"aggregate": None}}]},
            schedule={"boundary": "2026-01-01T00:00:00Z", "timezone": "UTC", "duration_seconds": 300},
            window_start_utc="2026-01-01T00:00:00Z", name="invalid", rule_hash="0" * 64)
