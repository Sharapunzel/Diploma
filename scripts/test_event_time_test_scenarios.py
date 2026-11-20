"""Runtime selection is rejected before Docker or temporary preparation."""

from types import SimpleNamespace
from uuid import UUID

import pytest

from scripts.event_time_test_scenarios import (
    archive_point,
    exact_late_growth,
    fresh_online_suffix,
    runtime_card,
    validate_runtime_arguments,
)


def arguments(**changes):
    return SimpleNamespace(**{"profile": "smoke-development", "aggregate_only": True,
        "detector_only": False, "rcf": False, "wal": False, "pytest": None,
        "browser_only": False, **changes})


@pytest.mark.parametrize("profile", ["smoke-development", "smoke-production", "end-to-end"])
def test_runtime_selection_accepts_both_supplied_modes_and_existing_e2e(profile):
    validate_runtime_arguments(arguments(profile=profile))


@pytest.mark.parametrize("changes", [
    {"profile": "frontend"}, {"profile": "backend-db"}, {"profile": "kafka-integration"},
    {"profile": "external-end-to-end"}, {"detector_only": True}, {"rcf": True}, {"wal": True},
    {"pytest": []}, {"pytest": ["tests/test_example.py"]}, {"browser_only": True},
])
def test_runtime_selection_rejects_conflicting_or_unrelated_profiles(changes):
    with pytest.raises(ValueError):
        validate_runtime_arguments(arguments(**changes))


def test_runtime_diagnostics_include_positions_without_private_payloads():
    summary = runtime_card("detector", {"aggregate_id": "aggregate", "source_id": "source",
        "last_consumed": {"index": 12}, "last_evaluated": {"index": 11}, "backlog": 2,
        "checkpoint": "private", "raw": "private", "password": "private"})
    assert summary["consumed_index"] == 12 and summary["evaluated_index"] == 11
    assert summary["detector_id"] == "detector" and summary["backlog"] == 2
    assert not {"checkpoint", "raw", "password"} & summary.keys()


def response(status, body):
    return SimpleNamespace(status_code=status, json=lambda: body)


def archive_fixture():
    aggregate, identity = str(UUID(int=1)), str(UUID(int=2))
    point = {"id": identity, "aggregate_id": aggregate, "window_index": 0,
        "origin": "initial_history", "time_basis": "event_time", "payload_state": "present",
        "features": [{"value": 2}], "event_count": 2}
    cleared = {"code": "aggregate_point_cleared", "details": {"origin_point_id": identity,
        "window_index": 0, "origin": "initial_history", "time_basis": "event_time",
        "clear_reason": "automatic", "clear_policy_version": "model_history_v1",
        "cleared_at": "2026-11-16T00:00:00Z"}}
    return aggregate, point, cleared


def test_canonical_archive_accepts_legitimate_automatic_clear_but_preserves_identity_and_finality():
    aggregate, point, cleared = archive_fixture()
    present = archive_point(response(200, point), aggregate, 0)
    assert archive_point(response(200, point), aggregate, 0, present) == present
    tombstone = archive_point(response(410, cleared), aggregate, 0, present)
    assert tombstone["id"] == point["id"] and tombstone["cleared"]
    assert archive_point(response(410, cleared), aggregate, 0, tombstone) == tombstone
    with pytest.raises(AssertionError, match="reappeared"):
        archive_point(response(200, point), aggregate, 0, tombstone)
    with pytest.raises(AssertionError):
        archive_point(response(410, {**cleared, "details": {**cleared["details"],
            "origin_point_id": str(UUID(int=3))}}), aggregate, 0, present)


def test_archive_late_read_still_compares_the_entire_available_payload():
    aggregate, point, _ = archive_fixture()
    present = archive_point(response(200, point), aggregate, 0)
    with pytest.raises(AssertionError, match="changed a saved"):
        archive_point(response(200, {**point, "features": [{"value": 3}]}), aggregate, 0, present)


def test_archive_readiness_does_not_mistake_not_ready_unknown_or_wrong_clear_for_canonical_history():
    aggregate, point, cleared = archive_fixture()
    assert archive_point(response(409, {"code": "canonical_point_not_ready"}), aggregate, 0) is None
    present = archive_point(response(200, point), aggregate, 0)
    with pytest.raises(AssertionError):
        archive_point(response(409, {"code": "canonical_point_not_ready"}), aggregate, 0, present)
    for status, body in ((404, {"code": "aggregate_point_not_found"}),
            (410, {**cleared, "details": {**cleared["details"], "clear_reason": "manual"}}),
            (410, {**cleared, "details": {**cleared["details"], "window_index": 1}})):
        with pytest.raises(AssertionError):
            archive_point(response(status, body), aggregate, 0)


def test_fresh_model_history_requires_the_latest_16_consecutive_present_complete_online_points():
    points = [{"origin": "online", "window_index": 17300 - offset, "payload_state": "present",
        "time_basis": "event_time", "completeness": "complete"} for offset in range(17)]
    assert fresh_online_suffix(points) == points[:16]
    assert fresh_online_suffix(points[:15]) is None
    for change in ({"window_index": 17000}, {"payload_state": "cleared"},
            {"completeness": "unknown"}, {"time_basis": "availability_at"}):
        changed = [{**point, **change} if index == 8 else point for index, point in enumerate(points)]
        assert fresh_online_suffix(changed) is None


def test_single_fresh_late_record_requires_exactly_one_new_diagnostic_count():
    assert not exact_late_growth(2, 2)
    assert exact_late_growth(3, 2)
    with pytest.raises(AssertionError, match="Unexpected extra"):
        exact_late_growth(4, 2)
