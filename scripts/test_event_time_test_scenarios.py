"""Runtime selection is rejected before Docker or temporary preparation."""

from types import SimpleNamespace

import pytest

from scripts.event_time_test_scenarios import runtime_card, validate_runtime_arguments


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
