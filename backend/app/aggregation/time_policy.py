"""Window execution policy; elapsed time is independent of queue and UTC grid."""

from datetime import timedelta
from math import isfinite

POLICY_VERSION = "window_execution_v1"
SOFT_LATENCY_SECONDS = 5
FINALIZATION_SECONDS = 5


class RecoveryTimeError(ValueError):
    pass


def recovery_remaining(started_at, deadline_at, seconds, reference):
    """UTC recovery may shorten an allowance, never move its origin forward."""
    tolerance = .000001  # persisted timestamp precision, not another allowance
    if (not isfinite(seconds) or not 0 < seconds <= 30
            or abs((deadline_at - started_at).total_seconds() - seconds) > tolerance
            or reference < started_at - timedelta(seconds=tolerance)):
        raise RecoveryTimeError("execution_recovery_time")
    remaining = (deadline_at - reference).total_seconds()
    if remaining > seconds + tolerance:
        raise RecoveryTimeError("execution_recovery_time")
    return min(seconds, max(0.0, remaining))


def execution_policy(duration):
    if type(duration) is not int or duration <= 0:
        raise ValueError("invalid_window_duration")
    return {"policy_version": POLICY_VERSION, "soft_latency_seconds": SOFT_LATENCY_SECONDS,
        "hard_budget_seconds": min(30.0, (4 * duration) / 5)}


def execution_diagnostics(attempt, measured_at, elapsed, status, through, timings=None):
    available = {key: value for key, value in (timings or {}).items()
        if key in {"selection", "manifest", "statistics", "interpret", "cohorts", "snapshot", "save", "commit"}}
    if timings and "total" in timings:
        available["snapshot"] = timings["total"]
    return {"policy_version": POLICY_VERSION, "soft_latency_seconds": SOFT_LATENCY_SECONDS,
        "hard_budget_seconds": attempt.hard_budget_seconds, "calculation_status": status,
        "calculation_reason": "execution_budget_exceeded" if status == "uncomputed" else None,
        "attempt_id": str(attempt.attempt_id), "origin": attempt.origin,
        "window_index": attempt.window_index, "start_utc": attempt.start_utc.isoformat(),
        "end_utc": attempt.end_utc.isoformat(), "started_at": attempt.started_at.isoformat(),
        "execution_deadline_at": attempt.deadline_at.isoformat(), "measured_at": measured_at.isoformat(),
        "measured_through": through, "elapsed_seconds": max(0.0, elapsed),
        "queue_seconds": None, "online_latency_seconds": None, "soft_latency_exceeded": None,
        "timings": available}
