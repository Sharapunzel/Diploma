"""Technical model-history reserve and conservative all-detector protection."""

from dataclasses import dataclass
from uuid import UUID

from ..detection.contracts import ModelConfig, digest, validate_persisted_config
from ..detection.protection import attempt_bounds, checkpoint_boundary, prepared_boundary
from .event_time import warmup_indices

POLICY_VERSION = "model_history_v1"
CHECK_SECONDS = 60
STATUSES = frozenset({"scheduled", "idle", "compacting", "protected", "deferred", "error", "inactive"})
REASONS = frozenset({"retention_state_missing", "aggregate_inactive", "aggregate_paused",
    "aggregate_time_semantics_retired", "cleanup_in_progress", "dependencies_changed",
    "protection_unavailable", "retention_budget", "retention_database_error", "retention_turn_failed",
    "model_history_reserve", "unconsumed_points", "frozen_warmup", "prepared_warmup_continuation",
    "planned_warmup", "insufficient_warmup_history"})


@dataclass(frozen=True)
class MembershipCompaction:
    cursor: UUID | None
    changed_count: int


def retired(detector):
    return (detector["execution_state"] == "terminated"
            and detector["failure_code"] == "aggregate_time_semantics_retired")


def protection_plan(schedule, detectors, attempts, last_closed, last_present, check_budget):
    """Return a logical index suffix, never a count of selected good rows."""
    reserve, protections = ModelConfig(dimensions=1).minimum_inputs, []
    for detector in detectors:
        check_budget()
        if retired(detector):
            continue
        config = validate_persisted_config(detector["config"])
        if (detector["config_hash"] != digest(detector["config"], check_budget=check_budget)
                or config.dimensions != len(detector["feature_keys"])
                or len(set(detector["feature_keys"])) != config.dimensions
                or type(detector["accelerated"]) is not bool
                or type(detector["step_seconds"]) is not int
                or not 0 < detector["step_seconds"] <= schedule["duration_seconds"]
                or detector["state"] not in {"waiting", "warming", "ready", "stopped", "error"}
                or detector["execution_state"] not in {"created", "requested", "starting", "running", "stopped", "failed", "terminated"}):
            raise ValueError("detector_dependency_invalid")
        reserve = max(reserve, config.minimum_inputs)
        if detector["checkpoint"] is not None:
            earliest, reason = checkpoint_boundary(detector, check_budget), "unconsumed_points"
        else:
            if detector["state"] == "ready":
                raise ValueError("checkpoint_missing_after_warmup")
            attempt = attempts.get(detector["active_attempt_id"])
            if detector["active_attempt_id"] is not None and attempt is None:
                raise ValueError("warmup_attempt_missing")
            if attempt is not None:
                first, _last, online = attempt_bounds(detector, schedule, attempt, check_budget)
                if attempt["status"] == "prepared":
                    earliest, reason = prepared_boundary(detector, schedule, attempt, check_budget), "prepared_warmup_continuation"
                elif attempt["status"] in {"preparing", "waiting"}:
                    earliest, reason = min(first, online), "frozen_warmup"
                else:
                    raise ValueError("checkpoint_missing_after_warmup")
            else:
                plan = warmup_indices(last_present if last_present is not None else -1,
                    schedule["duration_seconds"], detector["step_seconds"], config.sample_size,
                    config.output_after, detector["accelerated"])
                earliest = plan["base_first"] if plan["complete"] else 0
                reason = "planned_warmup" if plan["complete"] else "insufficient_warmup_history"
        protections.append({"detector_id": str(detector["id"]), "name": detector["name"],
            "reason": reason, "protected_from_index": earliest,
            "protected_to_index": last_closed if last_closed is not None and last_closed >= earliest else None})
    reserve_from = max(0, last_closed - reserve + 1) if last_closed is not None else 0
    protected = min((p["protected_from_index"] for p in protections), default=None)
    return {"reserve_windows": reserve, "keep_from_index": min(reserve_from, protected) if protected is not None else reserve_from,
        "protected_from_index": protected, "protected_total": len(protections), "protected_by": protections[:10]}
