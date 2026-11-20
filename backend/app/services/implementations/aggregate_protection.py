"""One protection plan for all independently controlled detectors."""

from datetime import timedelta

from ...aggregation.event_time import warmup_indices
from ...detection.contracts import DetectionError
from ...detection.protection import attempt_bounds, checkpoint_boundary, prepared_boundary


class AggregateProtectionService:
    def __init__(self, repository):
        self.repository = repository

    def plan(self, aggregate, schedule, detectors, keep_last, budget):
        self.repository.read_budget(budget.milliseconds())
        total = self.repository.cleanup_count(aggregate.id)
        self.repository.read_budget(budget.milliseconds())
        tail = self.repository.cleanup_tail(aggregate.id)
        self.repository.read_budget(budget.milliseconds())
        last = self.repository.canonical_last_index(schedule.id)
        blockers, minimum, stopped = [], 0, True
        for detector in detectors:
            budget.check()
            if (aggregate.execution_mode == "retired" and detector["execution_state"] == "terminated"
                    and detector["failure_code"] == "aggregate_time_semantics_retired"):
                continue
            if detector["execution_state"] != "stopped":
                stopped = False
                blockers.append({"detector_id": detector["id"], "name": detector["name"],
                    "reason": "cleanup_requires_stopped_detectors", "minimum_keep": total,
                    "protected_start_index": None, "protected_end_index": last})
                minimum = total
                continue
            try:
                if detector["checkpoint"] is not None:
                    earliest = checkpoint_boundary(detector, budget.check)
                    reason = "unconsumed_points"
                else:
                    attempt = self.repository.prepared_attempt(detector["active_attempt_id"], budget)
                    if detector["active_attempt_id"] and attempt is None:
                        raise ValueError("warmup_attempt_missing")
                    if attempt:
                        stored = {key: getattr(schedule, key) for key in
                            ("anchor_utc", "duration_seconds", "online_start_index")}
                        first, _last, online = attempt_bounds(detector, stored, attempt, budget.check)
                        if attempt["status"] == "prepared":
                            earliest = prepared_boundary(detector, stored, attempt, budget.check)
                            reason = "prepared_warmup_continuation"
                        elif attempt["status"] in {"preparing", "waiting"}:
                            earliest, reason = min(first, online), "frozen_warmup"
                        else:
                            raise ValueError("checkpoint_missing_after_warmup")
                    else:
                        config = detector["config"]
                        plan = warmup_indices(last if last is not None else -1,
                            schedule.duration_seconds, detector["step_seconds"], config["sample_size"],
                            config["output_after"], detector["accelerated"])
                        earliest = plan["base_first"] if plan["base_first"] is not None else 0
                        base_starts = [schedule.anchor_utc + timedelta(seconds=i * schedule.duration_seconds)
                            for i in range(earliest, plan["base_last"] + 1)]
                        self.repository.warmup_payload_preflight(schedule.id, base_starts,
                            lambda: self.repository.read_budget(budget.milliseconds()))
                        reason = "planned_warmup" if plan["complete"] else "insufficient_warmup_history"
                self.repository.read_budget(budget.milliseconds())
                needed = self.repository.cleanup_count(aggregate.id, earliest)
            except (KeyError, TypeError, ValueError, ArithmeticError, DetectionError) as error:
                from ...aggregation.event_time import EventTimeBudgetError
                raise EventTimeBudgetError("protection_plan") from error
            minimum = max(minimum, needed)
            if needed:
                blockers.append({"detector_id": detector["id"], "name": detector["name"],
                    "reason": reason, "minimum_keep": needed,
                    "protected_start_index": earliest, "protected_end_index": last})
        reason = ("cleanup_requires_stopped_detectors" if not stopped else
                  "cleanup_protected_points" if keep_last < minimum else None)
        return {"aggregate_id": aggregate.id, "reference_at": self.repository.now(),
            "requested_keep": keep_last, "total_points": total, "minimum_keep": minimum,
            "maximum_delete": total - minimum, "planned_delete": max(0, total - keep_last),
            "tail_identity": tail, "can_cleanup": reason is None, "reason": reason, "blockers": blockers}
