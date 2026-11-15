"""One protection plan for all independently controlled detectors."""

from datetime import timedelta
from decimal import Decimal

from ...aggregation.event_time import warmup_indices
from ...detection.contracts import DetectionError, ModelConfig, digest, feature_vector
from ...detection.numeric import Affine


def checkpoint_boundary(detector, check_budget):
    saved = detector["checkpoint"]
    body = saved["body"]
    config = ModelConfig(**detector["config"])
    if (detector["config_hash"] != digest(detector["config"])
            or saved["sha256"] != digest(body) or body["config"] != detector["config"]
            or body["keys"] != detector["feature_keys"] or body["rule_hash"] != detector["rule_hash"]
            or body["settings"] != config.settings or not body.get("mapper")
            or not body.get("affine") or not isinstance(body.get("history"), list)):
        raise ValueError("checkpoint_invalid")
    forest, affine = body["mapper"]["forestState"], body["affine"]
    if (body["runtime"]["format"] != 1 or type(body["updates"]) is not int or body["updates"] < config.minimum_inputs
            or len(detector["feature_keys"]) != config.dimensions
            or forest["dimensions"] != config.dimensions or forest["numberOfTrees"] != config.trees
            or forest["sampleSize"] != config.sample_size or forest["outputAfter"] != config.output_after
            or forest["shingleSize"] != 1 or body["mapper"]["forestMode"] != "STANDARD"
            or body["mapper"]["transformMethod"] != "NONE"
            or affine["algorithm"] != "min_range_decimal_float32_v1"
            or len(affine["center"]) != config.dimensions or len(affine["scale"]) != config.dimensions
            or not 0 < len(body["history"]) <= config.sample_size):
        raise ValueError("checkpoint_invalid")
    transform = Affine(tuple(Decimal(x) for x in affine["center"]), tuple(Decimal(x) for x in affine["scale"]))
    if (any(not x.is_finite() for x in transform.center)
            or any(not x.is_finite() or x <= 0 for x in transform.scale)):
        raise ValueError("checkpoint_invalid")
    for row in body["history"]:
        check_budget()
        if any(type(x) not in (int, float) for x in row["ml"]):
            raise ValueError("checkpoint_invalid")
        if list(transform.convert([Decimal(x) for x in row["exact"]], check_budget=check_budget)) != row["ml"]:
            raise ValueError("checkpoint_invalid")
    consumed = detector["consumed_index"]
    earliest = consumed + 1 if consumed is not None else detector["evaluation_start_index"]
    if earliest is None or earliest < 0:
        raise ValueError("checkpoint_cursor_unavailable")
    return earliest


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
                    if attempt and attempt["detector_id"] != detector["id"]:
                        raise ValueError("prepared_input_owner_mismatch")
                    if attempt and attempt["status"] == "ready":
                        raise ValueError("checkpoint_missing_after_warmup")
                    if attempt and attempt["status"] == "prepared" and attempt["inputs_hash"] != digest(attempt["inputs"]):
                        raise ValueError("prepared_input_hash_mismatch")
                    if (attempt and attempt["status"] == "prepared"
                            and attempt["inputs_hash"] == digest(attempt["inputs"])
                            and len(attempt["inputs"]) == attempt["plan"]["count"]):
                        for item in attempt["inputs"]:
                            budget.check()
                            feature_vector(item["features"], detector["feature_keys"], item["completeness"])
                            if "ml_input" not in item:
                                raise ValueError("prepared_input_incomplete")
                        earliest = attempt["plan"]["evaluation_start_index"]
                        reason = "prepared_warmup_continuation"
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
