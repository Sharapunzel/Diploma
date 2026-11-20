"""Strict persisted dependencies shared by manual and automatic protection.

No database or JVM is used here. A valid, self-contained saved input batch can
release its original canonical payloads; an incomplete attempt still pins its
immutable plan, irrespective of a newer aggregate tail.
"""

from datetime import datetime, timedelta

from .checkpoint import affine_transform, matches_ml_carrier, validate_checkpoint
from .contracts import DetectionError, digest, feature_vector, validate_persisted_config
from .history import plan_windows
from .numeric import Affine


def checkpoint_boundary(detector, check_budget):
    """Validate the existing envelope, mapper, affine, history and cursor."""
    check_budget()
    if detector["config_hash"] != digest(detector["config"], check_budget=check_budget):
        raise DetectionError("checkpoint_config_mismatch")
    validate_checkpoint(detector["checkpoint"], detector["config"], detector["feature_keys"],
                        detector["rule_hash"], check_budget=check_budget)
    consumed = detector["consumed_index"]
    evaluation = detector["evaluation_start_index"]
    if (type(evaluation) is not int or evaluation < 0
            or consumed is not None and (type(consumed) is not int or consumed < evaluation)):
        raise DetectionError("checkpoint_cursor_unavailable")
    return consumed + 1 if consumed is not None else evaluation


def attempt_bounds(detector, schedule, attempt, check_budget):
    """Validate the saved grid; return canonical base bounds and online start."""
    check_budget()
    if attempt["detector_id"] != detector["id"]:
        raise DetectionError("prepared_input_owner_mismatch")
    plan, config = attempt["plan"], validate_persisted_config(detector["config"])
    duration, step = schedule["duration_seconds"], detector["step_seconds"]
    expected = config.minimum_inputs if not detector["accelerated"] else max(
        config.minimum_inputs, 1 + (7 * duration + step - 1) // step)
    if (any(type(plan[key]) is not int for key in ("count", "step", "duration", "evaluation_start_index"))
            or plan["count"] != expected or not 0 < expected <= 4096
            or plan["duration"] != duration or plan["step"] != step
            or not 0 < step <= duration or not detector["accelerated"] and step != duration):
        raise DetectionError("warmup_plan_invalid")
    first, cutoff = datetime.fromisoformat(plan["first_start"]), datetime.fromisoformat(plan["cutoff"])
    if first.utcoffset() is None or cutoff.utcoffset() is None or attempt["cutoff"] != cutoff:
        raise DetectionError("warmup_plan_invalid")
    unit, anchor = timedelta(seconds=duration), schedule["anchor_utc"]
    distance = cutoff - anchor
    cutoff_index, remainder = divmod(distance, unit)
    if (remainder or first < anchor or cutoff != first + unit + timedelta(seconds=(expected - 1) * step)
            or plan["evaluation_start_index"] != max(schedule["online_start_index"], cutoff_index)):
        raise DetectionError("warmup_plan_invalid")
    base_first, base_last = (first - anchor) // unit, cutoff_index - 1
    if "base_closures" in plan:
        identities = plan["base_closures"]
        if ([row["window_index"] for row in identities] != list(range(base_first, base_last + 1))
                or len({row["origin_point_id"] for row in identities}) != len(identities)):
            raise DetectionError("warmup_plan_invalid")
    return base_first, base_last, plan["evaluation_start_index"]


def prepared_boundary(detector, schedule, attempt, check_budget):
    """Only an exact valid batch releases the canonical source of preparation."""
    _, _, online = attempt_bounds(detector, schedule, attempt, check_budget)
    inputs = attempt["inputs"]
    if (attempt["status"] != "prepared" or len(inputs) != attempt["plan"]["count"]
            or attempt["inputs_hash"] != digest(inputs, check_budget=check_budget)):
        raise DetectionError("prepared_input_hash_mismatch")
    check_budget()
    transform = affine_transform(inputs[0]["affine"], len(detector["feature_keys"]), check_budget)
    bases = attempt["plan"].get("base_closures")
    by_index = {row["window_index"]: row["origin_point_id"] for row in bases} if bases is not None else None
    unit = timedelta(seconds=attempt["plan"]["duration"])
    rows = []
    for item, (start, end) in zip(inputs, plan_windows(attempt["plan"]), strict=True):
        check_budget()
        values = feature_vector(item["features"], detector["feature_keys"], item["completeness"])
        if (item["basis"] != "event_time" or datetime.fromisoformat(item["start"]) != start
                or datetime.fromisoformat(item["end"]) != end or item.get("quality_reason")
                or item["exact"] != values or not matches_ml_carrier(
                    transform.convert(values, check_budget=check_budget), item["ml_input"],
                    check_budget=check_budget)):
            raise DetectionError("prepared_input_incomplete")
        if by_index is not None:
            left, right = ((start - schedule["anchor_utc"]) // unit,
                (end - schedule["anchor_utc"] - timedelta(microseconds=1)) // unit)
            if detector["accelerated"] and detector["step_seconds"] < attempt["plan"]["duration"]:
                if item["evidence"]["base_closures"] != [by_index[i] for i in range(left, right + 1)]:
                    raise DetectionError("prepared_input_identity_mismatch")
            elif item["point_id"] != by_index[left]:
                raise DetectionError("prepared_input_identity_mismatch")
        rows.append(values)
    # This is the same fixed transform used by preparation, not an arbitrary
    # affine that merely agrees with a corrupted ml_input field.
    if Affine.fit(rows, check_budget=check_budget) != transform:
        raise DetectionError("prepared_input_transform_mismatch")
    return online
