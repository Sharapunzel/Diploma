"""UTC-only bounded warmup planning."""

from datetime import UTC, timedelta

from .contracts import DetectionError


def warmup_plan(anchor, duration, now, minimum, step, accelerated=True):
    anchor, now = anchor.astimezone(UTC), now.astimezone(UTC)
    if type(step) is not int or not 0 < step <= duration:
        raise DetectionError("invalid_warmup_step")
    count = max(minimum, 1 + (7 * duration + step - 1) // step) if accelerated else minimum
    if count > 4096:
        raise DetectionError("resource_budget:warmup_windows")
    try:
        cutoff_index = (now - anchor) // timedelta(seconds=duration)
        if cutoff_index < 1:
            raise DetectionError("insufficient_history")
        cutoff = anchor + timedelta(seconds=cutoff_index * duration)
        start = cutoff - timedelta(seconds=duration + (count - 1) * step)
        if start < anchor:
            raise DetectionError("insufficient_history")
    except DetectionError:
        raise
    except (OverflowError, ValueError) as error:
        raise DetectionError("history_datetime_range") from error
    return {"cutoff": cutoff.isoformat(), "first_start": start.isoformat(),
            "count": count, "step": step, "duration": duration,
            "coverage_seconds": duration + (count - 1) * step,
            "evaluation_start_index": cutoff_index}


def plan_windows(plan):
    from datetime import datetime

    start = datetime.fromisoformat(plan["first_start"])
    for i in range(plan["count"]):
        left = start + timedelta(seconds=i * plan["step"])
        yield left, left + timedelta(seconds=plan["duration"])


def warmup_summary(attempt, detector):
    """Derive diagnostics solely from durable immutable inputs, never live events."""
    from datetime import datetime

    inputs = attempt["inputs"]
    eligible = [item for item in inputs if "ml_input" in item and not item.get("quality_reason")
                and item["completeness"] == "complete"]
    def coverage(items):
        total, previous = 0, None
        for left, right in sorted((datetime.fromisoformat(i["start"]),
                                   datetime.fromisoformat(i["end"])) for i in items):
            total += max(0, (right - max(left, previous or left)).total_seconds())
            previous = max(previous or right, right)
        return total
    return {"planned_windows": attempt["plan"]["count"], "materialized_windows": len(inputs),
            "feature_valid_windows": sum("exact" in i and not i.get("quality_reason") for i in inputs),
            "eligible_windows": len(eligible), "ineligible_windows": len(inputs) - len(eligible),
            "materialized_coverage_seconds": coverage(inputs),
            "eligible_coverage_seconds": coverage(eligible),
            "readiness": detector["state"], "reason": detector["reason"]}
