"""UTC-only bounded warmup planning."""

from datetime import timedelta


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
