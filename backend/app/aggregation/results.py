"""Storage-independent statistics and interpretation of aggregate features."""

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from .rules import CompiledRule, FeatureResult, evaluate_expression


@dataclass(frozen=True)
class FeatureStatistics:
    value: int | float | Decimal | None
    valid_count: int
    invalid_count: int


@dataclass(frozen=True)
class AggregateStatistics:
    observed_rows: int
    event_count: int
    observed_width: int
    event_time_min: datetime | None
    event_time_max: datetime | None
    features: Mapping[str, FeatureStatistics]


def interpret_result(statistics: AggregateStatistics, compiled: CompiledRule) -> dict:
    results: dict[str, FeatureResult] = {}
    for feature in compiled.rule.features:
        if feature.op == "derive":
            result = evaluate_expression(feature.expr, results)
        else:
            raw = statistics.features[feature.key]
            value = raw.value
            reason = "invalid_input_type" if raw.invalid_count else "no_values" if value is None else None
            if reason:
                value = None
            elif isinstance(value, Decimal):
                value = int(value) if value == value.to_integral_value() else float(value)
            if value is not None:
                try:
                    finite = math.isfinite(value)
                except OverflowError:
                    finite = False
                if not finite:
                    value, reason = None, "non_finite_result"
            result = FeatureResult(value=value, reason=reason, valid_count=raw.valid_count,
                                   invalid_count=raw.invalid_count)
        results[feature.key] = result
    return {"observed_rows": statistics.observed_rows, "event_count": statistics.event_count,
            "observed_width": statistics.observed_width,
            "event_time_min": statistics.event_time_min, "event_time_max": statistics.event_time_max,
            "features": [{"key": k, **v.model_dump()} for k, v in results.items()]}
