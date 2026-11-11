"""Storage-independent statistics and interpretation of aggregate features."""

import math
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, localcontext

from .rules import CompiledRule, FeatureResult, evaluate_expression, scalar_key


@dataclass(frozen=True)
class FeatureStatistics:
    value: int | float | Decimal | list | None
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


def interpret_result(statistics: AggregateStatistics, compiled: CompiledRule, duration: int = 1) -> dict:
    results: dict[str, FeatureResult] = {}
    for feature in compiled.calculation_features:
        if feature.op == "derive":
            result = evaluate_expression(feature.expr, results, duration, compiled.plan is not None)
        else:
            raw = statistics.features[feature.key]
            value = raw.value
            if value is not None and not raw.invalid_count:
                if feature.op in {"median", "percentile"}:
                    values = sorted(value)
                    with localcontext() as context:
                        context.prec = 100
                        position = Decimal(str(feature.percentile if feature.op == "percentile" else .5)) * (len(values) - 1)
                        index = int(position)
                        value = values[index]
                        if position != index:
                            value = Decimal(str(value)) + (Decimal(str(values[index + 1])) - Decimal(str(value))) * (position - index)
                elif feature.op in {"mode_count", "mode_share"}:
                    ecs_type = compiled.fields[feature.field].type
                    frequency = max(Counter(scalar_key(item, ecs_type) for item in value).values())
                    value = frequency if feature.op == "mode_count" else Decimal(frequency) / len(value)
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
            "features": [{"key": feature.key, **results[feature.key].model_dump()}
                         for feature in compiled.rule.features]}
