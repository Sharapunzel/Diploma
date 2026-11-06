"""Fixed affine transform with explicit float32 error/collision checks.

Original facts are never mutated. Callers persist the transform and bounded
history alongside a model checkpoint; no adaptive rescaling or clipping occurs.
"""

import struct
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal, localcontext


class NumericQualityError(ValueError):
    def __init__(self, reason: str, coordinate: int):
        self.reason = reason
        self.coordinate = coordinate
        super().__init__(f"{reason}: coordinate {coordinate}")


def _unlimited_budget():
    pass


def exact(value: float | Decimal, coordinate: int) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise NumericQualityError("non_numeric", coordinate)
    result = Decimal(str(value))
    if not result.is_finite():
        raise NumericQualityError("non_finite", coordinate)
    return result


@dataclass(frozen=True)
class Affine:
    center: tuple[Decimal, ...]
    scale: tuple[Decimal, ...]

    @classmethod
    def fit(cls, history, *, check_budget: Callable[[], None] | None = None):
        """Fit without mutating facts; propagate the caller's budget exception.

        Check each coordinate and index operation, including collision detection.
        A per-coordinate float32 index keeps fitting linear in the input size.
        """
        check = _unlimited_budget if check_budget is None else check_budget
        check()
        if not history or not history[0]:
            raise NumericQualityError("empty_history", 0)
        width = len(history[0])
        rows = []
        for row in history:
            check()
            if len(row) != width:
                raise NumericQualityError("dimension_mismatch", 0)
            values = []
            for i, value in enumerate(row):
                check()
                values.append(exact(value, i))
            rows.append(tuple(values))
        def column(i):
            for row in rows:
                check()
                yield row[i]
        centers = tuple(min(column(i)) for i in range(width))
        # Covers the entire finite Python binary64 range and adjacent int64s.
        with localcontext() as context:
            context.prec = 2048
            scales = tuple(max(column(i)) - centers[i] or Decimal(1)
                           for i in range(width))
        transform = cls(centers, scales)
        seen = []
        for _ in range(width):
            check()
            seen.append({})
        for row in rows:
            converted = transform.convert(row, check_budget=check)
            for i, value in enumerate(converted):
                check()
                previous = seen[i].get(value)
                if previous is not None and previous != row[i]:
                    raise NumericQualityError("float32_collision", i)
                seen[i][value] = row[i]
        return transform

    def convert(self, values, previous=(), *, check_budget: Callable[[], None] | None = None):
        """Convert with budget checks for each coordinate and prior sample."""
        check = _unlimited_budget if check_budget is None else check_budget
        check()
        if len(values) != len(self.center):
            raise NumericQualityError("dimension_mismatch", 0)
        result = []
        for i, raw in enumerate(values):
            check()
            value = exact(raw, i)
            with localcontext() as context:
                context.prec = 2048
                normalized = (value - self.center[i]) / self.scale[i]
                try:
                    converted = struct.unpack("!f", struct.pack("!f", float(normalized)))[0]
                    actual = Decimal.from_float(converted)
                except (OverflowError, struct.error):
                    raise NumericQualityError("float32_overflow", i) from None
                if not actual.is_finite():
                    raise NumericQualityError("float32_overflow", i)
                if normalized and not actual:
                    raise NumericQualityError("float32_underflow", i)
                if abs(actual - normalized) > Decimal("1e-6") * max(1, abs(normalized)):
                    raise NumericQualityError("float32_error", i)
            for old_values, old_ml in previous:
                check()
                if exact(old_values[i], i) != value and float(old_ml[i]) == converted:
                    raise NumericQualityError("float32_collision", i)
            result.append(converted)
        return tuple(result)

    def inverse(self, values):
        if len(values) != len(self.center):
            raise NumericQualityError("dimension_mismatch", 0)
        with localcontext() as context:
            context.prec = 2048
            return tuple(exact(v, i) * self.scale[i] + self.center[i]
                         for i, v in enumerate(values))
