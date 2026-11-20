"""Event-time invariants shared by snapshot, scheduling and history protection.

No database, HTTP or model runtime is used by this module. A deadline belongs
to the whole operation; individual reads may only consume its remainder.
"""

import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, timedelta

from .rules import DATE_PATTERN, date_utc

MAX_EVENTS = 100_000
MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024
MAX_MEMORY_BYTES = 256 * 1024 * 1024
MAX_SCAN_PAGE = 500
MAX_SCAN_VISITS = 5_000
MAX_MODEL_CANDIDATES = 4096


class EventTimeRangeError(ValueError):
    pass


class EventTimeBudgetError(ValueError):
    def __init__(self, factor):
        self.factor = factor
        super().__init__(factor)


@dataclass
class OperationBudget:
    seconds: float
    clock: Callable[[], float] | None = None
    started: float | None = None

    def __post_init__(self):
        self.clock = self.clock or time.monotonic
        self.started = self.clock() if self.started is None else self.started
        self.deadline = self.started + self.seconds

    def remaining(self):
        return max(0.0, self.deadline - self.clock())

    def milliseconds(self):
        remaining = self.deadline - self.clock()
        if remaining <= 0:
            raise EventTimeBudgetError("operation_seconds")
        return max(1, int(remaining * 1000))

    def check(self):
        self.milliseconds()


def event_timestamp(document, diagnostics, proven, reference_at):
    """Classify retained processing provenance, never reconstruct normalization."""
    if not proven:
        return None, "event_time_unknown_provenance"
    if any(item.get("code") == "event_time_fallback" for item in diagnostics):
        return None, "event_time_fallback"
    raw = document.get("@timestamp")
    if raw is None:
        return None, "event_time_missing"
    if not isinstance(raw, str) or re.fullmatch(DATE_PATTERN, raw) is None:
        return None, "event_time_invalid"
    try:
        parsed = date_utc(raw)
    except (ValueError, TypeError, OverflowError, AttributeError):
        return None, "event_time_invalid"
    if parsed > reference_at:
        return None, "event_time_future"
    return parsed, None


def index_at(anchor, duration, at):
    return (at.astimezone(UTC) - anchor.astimezone(UTC)) // timedelta(seconds=duration)


def online_start(anchor, duration, bootstrap_at):
    return max(0, index_at(anchor, duration, bootstrap_at))


def bounds(anchor, duration, index):
    try:
        start = anchor.astimezone(UTC) + timedelta(seconds=duration * index)
        return start, start + timedelta(seconds=duration)
    except OverflowError as error:
        raise EventTimeRangeError("schedule_datetime_range") from error


def close_deadline(end, duration):
    try:
        return end + timedelta(seconds=min(duration, 300))
    except OverflowError as error:
        raise EventTimeRangeError("schedule_datetime_range") from error


def warmup_indices(last_index, duration, step, sample_size, output_after, accelerated):
    """The same bounded plan is used for start and cleanup protection.

    cutoff is the end of the last available canonical window, not wall time.
    Offsets are seconds relative to the aggregate's anchor.
    """
    required = max(2 * sample_size, output_after + 1)
    if not accelerated:
        step = duration
        count = required
    else:
        if type(step) is not int or not 0 < step <= duration:
            raise ValueError("invalid_warmup_step")
        count = max(required, 1 + (7 * duration + step - 1) // step)
    if count > MAX_MODEL_CANDIDATES:
        raise EventTimeBudgetError("candidate_windows")
    cutoff = (last_index + 1) * duration
    first = cutoff - duration - (count - 1) * step
    starts = tuple(first + n * step for n in range(count) if first + n * step >= 0)
    base_first = starts[0] // duration if starts else None
    return {"required": required, "count": count, "starts": starts,
            "base_first": base_first, "base_last": last_index,
            "cutoff_seconds": cutoff, "complete": len(starts) == count}


def suffix_size(ordered_indices, earliest):
    """Count payloads, rather than assuming a dense range of window indices."""
    return sum(index >= earliest for index in ordered_indices)
