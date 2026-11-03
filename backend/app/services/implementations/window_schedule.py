"""UTC arithmetic for the non-overlapping working windows of an aggregate."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

MAX_DURATION_SECONDS = 7 * 24 * 60 * 60


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("window times must have a timezone")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class WorkingWindow:
    start: datetime
    end: datetime

    @property
    def scheduled_at(self) -> datetime:
        return self.end

    def contains(self, availability_at: datetime) -> bool:
        return self.start <= _utc(availability_at) < self.end


@dataclass(frozen=True, slots=True)
class WindowSchedule:
    anchor_utc: datetime
    duration_seconds: int

    def __post_init__(self) -> None:
        if type(self.duration_seconds) is not int or not 1 <= self.duration_seconds <= MAX_DURATION_SECONDS:
            raise ValueError("duration must be an integer between 1 and 604800 seconds")
        object.__setattr__(self, "anchor_utc", _utc(self.anchor_utc))

    def window(self, index: int) -> WorkingWindow:
        if type(index) is not int or index < 0:
            raise ValueError("window index must be a non-negative integer")
        start = self.anchor_utc + timedelta(seconds=index * self.duration_seconds)
        return WorkingWindow(start, start + timedelta(seconds=self.duration_seconds))

    def index_at(self, availability_at: datetime) -> int:
        delta = _utc(availability_at) - self.anchor_utc
        if delta < timedelta(0):
            raise ValueError("availability precedes the working schedule")
        return delta // timedelta(seconds=self.duration_seconds)

    def next_due(self, index: int, database_now: datetime) -> WorkingWindow | None:
        candidate = self.window(index)
        return candidate if _utc(database_now) >= candidate.scheduled_at else None
