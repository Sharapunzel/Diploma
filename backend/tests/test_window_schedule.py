from datetime import UTC, datetime, timedelta, timezone

import pytest

from app.services.implementations.window_schedule import WindowSchedule


@pytest.mark.parametrize("seconds", [1, 137, 604800])
def test_windows_are_half_open_and_due_only_at_end(seconds):
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    schedule = WindowSchedule(anchor, seconds)
    first = schedule.window(0)
    assert schedule.next_due(0, first.end - timedelta(microseconds=1)) is None
    assert schedule.next_due(0, first.end) == first
    assert first.contains(first.start)
    assert first.contains(first.end - timedelta(microseconds=1))
    assert not first.contains(first.end)
    assert schedule.index_at(first.end) == 1
    assert schedule.window(1).start == first.end


def test_timezone_changes_do_not_change_schedule_or_selection():
    anchor = datetime(2026, 3, 29, 0, tzinfo=UTC)
    local = timezone(timedelta(hours=5))
    first = WindowSchedule(anchor, 300)
    second = WindowSchedule(anchor.astimezone(local), 300)
    instant = anchor + timedelta(minutes=12)
    assert first.window(2) == second.window(2)
    assert first.index_at(instant) == second.index_at(instant.astimezone(local)) == 2
    assert first.next_due(2, instant) is None


@pytest.mark.parametrize("duration", [0, -1, 604801, 1.5, True])
def test_invalid_duration_is_rejected(duration):
    with pytest.raises(ValueError):
        WindowSchedule(datetime(2026, 1, 1, tzinfo=UTC), duration)


def test_naive_times_and_before_anchor_are_rejected():
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError):
        WindowSchedule(anchor.replace(tzinfo=None), 1)
    schedule = WindowSchedule(anchor, 1)
    with pytest.raises(ValueError):
        schedule.index_at(anchor - timedelta(microseconds=1))
    with pytest.raises(ValueError):
        schedule.next_due(0, anchor.replace(tzinfo=None))
