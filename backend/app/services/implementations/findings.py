from datetime import UTC, datetime, timedelta
from math import isfinite
from uuid import UUID

from ...core.errors import DomainError
from ...repositories.protocols.findings import FindingRepository
from ...schemas.findings import (
    FindingCard,
    FindingEventRange,
    FindingFeature,
    FindingListQuery,
)


def _date(value: str | None) -> datetime | None:
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is not None:
                return parsed.astimezone(UTC)
        except (ValueError, OverflowError):
            pass
    return None


def _at(values, index):
    return values[index] if isinstance(values, list) and index < len(values) else None


def _exact(value):
    return (
        str(value) if not isinstance(value, bool) and isinstance(value, (str, int, float)) else None
    )


def _features(data):
    for index, key in enumerate(data.get("feature_keys") or []):
        attribution = _at(data.get("attribution"), index)
        yield FindingFeature(
            key=key,
            name=_at(data.get("feature_names"), index) or key,
            actual=_exact(_at(data.get("exact_values"), index)),
            expected=_exact(_at(data.get("expected"), index)),
            attribution=attribution
            if isinstance(attribution, (int, float))
            and not isinstance(attribution, bool)
            and isfinite(attribution)
            and attribution >= 0
            else None,
        )


def _event_range(data, source_id):
    if data.get("event_count") == 0 or data.get("event_range_reason") == "empty_window":
        return None, "empty_window"
    lower, upper = (_date(data.get(key)) for key in ("event_time_min", "event_time_max"))
    if source_id is None or lower is None or upper is None or lower > upper:
        return None, "unavailable"
    try:
        exclusive = upper + timedelta(microseconds=1)
    except OverflowError:
        return None, "unrepresentable"
    return FindingEventRange(timestamp_from=lower, timestamp_to=exclusive), None


class FindingServiceImpl:
    def __init__(self, repository: FindingRepository):
        self.repository = repository

    def page(self, query: FindingListQuery):
        page = self.repository.page(query)
        if page is None:
            raise DomainError("finding_not_found", "Finding not found in the selected period", 404)
        return page

    def detectors(self):
        return self.repository.detectors()

    def card(self, finding_id: UUID) -> FindingCard:
        snapshot = self.repository.snapshot(finding_id)
        if snapshot is None:
            raise DomainError("finding_not_found", "Finding not found", 404)
        detector_id, late, data = snapshot
        source_id = UUID(data["source_id"]) if data.get("source_id") else None
        event_range, reason = _event_range(data, source_id)
        return FindingCard(
            id=finding_id,
            detector_id=detector_id,
            late=late,
            detector_name=data.get("detector_name") or None,
            source_name=data.get("source_name"),
            aggregate_name=data.get("aggregate_name"),
            source_id=source_id,
            window_start=_date(data.get("start")),
            window_end=_date(data.get("end")),
            time_basis=data.get("time_basis") or "availability_at",
            grade=data.get("grade"),
            confidence=data.get("confidence"),
            accelerated_used=data.get("accelerated_used") is True,
            event_range=event_range,
            event_range_reason=reason,
            expected_origin=data.get("expected_origin"),
            features=list(_features(data)),
            aggregation_started_at=_date(data.get("aggregation_started_at")),
            aggregation_completed_at=_date(data.get("aggregation_completed_at")),
            evaluation_started_at=_date(data.get("evaluation_started_at")),
            evaluation_completed_at=_date(data.get("evaluation_completed_at")),
        )
