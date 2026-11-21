from uuid import UUID

from sqlalchemy import DateTime, Float, and_, case, cast, func, literal, select, true
from sqlalchemy.orm import Session

from ...models.detectors import Finding
from ...schemas.findings import (
    FindingDetector,
    FindingFilters,
    FindingListQuery,
    FindingPage,
)


def _date(key: str):
    value = Finding.data[key].as_string()
    return case(
        (
            and_(
                value.op("~")(r"(Z|[+-][0-9]{2}:[0-9]{2})$"),
                func.pg_input_is_valid(value, "timestamp with time zone"),
            ),
            cast(value, DateTime(timezone=True)),
        ),
    )


def _value(key: str):
    return cast(Finding.data[key].as_string(), Float)


window_start = _date("start")
detector_name = func.nullif(Finding.data["detector_name"].as_string(), "")
time_basis = case(
    (Finding.data["time_basis"].as_string() == "event_time", "event_time"),
    (
        Finding.data["time_basis"].as_string() == "backend_processed_at_historical",
        "backend_processed_at_historical",
    ),
    else_="availability_at",
)
grade = _value("grade")
confidence = _value("confidence")
ordering = (window_start.desc(), Finding.id.desc())


def _predicates(filters: FindingFilters):
    yield window_start >= filters.timestamp_from
    yield window_start < filters.timestamp_to
    if filters.detector_id is not None:
        yield Finding.detector_id == filters.detector_id
    for value, lower, upper in (
        (grade, filters.grade_min, filters.grade_max),
        (confidence, filters.confidence_min, filters.confidence_max),
    ):
        if lower is not None:
            yield value >= lower
        if upper is not None:
            yield value <= upper


def _page_offset(query: FindingListQuery, predicates):
    if query.anchor_id is None:
        return literal(query.offset)
    ranked = (
        select(Finding.id, func.row_number().over(order_by=ordering).label("position"))
        .where(*predicates)
        .subquery()
    )
    return (
        select(((ranked.c.position - 1) // query.limit) * query.limit)
        .where(ranked.c.id == query.anchor_id)
        .scalar_subquery()
    )


class SqlAlchemyFindingRepository:
    def __init__(self, session: Session):
        self.session = session

    def page(self, query: FindingListQuery) -> FindingPage | None:
        predicates = tuple(_predicates(query))
        total = select(func.count()).select_from(Finding).where(*predicates).scalar_subquery()
        metadata = select(
            total.label("total"), _page_offset(query, predicates).label("page_offset")
        ).cte("findings_page")
        # Resolve the anchor and read its page in the same statement snapshot.
        items = (
            select(
                Finding.id,
                Finding.detector_id,
                detector_name.label("detector_name"),
                Finding.data["source_name"].as_string().label("source_name"),
                window_start.label("window_start"),
                _date("end").label("window_end"),
                time_basis.label("time_basis"),
                grade.label("grade"),
                confidence.label("confidence"),
                Finding.late,
                func.coalesce(Finding.data["accelerated_used"].as_boolean(), False).label(
                    "accelerated_used"
                ),
            )
            .where(*predicates)
            .order_by(*ordering)
            .limit(query.limit)
            .offset(func.coalesce(select(metadata.c.page_offset).scalar_subquery(), 0))
            .subquery("findings_items")
        )
        statement = (
            select(metadata.c.total, metadata.c.page_offset, *items.c)
            .select_from(metadata.outerjoin(items, true()))
            .order_by(items.c.window_start.desc(), items.c.id.desc())
        )
        rows = self.session.execute(statement).mappings().all()
        offset = rows[0]["page_offset"]
        if offset is None:
            return None
        return FindingPage(
            items=[dict(row) for row in rows if row["id"] is not None],
            total=rows[0]["total"],
            limit=query.limit,
            offset=offset,
        )

    def detectors(self) -> list[FindingDetector]:
        latest = (
            select(Finding.detector_id, detector_name.label("detector_name"))
            .distinct(Finding.detector_id)
            .order_by(
                Finding.detector_id,
                detector_name.is_(None),
                window_start.desc().nulls_last(),
                Finding.id.desc(),
            )
            .subquery()
        )
        rows = self.session.execute(
            select(latest).order_by(latest.c.detector_name, latest.c.detector_id)
        ).mappings()
        return [FindingDetector.model_validate(dict(row)) for row in rows]

    def snapshot(self, finding_id: UUID) -> tuple[UUID, bool, dict] | None:
        row = self.session.execute(
            select(Finding.detector_id, Finding.late, Finding.data).where(Finding.id == finding_id)
        ).one_or_none()
        return tuple(row) if row is not None else None
