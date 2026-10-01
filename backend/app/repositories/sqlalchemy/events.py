from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    DateTime,
    Numeric,
    String,
    and_,
    case,
    cast,
    column,
    exists,
    func,
    literal,
    or_,
    select,
)
from sqlalchemy.dialects.postgresql import ARRAY, INET, JSONB
from sqlalchemy.orm import Session

from ...ecs import EcsField
from ...models import ExternalConnection, ParsedLog, Source


def _path(field: EcsField):
    return ParsedLog.ecs_data[tuple(field.name.split("."))]


def _text(path):
    return path.op("#>>", return_type=String())(literal([], type_=ARRAY(String)))


def _timestamp_expression():
    value = ParsedLog.ecs_data["@timestamp"]
    text_value = value.as_string()
    valid = and_(
        func.jsonb_typeof(value) == "string",
        text_value.op("~")(r"(Z|[+-][0-9]{2}:?[0-9]{2})$"),
        func.pg_input_is_valid(text_value, "timestamp with time zone"),
    )
    return case((valid, cast(text_value, DateTime(timezone=True))), else_=None)


def _nested(name: str, value):
    nested = value
    for part in reversed(name.split(".")):
        nested = {part: nested}
    return nested


def _is_valid_value(value, field_type: str):
    json_type = func.jsonb_typeof(value)
    if field_type in {"keyword", "constant_keyword", "wildcard", "text", "match_only_text"}:
        return json_type == "string"
    if field_type in {
        "integer",
        "long",
        "float",
        "double",
        "scaled_float",
        "short",
        "byte",
        "unsigned_long",
        "half_float",
    }:
        return json_type == "number"
    if field_type == "boolean":
        return json_type == "boolean"
    if field_type in {"date", "ip"}:
        target_type = "timestamp with time zone" if field_type == "date" else "inet"
        text_value = _text(value)
        return and_(json_type == "string", func.pg_input_is_valid(text_value, target_type))
    return literal(False)


def _typed_date_or_ip(value, field_type: str):
    text_value = _text(value)
    valid = _is_valid_value(value, field_type)
    if field_type == "date":
        return case((valid, cast(text_value, DateTime(timezone=True))), else_=None)
    return case((valid, cast(text_value, INET())), else_=None)


def _filter_clause(field: EcsField, operator: str, value):
    path = _path(field)
    if operator == "exists":
        return path.is_not(None)
    if operator == "not_exists":
        return path.is_(None)
    is_array = field.is_array
    if operator in {"eq", "neq", "in"}:
        values = value if operator == "in" else [value]
        if field.type in {"date", "ip"}:
            target_type = DateTime(timezone=True) if field.type == "date" else INET()
            targets = [cast(literal(item), target_type) for item in values]
            if is_array:
                safe_array = case(
                    (func.jsonb_typeof(path) == "array", path),
                    else_=literal([], type_=JSONB),
                )
                members = (
                    func.jsonb_array_elements(safe_array)
                    .table_valued(column("value", JSONB))
                    .alias("typed_event_filter_values")
                )
                invalid_values = exists(
                    select(1)
                    .select_from(members)
                    .where(~_is_valid_value(members.c.value, field.type))
                )
                candidate = _typed_date_or_ip(members.c.value, field.type)
                matched = exists(select(1).select_from(members).where(candidate.in_(targets)))
                valid_parent = and_(func.jsonb_typeof(path) == "array", ~invalid_values)
                return (
                    and_(valid_parent, ~matched)
                    if operator == "neq"
                    else and_(valid_parent, matched)
                )
            actual = _typed_date_or_ip(path, field.type)
            matched = actual.in_(targets)
            return (
                and_(actual.is_not(None), ~matched)
                if operator == "neq"
                else and_(actual.is_not(None), matched)
            )
        if is_array:
            safe_array = case(
                (func.jsonb_typeof(path) == "array", path),
                else_=literal([], type_=JSONB),
            )
            members = (
                func.jsonb_array_elements(safe_array)
                .table_valued(column("value", JSONB))
                .alias("event_filter_values")
            )
            candidate_valid = _is_valid_value(members.c.value, field.type)
            invalid_values = exists(select(1).select_from(members).where(~candidate_valid))
            valid_parent = and_(func.jsonb_typeof(path) == "array", ~invalid_values)
            matched = or_(
                *(ParsedLog.ecs_data.contains(_nested(field.name, [item])) for item in values)
            )
            return (
                and_(valid_parent, matched) if operator != "neq" else and_(valid_parent, ~matched)
            )
        matches = or_(*(ParsedLog.ecs_data.contains(_nested(field.name, item)) for item in values))
        well_formed = _is_valid_value(path, field.type)
        return and_(well_formed, ~matches) if operator == "neq" else and_(well_formed, matches)
    if operator in {"contains", "starts_with", "ends_with"}:
        if is_array and field.type not in {
            "keyword",
            "constant_keyword",
            "wildcard",
            "text",
            "match_only_text",
        }:
            return _filter_clause(field, "eq", value)
        text_value = _text(path)
        value = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        pattern = {"contains": f"%{value}%", "starts_with": f"{value}%", "ends_with": f"%{value}"}[
            operator
        ]
        if is_array:
            members = (
                func.jsonb_array_elements(
                    case((func.jsonb_typeof(path) == "array", path), else_=literal([], type_=JSONB))
                )
                .table_valued(column("value", JSONB))
                .alias("event_string_members")
            )
            valid_values = _is_valid_value(members.c.value, field.type)
            invalid_values = exists(select(1).select_from(members).where(~valid_values))
            return and_(
                func.jsonb_typeof(path) == "array",
                ~invalid_values,
                exists(
                    select(1)
                    .select_from(members)
                    .where(valid_values, _text(members.c.value).like(pattern, escape="\\"))
                ),
            )
        return and_(func.jsonb_typeof(path) == "string", text_value.like(pattern, escape="\\"))
    if field.type in {"integer", "long", "float", "double", "scaled_float", "short", "byte"}:
        text_value = _text(path)
        valid = and_(
            func.jsonb_typeof(path) == "number", func.pg_input_is_valid(text_value, "numeric")
        )
        actual = case((valid, cast(text_value, Numeric)), else_=None)
        expected = Numeric()
    else:
        text_value = _text(path)
        valid = and_(
            func.jsonb_typeof(path) == "string",
            func.pg_input_is_valid(text_value, "timestamp with time zone"),
        )
        actual = case((valid, cast(text_value, DateTime(timezone=True))), else_=None)
        expected = DateTime(timezone=True)
    target = literal(value, type_=expected)
    comparison = {
        "gt": actual > target,
        "gte": actual >= target,
        "lt": actual < target,
        "lte": actual <= target,
    }[operator]
    return and_(actual.is_not(None), comparison)


class SqlAlchemyEventQueryRepository:
    def __init__(self, session: Session):
        self.session = session

    def source_type(self, source_id: UUID):
        row = self.session.execute(
            select(Source.source_type, Source.is_enabled).where(Source.id == source_id)
        ).one_or_none()
        return row

    def external_source(self, source_id: UUID):
        return self.session.execute(
            select(Source, ExternalConnection)
            .join(ExternalConnection, Source.external_connection_id == ExternalConnection.id)
            .where(Source.id == source_id, Source.source_type == "external")
        ).one_or_none()

    def snapshot_boundary(self) -> datetime:
        return self.session.scalar(select(func.current_timestamp()))

    def search(
        self,
        source_id,
        filters,
        timestamp_from,
        timestamp_to,
        sort,
        after,
        snapshot_boundary,
        limit,
    ):
        event_time = _timestamp_expression()
        statement = select(ParsedLog.id, event_time, ParsedLog.ecs_data).where(
            ParsedLog.source_id == source_id,
            ParsedLog.created_at <= snapshot_boundary,
            event_time.is_not(None),
        )
        if timestamp_from is not None:
            statement = statement.where(event_time >= timestamp_from)
        if timestamp_to is not None:
            statement = statement.where(event_time < timestamp_to)
        for field, operator, value in filters:
            statement = statement.where(_filter_clause(field, operator, value))
        ordering = event_time.desc() if sort == "desc" else event_time.asc()
        id_order = ParsedLog.id.desc() if sort == "desc" else ParsedLog.id.asc()
        if after is not None:
            comparison = "<" if sort == "desc" else ">"
            statement = statement.where(
                (event_time < after[0]) | ((event_time == after[0]) & (ParsedLog.id < after[1]))
                if comparison == "<"
                else (event_time > after[0])
                | ((event_time == after[0]) & (ParsedLog.id > after[1]))
            )
        rows = self.session.execute(statement.order_by(ordering, id_order).limit(limit))
        return list(rows)

    def get(self, source_id: UUID, event_id: UUID):
        return self.session.scalar(
            select(ParsedLog).where(ParsedLog.source_id == source_id, ParsedLog.id == event_id)
        )
