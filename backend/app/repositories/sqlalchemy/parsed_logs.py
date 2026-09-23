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
    delete,
    exists,
    func,
    literal,
    select,
    tuple_,
)
from sqlalchemy.dialects.postgresql import ARRAY, INET, JSONB
from sqlalchemy.orm import Session, load_only

from ...ecs import ValidatedEcsFilter
from ...models import ParsedLog


def _nested_json_path(field_name: str, value):
    nested = value
    for part in reversed(field_name.split(".")):
        nested = {part: nested}
    return nested


def _json_filter_value(field: ValidatedEcsFilter):
    value = field.value
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _array_membership(field: ValidatedEcsFilter):
    path = ParsedLog.ecs_data[tuple(field.field.name.split("."))]
    well_formed = _array_is_well_formed(field, path)
    if field.field.type in {"date", "ip"}:
        safe_array = case(
            (func.jsonb_typeof(path) == "array", path),
            else_=literal([], type_=JSONB),
        )
        values = func.jsonb_array_elements(safe_array).table_valued(
            column("value", JSONB)
        ).alias("ecs_typed_values")
        text_value = values.c.value.op("#>>", return_type=String())(
            literal([], type_=ARRAY(String))
        )
        target_type = "timestamp with time zone" if field.field.type == "date" else "inet"
        valid = and_(
            func.jsonb_typeof(values.c.value) == "string",
            func.pg_input_is_valid(text_value, target_type),
        )
        sql_type = DateTime(timezone=True) if field.field.type == "date" else INET()
        typed_value = case((valid, cast(text_value, sql_type)), else_=None)
        expected = literal(field.value, type_=sql_type)
        matches = exists(
            select(1).select_from(values).where(typed_value == expected)
        )
        return and_(well_formed, matches)
    return and_(
        well_formed,
        ParsedLog.ecs_data.contains(
            _nested_json_path(field.field.name, [_json_filter_value(field)])
        ),
    )


def _array_is_well_formed(field: ValidatedEcsFilter, path):
    safe_array = case(
        (func.jsonb_typeof(path) == "array", path),
        else_=literal([], type_=JSONB),
    )
    values = func.jsonb_array_elements(safe_array).table_valued(
        column("value", JSONB)
    ).alias("ecs_checked_values")
    field_type = field.field.type
    if field_type in {"keyword", "constant_keyword", "wildcard", "text", "match_only_text", "date", "ip"}:
        valid_element = func.jsonb_typeof(values.c.value) == "string"
        if field_type in {"date", "ip"}:
            valid_input_type = "timestamp with time zone" if field_type == "date" else "inet"
            valid_element = and_(
                valid_element,
                func.pg_input_is_valid(
                    values.c.value.op("#>>", return_type=String())(
                        literal([], type_=ARRAY(String))
                    ),
                    valid_input_type,
                ),
            )
    elif field_type in {"integer", "long", "float", "double", "scaled_float", "short", "byte", "half_float", "unsigned_long"}:
        valid_element = func.jsonb_typeof(values.c.value) == "number"
    elif field_type == "boolean":
        valid_element = func.jsonb_typeof(values.c.value) == "boolean"
    else:
        return func.jsonb_typeof(path) == "array"
    has_invalid_element = exists(
        select(1).select_from(values).where(~valid_element)
    )
    return and_(func.jsonb_typeof(path) == "array", ~has_invalid_element)


def _contains_filter(field: ValidatedEcsFilter):
    path = ParsedLog.ecs_data[tuple(field.field.name.split("."))]
    if field.field.is_array and field.field.type not in {
        "keyword", "constant_keyword", "wildcard", "text", "match_only_text"
    }:
        return _array_membership(field)
    pattern = str(field.value).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    if field.field.is_array:
        safe_array = case(
            (func.jsonb_typeof(path) == "array", path),
            else_=literal([], type_=JSONB),
        )
        values = (
            func.jsonb_array_elements_text(safe_array)
            .table_valued("value")
            .alias("ecs_array_values")
        )
        return and_(_array_is_well_formed(field, path), exists(
            select(1)
            .select_from(values)
            .where(values.c.value.ilike(f"%{pattern}%", escape="\\"))
        ))
    return and_(
        func.jsonb_typeof(path) == "string",
        path.as_string().ilike(f"%{pattern}%", escape="\\"),
    )


def _filter_expression(field: ValidatedEcsFilter):
    path = ParsedLog.ecs_data[tuple(field.field.name.split("."))]
    operator = field.operator
    if operator == "exists":
        return path.is_not(None)
    if operator == "not_exists":
        return path.is_(None)
    if operator == "contains":
        return _contains_filter(field)
    if field.field.is_array:
        equal = _array_membership(field)
        if operator == "eq":
            return equal
        if operator == "neq":
            return and_(_array_is_well_formed(field, path), ~equal)
        raise ValueError(f"Unsupported ECS array operator: {operator}")
    if operator in {"eq", "neq"} and field.field.type not in {"date", "ip"} | {
        "integer", "long", "float", "double", "scaled_float", "short", "byte"
    }:
        expected_value = [field.value] if field.field.is_array else field.value
        equal = ParsedLog.ecs_data.contains(_nested_json_path(field.field.name, expected_value))
        if operator == "eq":
            return equal
        return and_(path.is_not(None), ~equal)
    text_value = path.as_string()
    if field.field.type in {"integer", "long", "float", "double", "scaled_float", "short", "byte"}:
        numeric_value = case(
            (
                and_(
                    func.jsonb_typeof(path) == "number",
                    func.pg_input_is_valid(text_value, "numeric"),
                ),
                cast(text_value, Numeric),
            ),
            else_=None,
        )
        expected = Numeric()
    elif field.field.type == "date":
        numeric_value = case(
            (
                and_(
                    func.jsonb_typeof(path) == "string",
                    func.pg_input_is_valid(text_value, "timestamp with time zone"),
                ),
                cast(text_value, DateTime(timezone=True)),
            ),
            else_=None,
        )
        expected = DateTime(timezone=True)
    else:
        numeric_value = case(
            (
                and_(
                    func.jsonb_typeof(path) == "string",
                    func.pg_input_is_valid(text_value, "inet"),
                ),
                cast(text_value, INET),
            ),
            else_=None,
        )
        expected = INET()
    right = literal(field.value, type_=expected)
    comparison = {
        "eq": numeric_value == right,
        "neq": numeric_value != right,
        "gt": numeric_value > right,
        "gte": numeric_value >= right,
        "lt": numeric_value < right,
        "lte": numeric_value <= right,
    }[operator]
    return and_(numeric_value.is_not(None), comparison)


class SqlAlchemyParsedLogRepository:
    def __init__(self, session: Session):
        self.session = session

    def snapshot_boundary(self) -> datetime:
        return self.session.scalar(select(func.current_timestamp()))

    def search(
        self,
        *,
        source_ids: list[UUID] | None,
        connection_ids: list[UUID] | None,
        normalizer_ids: list[UUID] | None,
        kafka_topics: list[str] | None,
        kafka_partitions: list[int] | None,
        raw_query: str | None,
        processed_from: datetime | None,
        processed_to: datetime | None,
        collected_from: datetime | None,
        collected_to: datetime | None,
        ecs_filters: list[ValidatedEcsFilter],
        after: tuple[datetime, UUID] | None,
        snapshot_boundary: datetime,
        limit: int,
    ) -> list[tuple[ParsedLog, str, str | None]]:
        statement = select(
            ParsedLog,
            func.left(ParsedLog.raw, 500).label("raw_preview"),
            ParsedLog.ecs_data["ecs"]["version"].as_string().label("ecs_version"),
        ).options(
            load_only(
                ParsedLog.id, ParsedLog.source_id, ParsedLog.connection_id,
                ParsedLog.normalizer_id, ParsedLog.source_name, ParsedLog.connection_name,
                ParsedLog.normalizer_name, ParsedLog.normalizer_version, ParsedLog.kafka_topic,
                ParsedLog.kafka_partition, ParsedLog.kafka_offset,
                ParsedLog.fluent_bit_collected_at, ParsedLog.backend_received_at,
                ParsedLog.backend_processed_at, ParsedLog.created_at,
            )
        )
        for filter_column, values in (
            (ParsedLog.source_id, source_ids),
            (ParsedLog.connection_id, connection_ids),
            (ParsedLog.normalizer_id, normalizer_ids),
            (ParsedLog.kafka_topic, kafka_topics),
            (ParsedLog.kafka_partition, kafka_partitions),
        ):
            if values is not None:
                statement = statement.where(filter_column.in_(values))
        if raw_query is not None:
            escaped = raw_query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            statement = statement.where(ParsedLog.raw.ilike(f"%{escaped}%", escape="\\"))
        for time_column, lower, upper in (
            (ParsedLog.backend_processed_at, processed_from, processed_to),
            (ParsedLog.fluent_bit_collected_at, collected_from, collected_to),
        ):
            if lower is not None:
                statement = statement.where(time_column >= lower)
            if upper is not None:
                statement = statement.where(time_column < upper)
        for ecs_filter in ecs_filters:
            statement = statement.where(_filter_expression(ecs_filter))
        statement = statement.where(ParsedLog.created_at <= snapshot_boundary)
        if after is not None:
            statement = statement.where(
                tuple_(ParsedLog.backend_processed_at, ParsedLog.id) < tuple_(after[0], after[1])
            )
        statement = statement.order_by(
            ParsedLog.backend_processed_at.desc(), ParsedLog.id.desc()
        ).limit(limit)
        return [
            (log, preview or "", version)
            for log, preview, version in self.session.execute(statement)
        ]

    def find_by_id(self, log_id: UUID) -> ParsedLog | None:
        return self.session.get(ParsedLog, log_id)

    def delete(self, log: ParsedLog) -> None:
        self.session.delete(log)

    def delete_many(
        self, source_id: UUID | None, processed_from: datetime | None,
        processed_to: datetime | None,
    ) -> int:
        statement = delete(ParsedLog)
        if source_id is not None:
            statement = statement.where(ParsedLog.source_id == source_id)
        if processed_from is not None:
            statement = statement.where(ParsedLog.backend_processed_at >= processed_from)
        if processed_to is not None:
            statement = statement.where(ParsedLog.backend_processed_at < processed_to)
        result = self.session.execute(statement)
        return result.rowcount or 0
