from __future__ import annotations

import ipaddress
import math
from datetime import UTC, datetime
from typing import Any

from ..ecs import STRING_TYPES, EcsField
from .schema import DateTransform

INTEGER_RANGES = {
    "byte": (-128, 127),
    "short": (-32_768, 32_767),
    "integer": (-2_147_483_648, 2_147_483_647),
    "long": (-9_223_372_036_854_775_808, 9_223_372_036_854_775_807),
}
FLOAT_TYPES = frozenset({"float", "double", "scaled_float"})


class ConversionError(ValueError):
    pass


def date_format_directives(format_text: str) -> set[str] | None:
    directives: set[str] = set()
    index = 0
    while index < len(format_text):
        if format_text[index] == "%":
            index += 1
            if index == len(format_text) or format_text[index] not in "YymdHMSbBz%":
                return None
            if format_text[index] != "%":
                directives.add(format_text[index])
        index += 1
    return directives


def parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ConversionError("timestamp must be a string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ConversionError("timestamp is invalid") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ConversionError("timestamp requires a timezone")
    return parsed


def _date(value: object, transform: DateTransform | None) -> str:
    if not isinstance(value, str):
        raise ConversionError("date must be a string")
    try:
        if transform is None:
            parsed = parse_timestamp(value)
        else:
            format_text = transform.date_format
            input_text = value
            directives = date_format_directives(format_text) or set()
            if not ({"m", "b"} & directives) or not {"d", "H", "M", "S"} <= directives:
                raise ConversionError("date format does not contain a complete event time")
            if "Y" not in directives:
                if transform.year is None:
                    raise ConversionError("date format requires an explicit year")
                format_text += " %Y"
                input_text += f" {transform.year}"
            parsed = datetime.strptime(input_text, format_text)  # noqa: DTZ007 -- timezone checked below.
            if parsed.tzinfo is None:
                if transform.timezone is None:
                    raise ConversionError("date format requires a timezone")
                parsed = parsed.replace(tzinfo=UTC)
        return parsed.isoformat().replace("+00:00", "Z")
    except (ValueError, OverflowError) as error:
        raise ConversionError("date is invalid") from error


def _one(field: EcsField, value: Any, transform: DateTransform | None) -> Any:
    if field.type in STRING_TYPES:
        if not isinstance(value, str):
            raise ConversionError("string value required")
        return value
    if field.type in INTEGER_RANGES:
        if isinstance(value, bool):
            raise ConversionError("integer value required")
        try:
            number = int(value) if isinstance(value, str) else value
        except (ValueError, TypeError) as error:
            raise ConversionError("integer value required") from error
        if not isinstance(number, int) or not INTEGER_RANGES[field.type][0] <= number <= INTEGER_RANGES[field.type][1]:
            raise ConversionError("integer out of range")
        return number
    if field.type in FLOAT_TYPES:
        if isinstance(value, bool):
            raise ConversionError("finite number required")
        try:
            number = float(value)
        except (ValueError, TypeError, OverflowError) as error:
            raise ConversionError("finite number required") from error
        if not math.isfinite(number):
            raise ConversionError("finite number required")
        return number
    if field.type == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.lower() in {"true", "false"}:
            return value.lower() == "true"
        raise ConversionError("boolean value required")
    if field.type == "ip":
        if not isinstance(value, str):
            raise ConversionError("IP address required")
        try:
            return str(ipaddress.ip_address(value))
        except ValueError as error:
            raise ConversionError("IP address is invalid") from error
    if field.type == "date":
        return _date(value, transform)
    raise ConversionError("ECS type is not mappable")


def convert_value(
    field: EcsField, value: Any, transform: DateTransform | None = None
) -> Any:
    if field.is_array:
        items = value if isinstance(value, list) else [value]
        if not items:
            raise ConversionError("array must not be empty")
        return [_one(field, item, transform) for item in items]
    if isinstance(value, list):
        raise ConversionError("scalar ECS field cannot receive an array")
    return _one(field, value, transform)
