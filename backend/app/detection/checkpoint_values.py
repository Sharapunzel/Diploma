"""Strict primitive types for the pinned persisted mapper format."""

import math

from .contracts import DetectionError


def require(condition):
    if not condition:
        raise DetectionError("checkpoint_mapper_structure")


def integer(value, minimum=None, maximum=None):
    require(type(value) is int and (minimum is None or value >= minimum)
            and (maximum is None or value <= maximum))


def number(value):
    require(type(value) in (int, float) and math.isfinite(value))


def counts(value, **expected):
    for key, wanted in expected.items():
        integer(value[key])
        require(value[key] == wanted)


def vector(value, check, *, length=None):
    require(isinstance(value, list) and (length is None or len(value) == length))
    for item in value:
        check()
        number(item)


def state(value, version=None):
    require(isinstance(value, dict) and bool(value))
    if version is not None:
        require(value.get("version") == version)
    return value


def deviations(value, check, length):
    require(isinstance(value, list) and len(value) == length)
    for item in value:
        check()
        state(item)
        integer(item["count"], 0)
        for key in ("discount", "weight", "sum", "sumSquared"):
            number(item[key])
        require(0 <= item["discount"] < 1 and item["weight"] >= 0 and item["sumSquared"] >= 0)
