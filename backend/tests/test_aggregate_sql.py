"""Execute the aggregate compiler on PostgreSQL, without mutating source data."""

import os

import pytest
from sqlalchemy import create_engine, false, literal, select, union_all
from sqlalchemy.dialects.postgresql import JSONB

from app.aggregation.results import interpret_result
from app.aggregation.rules import AggregateRule, compile_rule
from app.ecs import PackagedEcsCatalog
from app.repositories.sqlalchemy.aggregate_compiler import aggregate_statement, read_statistics


@pytest.fixture(scope="module")
def database():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.fail("TEST_DATABASE_URL must name isolated test PostgreSQL")
    engine = create_engine(url)
    assert engine.url.database != "diploma_db"
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="module")
def catalog():
    return PackagedEcsCatalog.load()


def calculate(database, catalog, rule, events):
    statements = [select(literal({"@timestamp": "2026-01-01T00:00:00Z", **event},
                                  type_=JSONB).label("ecs_data")) for event in events]
    base = (union_all(*statements).subquery() if len(statements) > 1
            else statements[0].subquery() if statements
            else select(literal({}, type_=JSONB).label("ecs_data")).where(false()).subquery())
    compiled = compile_rule(AggregateRule.model_validate(rule), catalog)
    with database.connect() as connection:
        row = connection.execute(aggregate_statement(base, compiled)).mappings().one()
    return interpret_result(read_statistics(row, compiled), compiled)


def test_authentication_scalar_array_distinct_and_ratio(database, catalog):
    rule = {"where": {"op": "has_any", "field": "event.category", "value": ["authentication"]},
            "features": [
                {"key": "n", "name": "N", "op": "count"},
                {"key": "f", "name": "F", "op": "count", "where": {
                    "op": "eq", "field": "event.outcome", "value": "failure"}},
                {"key": "u", "name": "U", "op": "distinct_count", "field": "user.id"},
                {"key": "r", "name": "R", "op": "derive", "expr": {
                    "op": "divide", "args": [{"ref": "f"}, {"ref": "n"}]}}]}
    result = calculate(database, catalog, rule, [
        {"event": {"category": ["authentication"], "outcome": "failure"}, "user": {"id": "a"}},
        {"event": {"category": ["authentication"], "outcome": "success"}, "user": {"id": "a"}},
        {"event": {"category": ["web"]}, "user": {"id": "b"}}])
    assert result["event_count"] == 2
    assert [f["value"] for f in result["features"]] == [2, 1, 1, .5]
    assert result["event_time_min"] == result["event_time_max"]


def test_web_numeric_null_and_exact_distinct(database, catalog):
    rule = {"features": [{"key": op, "name": op, "op": op, "field": "network.bytes"}
                         for op in ["sum", "avg", "min", "max", "distinct_count"]]}
    result = calculate(database, catalog, rule, [
        {"network": {"bytes": 2}}, {"network": {"bytes": 4}},
        {"network": {"bytes": 4}}, {"network": {"bytes": None}}, {}])
    assert [f["value"] for f in result["features"]] == [10, 10/3, 2, 4, 2]
    assert all(f["valid_count"] == 3 for f in result["features"])


@pytest.mark.parametrize("value", ["2", True, [], {}, 1.5, 1.0])
def test_invalid_numeric_type_poison_statistics(database, catalog, value):
    result = calculate(database, catalog,
        {"features": [{"key": "n", "name": "N", "op": "sum", "field": "network.bytes"}]},
        [{"network": {"bytes": 3}}, {"network": {"bytes": value}}])
    feature = result["features"][0]
    assert feature["value"] is None and feature["reason"] == "invalid_input_type"
    assert feature["invalid_count"] == 1


@pytest.mark.parametrize("events,valid_count", [
    ([{"network": {"bytes": 1.0}}], 0),
    ([{"network": {"bytes": 1.0}}, {"network": {"bytes": 3}}], 1),
])
def test_fractional_json_long_poison_all_numeric_statistics(database, catalog, events, valid_count):
    result = calculate(database, catalog, {"features": [
        {"key": op, "name": op, "op": op, "field": "network.bytes"}
        for op in ("sum", "avg", "min", "max", "distinct_count")]}, events)
    for feature in result["features"]:
        assert feature["value"] is None
        assert feature["reason"] == "invalid_input_type"
        assert feature["valid_count"] == valid_count
        assert feature["invalid_count"] == 1


@pytest.mark.parametrize("op,value", [("eq", 1), ("in", [1]), ("exists", None), ("not_exists", None)])
@pytest.mark.parametrize("global_condition", [False, True])
def test_fractional_json_long_invalid_in_conditions(database, catalog, op, value, global_condition):
    condition = {"op": op, "field": "network.bytes"}
    if value is not None:
        condition["value"] = value
    feature = {"key": "n", "name": "N", "op": "count"}
    rule = {"features": [feature]}
    if global_condition:
        rule["where"] = condition
    else:
        feature["where"] = condition
    result = calculate(database, catalog, rule,
                       [{"network": {"bytes": 1.0}}, {"network": {"bytes": 1}}])
    assert result["features"][0]["value"] is None
    assert result["features"][0]["reason"] == "invalid_input_type"
    assert result["features"][0]["invalid_count"] == 1


@pytest.mark.parametrize("category", [["authentication", 1], "authentication", [None], {}])
def test_array_invalid_type_poison_global_condition(database, catalog, category):
    result = calculate(database, catalog,
        {"where": {"op": "has_any", "field": "event.category", "value": ["authentication"]},
         "features": [{"key": "n", "name": "N", "op": "count"}]},
        [{"event": {"category": category}}])
    feature = result["features"][0]
    assert feature["value"] is None and feature["invalid_count"] == 1


def test_invalid_local_condition_only_affects_its_feature(database, catalog):
    result = calculate(database, catalog, {"features": [
        {"key": "all", "name": "All", "op": "count"},
        {"key": "failed", "name": "Failed", "op": "count", "where": {
            "op": "eq", "field": "event.outcome", "value": "failure"}}]},
        [{"event": {"outcome": ["failure"]}}])
    assert result["features"][0]["value"] == 1
    assert result["features"][1]["value"] is None


def test_empty_has_no_invented_numeric_zero(database, catalog):
    result = calculate(database, catalog, {"features": [
        {"key": "count", "name": "Count", "op": "count"},
        {"key": "sum", "name": "Sum", "op": "sum", "field": "network.bytes"}]}, [])
    assert result["features"][0]["value"] == 0
    assert result["features"][1]["value"] is None
    assert result["features"][1]["reason"] == "no_values"


@pytest.mark.parametrize("value,valid", [(2**63 - 1, True), (2**63, False), (-(2**63), True)])
def test_ecs_long_boundaries(database, catalog, value, valid):
    result = calculate(database, catalog, {"features": [
        {"key": "n", "name": "N", "op": "max", "field": "network.bytes"}]},
        [{"network": {"bytes": value}}])
    feature = result["features"][0]
    assert feature["value"] == (value if valid else None)
    assert feature["invalid_count"] == (0 if valid else 1)
