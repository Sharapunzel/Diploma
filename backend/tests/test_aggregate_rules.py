"""Aggregate language contracts shared by API, worker and future editor."""

import json
import subprocess
import sys
from dataclasses import replace
from decimal import Decimal

import pytest
from pydantic import ValidationError
from sqlalchemy import column, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB

from app.aggregation.budget import estimate
from app.aggregation.results import AggregateStatistics, FeatureStatistics, interpret_result
from app.aggregation.rules import (
    AggregateRule,
    Expression,
    FeatureResult,
    Limits,
    RuleError,
    compile_rule,
    evaluate_expression,
    operator_catalog,
)
from app.ecs import PackagedEcsCatalog
from app.repositories.sqlalchemy.aggregate_compiler import aggregate_statement
from app.schemas.aggregates import ScheduleInput


@pytest.fixture(scope="module")
def catalog():
    return PackagedEcsCatalog.load()


AUTH_RULE = {
    "where": {"field": "event.category", "op": "has_any", "value": ["authentication"]},
    "features": [
        {"key": "events", "name": "События", "op": "count"},
        {"key": "failures", "name": "Отказы", "op": "count",
         "where": {"field": "event.outcome", "op": "eq", "value": "failure"}},
        {"key": "users", "name": "Пользователи", "op": "distinct_count", "field": "user.id"},
        {"key": "ratio", "name": "Доля", "op": "derive",
         "expr": {"op": "divide", "args": [{"ref": "failures"}, {"ref": "events"}]}},
    ],
}
WEB_RULE = {
    "where": {"op": "all", "args": [
        {"op": "in", "field": "http.request.method", "value": ["GET", "POST"]},
        {"op": "exists", "field": "network.bytes"}]},
    "features": [
        {"key": op, "name": op, "op": op, "field": "network.bytes"}
        for op in ("sum", "avg", "min", "max", "distinct_count")
    ] + [{"key": "count", "name": "count", "op": "count"}],
}


@pytest.mark.parametrize("document", [AUTH_RULE, WEB_RULE])
def test_two_source_rules_have_stable_hash_and_order(document, catalog):
    compiled = compile_rule(AggregateRule.model_validate(document), catalog)
    roundtrip = compile_rule(AggregateRule.model_validate_json(json.dumps(document)), catalog)
    assert roundtrip.sha256 == compiled.sha256
    assert [f.key for f in compiled.rule.features] == [f["key"] for f in document["features"]]
    reversed_document = {**document, "features": list(reversed(document["features"]))}
    if document is WEB_RULE:
        assert compile_rule(AggregateRule.model_validate(reversed_document), catalog).sha256 != compiled.sha256


@pytest.mark.parametrize("patch,code", [
    ({"op": "percentile", "field": "network.bytes"}, "unknown_operator"),
    ({"op": "sum", "field": "user.id"}, "unsupported_field_type"),
    ({"op": "sum", "field": "event.category"}, "unsupported_field_type"),
    ({"op": "sum", "field": "network.bytes); DROP TABLE app.users;--"}, "unknown_field"),
    ({"op": "count", "field": "user.id"}, "invalid_arguments"),
    ({"op": "distinct_count", "field": "http.request.body.content"}, "unsupported_field_type"),
    ({"op": "derive", "expr": {"ref": "later"}}, "unknown_or_forward_reference"),
])
def test_rejects_unknown_operations_paths_and_implicit_types(patch, code, catalog):
    rule = AggregateRule.model_validate({"features": [{"key": "n", "name": "N", **patch}]})
    with pytest.raises(RuleError, match=code):
        compile_rule(rule, catalog)


@pytest.mark.parametrize("condition,code", [
    ({"op": "eq", "field": "event.category", "value": "web"}, "unsupported_field_type"),
    ({"op": "has_any", "field": "event.category", "value": ["web", 1]}, "invalid_literal_type"),
    ({"op": "eq", "field": "network.bytes", "value": "1"}, "invalid_literal_type"),
    ({"op": "in", "field": "user.id", "value": []}, "literal_budget_exceeded"),
    ({"op": "exists", "field": "user.id", "value": "x"}, "invalid_arguments"),
])
def test_condition_types_and_shape(condition, code, catalog):
    rule = AggregateRule.model_validate({"where": condition, "features": WEB_RULE["features"]})
    with pytest.raises(RuleError, match=code):
        compile_rule(rule, catalog)


def test_unknown_keys_and_duplicate_keys(catalog):
    with pytest.raises(ValidationError):
        AggregateRule.model_validate({**WEB_RULE, "sql": "SELECT 1"})
    with pytest.raises(RuleError, match="duplicate_feature_key"):
        compile_rule(AggregateRule.model_validate({"features": WEB_RULE["features"] * 2}), catalog)


def test_literals_are_parameterized(catalog):
    document = {"where": {"op": "eq", "field": "user.id", "value": "'); DROP TABLE x;--"},
                "features": [{"key": "n", "name": "N", "op": "count"}]}
    compiled = compile_rule(AggregateRule.model_validate(document), catalog)
    base = select(column("ecs_data", JSONB)).subquery()
    statement = aggregate_statement(base, compiled).compile(dialect=postgresql.dialect())
    assert "DROP TABLE" not in str(statement)
    assert "'); DROP TABLE x;--" in statement.params.values()
    assert "FILTER" in str(statement)


def test_budgets_are_explicit_and_do_not_limit_to_five(catalog):
    compiled = compile_rule(AggregateRule.model_validate(WEB_RULE), catalog)
    result = estimate(compiled, 10, 100, 1, Limits(), True)
    assert result["vector_width"] == 6
    assert result["cost"] == 10 * compiled.weight
    assert not result["time_guarantee"]
    with pytest.raises(RuleError, match="preview_rows"):
        estimate(compiled, 100001, 100001, 1, Limits(), True)
    with pytest.raises(RuleError, match="operator_cost"):
        estimate(compiled, 100, 100, 1, replace(Limits(), cost=1), False)
    with pytest.raises(RuleError, match="total_memory"):
        estimate(compiled, 100000, 100000, 1, replace(Limits(), total_memory_mb=1), False)


def test_depth_literal_and_json_budgets(catalog):
    where = {"op": "exists", "field": "user.id"}
    for _ in range(8):
        where = {"op": "all", "args": [where]}
    with pytest.raises(RuleError, match="depth_budget_exceeded"):
        compile_rule(AggregateRule.model_validate({"where": where, "features": WEB_RULE["features"]}), catalog)
    with pytest.raises(RuleError, match="json_budget_exceeded"):
        compile_rule(AggregateRule.model_validate(WEB_RULE), catalog, replace(Limits(), json_bytes=10))


@pytest.mark.parametrize("op,a,b,value,reason", [
    ("add", 3, 4, 7, None), ("subtract", 3, 4, -1, None),
    ("multiply", 3, 4, 12, None), ("divide", 3, 4, .75, None),
    ("divide", 3, 0, None, "division_by_zero"),
    ("multiply", 1e308, 1e308, None, "non_finite_result"),
])
def test_arithmetic_reasons(op, a, b, value, reason):
    expression = Expression.model_validate({"op": op, "args": [{"value": a}, {"value": b}]})
    result = evaluate_expression(expression, {})
    assert result.value == value and result.reason == reason


def test_null_reference_is_not_zero():
    result = evaluate_expression(Expression(ref="x"),
        {"x": FeatureResult(value=None, reason="no_values", valid_count=0, invalid_count=0)})
    assert result.value is None and result.reason == "missing_operand"


def test_invalid_input_poison_and_empty_statistics(catalog):
    compiled = compile_rule(AggregateRule.model_validate(WEB_RULE), catalog)
    features = {feature.key: FeatureStatistics(None if i < 4 else 0, 0, 0)
                for i, feature in enumerate(compiled.rule.features)}
    statistics = AggregateStatistics(0, 0, 64, None, None, features)
    result = interpret_result(statistics, compiled)
    assert [f["value"] for f in result["features"]] == [None] * 4 + [0, 0]
    assert result["features"][0]["reason"] == "no_values"
    features["sum"] = FeatureStatistics(123, 2, 1)
    invalid = interpret_result(statistics, compiled)["features"][0]
    assert invalid == {"key": "sum", "value": None, "reason": "invalid_input_type",
                       "valid_count": 2, "invalid_count": 1}


@pytest.mark.parametrize("raw_value", [float("inf"), Decimal("1e1000")])
def test_domain_interpretation_non_finite_and_derived_reason(catalog, raw_value):
    compiled = compile_rule(AggregateRule.model_validate({"features": [
        {"key": "total", "name": "Total", "op": "sum", "field": "network.bytes"},
        {"key": "ratio", "name": "Ratio", "op": "derive", "expr": {
            "op": "divide", "args": [{"ref": "total"}, {"value": 2}]}}]}), catalog)
    statistics = AggregateStatistics(1, 1, 64, None, None,
                                    {"total": FeatureStatistics(raw_value, 1, 0)})
    result = interpret_result(statistics, compiled)
    assert [f["key"] for f in result["features"]] == ["total", "ratio"]
    assert [f["value"] for f in result["features"]] == [None, None]
    assert [f["reason"] for f in result["features"]] == ["non_finite_result", "missing_operand"]


def test_worker_import_does_not_load_http_composition():
    subprocess.run([sys.executable, "-B", "-c",
                    ("import sys; import app.aggregate_worker; "
                    "assert 'app.dependencies' not in sys.modules; "
                    "assert 'app.api.v1.router' not in sys.modules")], check=True)


def test_catalog_excludes_future_operations():
    catalog = {entry["name"]: entry for entry in operator_catalog()}
    assert "percentile" not in catalog and "not" not in catalog
    assert catalog["distinct_count"]["weight"] == 8
    assert catalog["has_any"]["inputs"] == ("keyword_array",)


def test_non_finite_literal_is_addressed(catalog):
    rule = AggregateRule.model_validate({"features": [{"key": "n", "name": "N", "op": "derive",
        "expr": {"value": float("inf")}}]})
    with pytest.raises(RuleError, match="non_finite_literal"):
        compile_rule(rule, catalog)


@pytest.mark.parametrize("value,reason", [("3", "non_numeric_operand"),
    (float("inf"), "non_finite_operand"), (None, "missing_operand")])
def test_defensive_operand_types_are_not_coerced(value, reason):
    previous = FeatureResult.model_construct(value=value, reason=None, valid_count=1, invalid_count=0)
    result = evaluate_expression(Expression(ref="x"), {"x": previous})
    assert result.value is None and result.reason == reason and result.invalid_count == 1


def test_iana_boundary_with_explicit_offset_preserves_utc_schedule():
    utc = ScheduleInput(boundary="2026-11-01T00:00:00Z", timezone="UTC", duration_seconds=300)
    local = ScheduleInput(boundary="2026-11-01T05:00:00+05:00", timezone="Asia/Yekaterinburg", duration_seconds=300)
    assert utc.anchor_utc == local.anchor_utc
    with pytest.raises(ValidationError, match="boundary_offset"):
        ScheduleInput(boundary="2026-03-29T02:30:00+01:00", timezone="Europe/Berlin", duration_seconds=300)


def test_wide_sql_plan_has_addressed_resource_error(catalog):
    rule = AggregateRule.model_validate({"features": [
        {"key": f"n{i}", "name": "N", "op": "count"} for i in range(600)]})
    compiled = compile_rule(rule, catalog)
    with pytest.raises(RuleError, match="statement_width"):
        estimate(compiled, 0, 0, 1, Limits(), True)


def test_vector_width_is_not_an_arbitrary_feature_count_limit(catalog):
    rule = AggregateRule.model_validate({"features": [
        {"key": f"n{i}", "name": "N", "op": "derive", "expr": {"value": 1}}
        for i in range(600)]})
    compiled = compile_rule(rule, catalog)
    assert estimate(compiled, 0, 0, 1, Limits(), True)["vector_width"] == 600
