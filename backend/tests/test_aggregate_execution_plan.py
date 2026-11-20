"""Pure shared-plan reuse, isolation and exact interpretation regressions."""

import builtins
import hashlib
import json
from collections import Counter, OrderedDict
from dataclasses import replace
from decimal import Decimal

import pytest
from sqlalchemy import column, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB

from app.aggregation import compilation_cache, results, rules
from app.aggregation.budget import estimate
from app.aggregation.plan import statistic_indices
from app.aggregation.rules import AggregateRule, Limits, RuleError, compile_rule
from app.ecs import PackagedEcsCatalog
from app.repositories.sqlalchemy.aggregate_compiler import aggregate_statement, read_statistics


@pytest.fixture(scope="module")
def catalog():
    return PackagedEcsCatalog.load()


def exact_rule():
    return AggregateRule.model_validate({"where": {"op": "exists", "field": "network.bytes"},
        "features": [{"key": op, "name": op, "op": op, "field": "network.bytes",
            **({"percentile": .75} if op == "percentile" else {})}
            for op in ("median", "percentile", "mode_count", "mode_share")]})


def test_identical_exact_inputs_have_one_sql_array_and_shared_guards(catalog):
    compiled = compile_rule(exact_rule(), catalog)
    statement = aggregate_statement(select(column("ecs_data", JSONB)).subquery(), compiled)
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert sql.count("array_agg(") == 1
    assert "aggregate_typed AS MATERIALIZED" in sql
    assert "aggregate_predicates AS MATERIALIZED" in sql
    assert set(statistic_indices(compiled).values()) == {0}
    assert len(statement.selected_columns) == 8
    # Admission still charges all four exact consumers conservatively.
    budget = estimate(compiled, 3, 3, 300, Limits(), True)
    assert budget["exact_materialized_features"] == 4
    assert budget["statement_columns"] == 17


def test_exact_interpretation_sorts_and_counts_shared_input_once(catalog, monkeypatch):
    compiled = compile_rule(exact_rule(), catalog)
    values = [Decimal(2), Decimal(1), Decimal(1)]
    statistics = read_statistics({"observed_rows": 3, "observed_width": 50,
        "event_count": 3, "event_time_min": None, "event_time_max": None,
        "v0": values, "n0": 3, "b0": 0}, compiled)
    assert all(feature.value is values for feature in statistics.features.values())
    calls = {"sort": 0, "counter": 0}

    def sorted_values(value):
        calls["sort"] += 1
        return builtins.sorted(value)

    def frequencies(value):
        calls["counter"] += 1
        return Counter(value)

    monkeypatch.setattr(results, "sorted", sorted_values, raising=False)
    monkeypatch.setattr(results, "Counter", frequencies)
    actual = results.interpret_result(statistics, compiled)["features"]
    assert [feature["value"] for feature in actual] == [1, 1.5, 2, float(Decimal(2) / 3)]
    assert all(feature["reason"] is None for feature in actual)
    assert calls == {"sort": 1, "counter": 1}


def test_effective_filters_remain_separate_inputs(catalog):
    rule = exact_rule().model_dump(exclude_none=True)
    rule["features"][1]["where"] = {"op": "gt", "field": "network.bytes", "value": 1}
    compiled = compile_rule(AggregateRule.model_validate(rule), catalog)
    indices = statistic_indices(compiled)
    assert indices["median"] == indices["mode_share"]
    assert indices["median"] != indices["percentile"]
    sql = str(aggregate_statement(select(column("ecs_data", JSONB)).subquery(), compiled)
        .compile(dialect=postgresql.dialect()))
    assert sql.count("array_agg(") == 2


@pytest.fixture
def empty_cache(monkeypatch):
    monkeypatch.setattr(compilation_cache, "_entries", OrderedDict())
    monkeypatch.setattr(compilation_cache, "_bytes", 0)


def test_compilation_cache_is_bounded_defensive_and_respects_limits(catalog, empty_cache, monkeypatch):
    original, calls = rules._compile_rule, []

    def compile_once(*args):
        calls.append(True)
        return original(*args)

    monkeypatch.setattr(rules, "_compile_rule", compile_once)
    monkeypatch.setattr(compilation_cache, "MAX_ENTRIES", 2)
    first = compile_rule(exact_rule(), catalog)
    first.rule.features[0].name = "changed caller"
    second = compile_rule(exact_rule(), catalog)
    assert second.rule.features[0].name == "median" and len(calls) == 1
    second.fields.clear()
    assert "network.bytes" in compile_rule(exact_rule(), catalog).fields
    compile_rule(exact_rule(), catalog, replace(Limits(), cost=123))
    compile_rule(exact_rule(), catalog, replace(Limits(), cost=456))
    assert len(compilation_cache._entries) == 2 and len(calls) == 3
    assert compilation_cache._bytes <= compilation_cache.MAX_BYTES
    compile_rule(exact_rule(), catalog)
    assert len(calls) == 4


def test_changed_catalog_definition_cannot_reuse_previous_plan(catalog, empty_cache):
    class MutableCatalog:
        version = catalog.version
        changed = False

        def get(self, name):
            field = catalog.get(name)
            return replace(field, type="keyword") if self.changed and name == "network.bytes" else field

    changing = MutableCatalog()
    compile_rule(exact_rule(), changing)
    changing.changed = True
    with pytest.raises(RuleError, match="unsupported_field_type"):
        compile_rule(exact_rule(), changing)


def test_legacy_maximum_width_keeps_all_outputs_after_statistic_reuse(catalog):
    rule = AggregateRule.model_validate({"features": [
        {"key": f"n{index}", "name": "N", "op": "count"} for index in range(553)]})
    compiled = compile_rule(rule, catalog)
    expected_hash = hashlib.sha256(json.dumps(rule.model_dump(exclude_none=True),
        sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    assert compiled.sha256 == expected_hash
    statement = aggregate_statement(select(column("ecs_data", JSONB)).subquery(), compiled)
    assert len(statement.selected_columns) == 8
    statistics = read_statistics({"observed_rows": 2, "observed_width": 20,
        "event_count": 2, "event_time_min": None, "event_time_max": None,
        "v0": 2, "n0": 2, "b0": 0}, compiled)
    actual = results.interpret_result(statistics, compiled)["features"]
    assert actual == [{"key": f"n{index}", "value": 2, "reason": None,
        "valid_count": 2, "invalid_count": 0} for index in range(553)]


def test_exact_sort_expiry_does_not_produce_a_partial_vector(catalog, monkeypatch):
    from app.aggregation.event_time import EventTimeBudgetError, OperationBudget

    clock = [0.0]
    budget = OperationBudget(4, clock=lambda: clock[0])
    compiled = compile_rule(exact_rule(), catalog)
    statistics = read_statistics({"observed_rows": 3, "observed_width": 50,
        "event_count": 3, "event_time_min": None, "event_time_max": None,
        "v0": [Decimal(2), Decimal(1), Decimal(1)], "n0": 3, "b0": 0}, compiled)

    def slow_sort(value):
        clock[0] = 4
        return builtins.sorted(value)

    monkeypatch.setattr(results, "sorted", slow_sort, raising=False)
    with pytest.raises(EventTimeBudgetError, match="operation_seconds"):
        results.interpret_result(statistics, compiled, check_budget=budget.check)


def test_exact_frequency_loop_checks_remaining_budget_in_bounded_portions(catalog, monkeypatch):
    from app.aggregation.event_time import EventTimeBudgetError, OperationBudget

    clock, visits = [0.0], []
    budget = OperationBudget(4, clock=lambda: clock[0])
    rule = AggregateRule.model_validate({"features": [
        {"key": "mode", "name": "Mode", "op": "mode_count", "field": "network.bytes"}]})
    compiled = compile_rule(rule, catalog)
    statistics = read_statistics({"observed_rows": 2000, "observed_width": 50,
        "event_count": 2000, "event_time_min": None, "event_time_max": None,
        "v0": list(range(2000)), "n0": 2000, "b0": 0}, compiled)
    original = results.scalar_key

    def item_key(item, ecs_type):
        visits.append(item)
        if len(visits) == 300:
            clock[0] = 4
        return original(item, ecs_type)

    monkeypatch.setattr(results, "scalar_key", item_key)
    with pytest.raises(EventTimeBudgetError, match="operation_seconds"):
        results.interpret_result(statistics, compiled, check_budget=budget.check)
    assert 300 <= len(visits) <= 512


def test_coordinate_preflight_stops_before_visiting_the_whole_input():
    from app.aggregation.event_time import EventTimeBudgetError, OperationBudget
    from app.services.implementations.aggregate_snapshot import coordinate_memory

    clock, visited = [0.0], []
    budget = OperationBudget(0.8, clock=lambda: clock[0])

    def coordinates():
        for index in range(2000):
            visited.append(index)
            if index == 100:
                clock[0] = 0.8
            yield {"topic": "input"}

    with pytest.raises(EventTimeBudgetError, match="operation_seconds"):
        coordinate_memory(coordinates(), 1536, 4, budget)
    assert 101 <= len(visited) <= 129
    assert coordinate_memory([{"topic": "ж"}, {"topic": None}], 1536, 4,
        OperationBudget(1)) == 2 * 1536 + 4 * len("ж".encode())
