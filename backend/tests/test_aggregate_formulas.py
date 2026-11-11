"""Formula contracts and real PostgreSQL calculations, including legacy goldens."""

import copy
import hashlib
import json
from dataclasses import replace
from uuid import UUID, uuid4

import pytest
import test_aggregate_service as aggregate_cases
from pydantic import ValidationError
from test_aggregate_sql import calculate

from app.aggregation.budget import estimate
from app.aggregation.results import AggregateStatistics, FeatureStatistics, interpret_result
from app.aggregation.rules import AggregateRule, Limits, RuleError, compile_rule, operator_catalog
from app.schemas.aggregates import CreateAggregateRequest

pytest_plugins = ["test_aggregate_sql"]


LEGACY = {"features": [
    {"key": "n", "name": "Count", "op": "count"},
    {"key": "r", "name": "Ratio", "op": "derive", "expr": {
        "op": "divide", "args": [{"ref": "n"}, {"value": 2.0}]}},
]}
LEGACY_HASH = "5fe5b0627db17f5b7055a41749eed0ab88cbdc3454b312c863d35c962f226fa1"
LEGACY_DIGEST = "22e5808b3dd018652bde2f4f34a66c9f7ed4bfa9076da6aa76196bb7783ae7d1"


def leaf(op="sum", field="network.bytes", **kwargs):
    return {"aggregate": {"op": op, **({"field": field} if op != "count" else {}), **kwargs}}


def formula(expr, key="result", **kwargs):
    return {"key": key, "name": key, "op": "formula", "expr": expr, **kwargs}


def test_head_legacy_goldens(catalog):
    # Constants were captured from HEAD 72f80e01 before changing the engine.
    compiled = compile_rule(AggregateRule.model_validate(LEGACY), catalog)
    assert compiled.canonical == LEGACY and compiled.sha256 == LEGACY_HASH
    body = CreateAggregateRequest(source_id=UUID(int=2), aggregate_rule=LEGACY,
        schedule={"boundary": "2026-11-01T00:00:00Z", "timezone": "UTC", "duration_seconds": 300},
        window_start_utc="2026-11-01T00:00:00Z", time_basis="backend_processed_at_historical",
        name="Legacy", rule_hash=LEGACY_HASH)
    document = body.model_dump(mode="json")
    assert "auxiliary" not in document["aggregate_rule"]
    for feature in document["aggregate_rule"]["features"]:
        if feature.get("percentile") is None:
            feature.pop("percentile", None)
    assert "aggregate" not in document["aggregate_rule"]["features"][1]["expr"]
    assert hashlib.sha256(json.dumps(document, ensure_ascii=False, sort_keys=True,
        separators=(",", ":")).encode()).hexdigest() == LEGACY_DIGEST
    assert hashlib.sha256(json.dumps(operator_catalog(), ensure_ascii=False, sort_keys=True,
        separators=(",", ":")).encode()).hexdigest() == "734e9fe453c32c0d4a2f7b31a9869117b2795e2216d525134b368ff58fe170b4"


def test_extended_full_dto_roundtrip_preserves_legacy_nullable_expression_fields(catalog):
    rule = AggregateRule.model_validate({"features": [formula({"op": "add", "args": [leaf("count"), {"ref": "inner"}]})],
        "auxiliary": [formula({"value": 2.0}, "inner")]})
    dumped = rule.model_dump(mode="json")
    assert dumped["auxiliary"][0]["expr"]["op"] is None
    assert compile_rule(rule, catalog).sha256 == compile_rule(AggregateRule.model_validate(dumped), catalog).sha256


def test_formula_empty_output_keeps_coverage_gate_and_strict_detector_shape(catalog):
    from app.detection.contracts import DetectionError, feature_vector

    compiled = compile_rule(AggregateRule.model_validate({"features": [formula({"ref": "hidden"})],
        "auxiliary": [{"key": "hidden", "name": "Hidden", "op": "count"}]}), catalog)
    result = interpret_result(AggregateStatistics(0, 0, 256, None, None,
        {"hidden": FeatureStatistics(0, 0, 0)}), compiled)["features"]
    assert feature_vector(result, ["result"], "complete") == [0]
    for completeness in ("unknown", "interrupted", "unverified"):
        with pytest.raises(DetectionError, match="coverage:" + completeness):
            feature_vector(result, ["result"], completeness)
    with pytest.raises(DetectionError, match="feature_order_or_payload"):
        feature_vector([*result, {"key": "hidden", "value": 0}], ["result"], "complete")


def test_primary_window_ratio_and_nested_exact_integer(database, catalog):
    rule = {"features": [formula({"op": "ratio", "args": [
        leaf(field="source.bytes"), leaf(field="destination.bytes")]}),
        formula({"op": "add", "args": [leaf(), {"value": 1}]}, "exact")]}
    result = calculate(database, catalog, rule, [
        {"source": {"bytes": 6}, "destination": {"bytes": 2}, "network": {"bytes": 2**60}},
        {"source": {"bytes": 3}, "destination": {"bytes": 1}},
    ])
    assert [f["value"] for f in result["features"]] == [3, 2**60 + 1]


def test_auxiliary_closure_output_order_and_filter_scope(database, catalog):
    eq = lambda field, value: {"op": "eq", "field": field, "value": value}
    rule = {"where": eq("user.name", "Andrew"), "auxiliary": [
        {"key": "logon", "name": "Logon", "op": "count", "where": eq("event.action", "logon")},
        formula({"op": "multiply", "args": [{"ref": "logon"}, {"value": 2}]}, "twice"),
        {"key": "unused", "name": "Unused", "op": "sum", "field": "source.bytes"}],
        "features": [formula({"op": "divide", "args": [{"ref": "twice"},
            leaf("count", where=eq("event.action", "logoff"))]}, where=eq("event.action", "logoff")),
            formula({"ref": "twice"}, "second")]}
    result = calculate(database, catalog, rule, [
        {"user": {"name": "Andrew"}, "event": {"action": action}, "source": {"bytes": "wrong"}}
        for action in ("logon", "logon", "logoff")
    ] + [{"user": {"name": "Other"}, "event": {"action": "logon"}}])
    assert result["event_count"] == 3
    assert [f["key"] for f in result["features"]] == ["result", "second"]
    assert [f["value"] for f in result["features"]] == [4, 4]


@pytest.mark.parametrize("auxiliary", [None, []])
def test_explicit_empty_auxiliary_rejected(auxiliary):
    with pytest.raises(ValidationError):
        AggregateRule.model_validate({**LEGACY, "auxiliary": auxiliary})


@pytest.mark.parametrize("rule,code", [
    ({"features": [formula({"ref": "result"})]}, "cyclic_reference"),
    ({"features": [formula({"ref": "missing"})]}, "unknown_reference"),
    ({"features": [formula({"ref": "other"})], "auxiliary": [formula({"ref": "result"}, "other")]}, "cyclic_reference"),
    ({"features": [formula({"value": 1}, where={"op": "exists", "field": "user.name"})]}, "where_without_aggregate"),
    ({"features": [{"key": "n", "name": "N", "op": "derive", "expr": leaf()}]}, "aggregate_requires_formula"),
    ({"features": [formula({"aggregate": {"op": "derive"}})]}, "invalid_aggregate_operator"),
    ({"features": [formula(leaf("sum", "user.name"))]}, "unsupported_field_type"),
    ({"features": [formula({"op": "divide", "args": [leaf()]})]}, "binary_expression_required"),
    ({"features": [formula({"value": 1})], "auxiliary": [formula({"value": 2})]}, "duplicate_feature_key"),
])
def test_rejects_invalid_graph_and_shapes(catalog, rule, code):
    with pytest.raises(RuleError, match=code) as error:
        compile_rule(AggregateRule.model_validate(rule), catalog)
    assert error.value.path


def test_hidden_statistics_budgets_and_deduplication(catalog):
    rule = {"features": [formula({"op": "add", "args": [leaf("distinct_count"), leaf("percentile", percentile=.95)]})]}
    compiled = compile_rule(AggregateRule.model_validate(rule), catalog)
    budget = estimate(compiled, 10, 10, 300, Limits(), True)
    assert budget["vector_width"] == 1 and budget["statement_columns"] == 11
    assert budget["exact_materialized_features"] == 1
    assert budget["estimated_memory_bytes"] >= 5 * 10 * 256
    with pytest.raises(RuleError, match="total_memory"):
        estimate(compiled, 1000, 1000, 300, replace(Limits(), total_memory_mb=1), True)
    duplicated = compile_rule(AggregateRule.model_validate({"features": [formula({"op": "add", "args": [leaf(), leaf()]})]}), catalog)
    assert estimate(duplicated, 1, 1, 1, Limits(), True)["statement_columns"] == 8


@pytest.mark.parametrize("events,expr,reason", [
    ([], leaf(), "no_values"),
    ([{"network": {"bytes": "wrong"}}], leaf(), "invalid_input_type"),
    ([{"network": {"bytes": 1.0}}], leaf(), "invalid_input_type"),
    ([{"network": {"bytes": 0}}], {"op": "divide", "args": [{"value": 1}, leaf()]}, "division_by_zero"),
])
def test_required_dependency_quality_reaches_output(database, catalog, events, expr, reason):
    rule = {"features": [formula({"ref": "needed"})], "auxiliary": [formula(expr, "needed")]}
    result = calculate(database, catalog, rule, events)["features"]
    assert result == [{"key": "result", "value": None, "reason": reason, "valid_count": 0, "invalid_count": 1}]


def test_forward_references_are_extended_only_and_output_order_is_fixed(database, catalog):
    extended = {"features": [formula({"ref": "later"}), formula(leaf("count"), "later")]}
    assert [f["value"] for f in calculate(database, catalog, extended, [{}, {}])["features"]] == [2, 2]
    legacy = {"features": [dict(f, op="derive") for f in extended["features"]]}
    with pytest.raises(RuleError, match="unknown_or_forward_reference"):
        compile_rule(AggregateRule.model_validate(legacy), catalog)


def test_hidden_postgresql_width_and_expression_literal_limits(catalog):
    leaves = [leaf("count", where={"op": "eq", "field": "user.id", "value": str(i)}) for i in range(554)]
    compiled = compile_rule(AggregateRule.model_validate({"features": [formula({"op": "min_of", "args": leaves})]}), catalog)
    with pytest.raises(RuleError, match="statement_width"):
        estimate(compiled, 0, 0, 1, Limits(), True)
    with pytest.raises(RuleError, match="literal_budget_exceeded"):
        compile_rule(AggregateRule.model_validate({"features": [formula({"op": "min_of", "args": [{"value": i} for i in range(257)]})]}), catalog)


@pytest.mark.parametrize("expr", [
    {"aggregate": {"op": "count"}, "value": 1},
    {"aggregate": {"op": "sum", "field": "network.bytes", "expr": {"value": 1}}},
    {"aggregate": {"op": "count", "field": "network.bytes"}},
    {"aggregate": None},
    {"aggregate": {"op": "sum", "field": "network.bytes", "percentile": .5}},
    {"value": "1"},
])
def test_closed_expression_and_operand_shapes(catalog, expr):
    with pytest.raises((RuleError, ValidationError)):
        compile_rule(AggregateRule.model_validate({"features": [formula(expr)]}), catalog)


@pytest.mark.parametrize("op", ["derive", "formula"])
@pytest.mark.parametrize("nested", [False, True])
def test_explicit_null_aggregate_rejected_by_schema_before_serialization(op, nested):
    node = {"ref": "n", "aggregate": None}
    expression = {"op": "add", "args": [node, {"value": 1}]} if nested else node
    document = {"features": [
        {"key": "n", "name": "N", "op": "count"},
        {"key": "r", "name": "R", "op": op, "expr": expression}]}
    with pytest.raises(ValidationError) as error:
        AggregateRule.model_validate(document)
    assert error.value.errors()[0]["loc"] == (
        "features", 1, "expr", *(("args", 0) if nested else ()), "aggregate")


def test_input_depth_is_checked_before_generated_effective_conditions(catalog):
    condition = {"op": "exists", "field": "user.name"}
    for _ in range(7):
        condition = {"op": "not", "args": [condition]}
    rule = {"features": [formula(leaf("count", where=condition), where=condition)]}
    compiled = compile_rule(AggregateRule.model_validate(rule), catalog)
    assert compiled.weight > 0  # Both user trees are valid depth 8.
    rule["features"][0]["where"] = {"op": "not", "args": [condition]}
    with pytest.raises(RuleError, match="depth_budget_exceeded"):
        compile_rule(AggregateRule.model_validate(rule), catalog)


@pytest.fixture
def api_database():
    yield from aggregate_cases.database.__wrapped__()


@pytest.fixture
def api_source(api_database):
    yield from aggregate_cases.source.__wrapped__(api_database)


def test_real_legacy_receipt_replays_with_head_document(api_database, api_source, catalog):
    body = CreateAggregateRequest(source_id=api_source, aggregate_rule=LEGACY,
        schedule={"boundary": "2026-11-01T00:00:00Z", "timezone": "UTC", "duration_seconds": 300},
        window_start_utc="2026-11-01T00:00:00Z", time_basis="backend_processed_at_historical",
        name="Legacy", rule_hash=LEGACY_HASH)
    # Explicit old nullable fields, independent of the new Pydantic serializer.
    old_expr = {"op": "divide", "ref": None, "value": None, "args": [
        {"op": None, "args": None, "ref": "n", "value": None},
        {"op": None, "args": None, "ref": None, "value": 2.0}]}
    old_rule = {"where": None, "features": [
        {**LEGACY["features"][0], "field": None, "where": None, "expr": None},
        {**LEGACY["features"][1], "field": None, "where": None, "expr": old_expr}]}
    old_document = {"source_id": str(api_source), "aggregate_rule": old_rule,
        "schedule": {"boundary": "2026-11-01T00:00:00Z", "timezone": "UTC", "duration_seconds": 300},
        "window_start_utc": "2026-11-01T00:00:00Z", "time_basis": "backend_processed_at_historical",
        "name": "Legacy", "rule_hash": LEGACY_HASH}
    digest = hashlib.sha256(json.dumps(old_document, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    with api_database.session_factory() as session:
        api = aggregate_cases.service(session, catalog)
        card = api.create(body, UUID(int=1), "old-receipt")
        assert api.repository.get(card.id).request_hash == digest
        assert api.copy(card.id).aggregate_rule.model_dump(exclude_none=True) == LEGACY
    with api_database.session_factory() as session:
        assert aggregate_cases.service(session, catalog).create(body, UUID(int=1), "old-receipt").id == card.id


def test_extended_api_validate_preview_create_card_copy_and_points(api_database, api_source, catalog):
    import os

    from fastapi import Depends
    from fastapi.testclient import TestClient

    from app.config import Settings
    from app.core.security import hash_password
    from app.db import get_session
    from app.dependencies import get_aggregate_service
    from app.main import create_app
    from app.models import User

    app = create_app(Settings(environment="test", database_url=os.environ["TEST_DATABASE_URL"]), database=api_database,
        ecs_catalog=catalog, start_consumers=False)
    def dependency(session=Depends(get_session)):
        return aggregate_cases.service(session, catalog)
    app.dependency_overrides[get_aggregate_service] = dependency
    username = uuid4().hex
    with api_database.session_factory.begin() as session:
        session.add(User(username=username, display_name=username, password_hash=hash_password("synthetic-password"),
            role_id=UUID("00000000-0000-4000-8000-000000000001")))
    rule = {"features": [formula({"op": "add", "args": [leaf("count"), {"ref": "inner"}]})],
        "auxiliary": [formula({"value": 2.0}, "inner")]}
    body = aggregate_cases.request(api_source).model_dump(mode="json")
    body["aggregate_rule"] = rule
    with TestClient(app) as client:
        assert client.post("/api/v1/auth/local/login", json={"username": username, "password": "synthetic-password"}).status_code == 200
        csrf = {"X-CSRF-Token": client.get("/api/v1/auth/session").json()["csrf_token"]}
        checked = client.post("/api/v1/aggregates/validate", headers=csrf,
            json={key: body[key] for key in ("source_id", "schedule", "aggregate_rule")})
        assert checked.status_code == 200 and checked.json()["feature_keys"] == ["result"]
        preview = client.post("/api/v1/aggregates/preview", headers=csrf, json=body)
        assert preview.status_code == 200 and [f["value"] for f in preview.json()["features"]] == [2]
        created = client.post("/api/v1/aggregates", headers={**csrf, "Idempotency-Key": uuid4().hex},
            json={**body, "name": "Formula", "rule_hash": checked.json()["rule_hash"]})
        assert created.status_code == 201, created.text
        identity = created.json()["id"]
        copied = client.get(f"/api/v1/aggregates/{identity}/copy").json()["aggregate_rule"]
        assert copied["features"][0]["expr"]["args"][0]["aggregate"]["op"] == "count"
        assert type(copied["auxiliary"][0]["expr"]["value"]) is float
        assert len(client.get(f"/api/v1/aggregates/{identity}").json()["aggregate_rule"]["features"]) == 1
        assert client.get(f"/api/v1/aggregates/{identity}/points").json()["items"] == []


@pytest.mark.parametrize("op", ["derive", "formula"])
@pytest.mark.parametrize("nested", [False, True])
def test_http_db_replay_rejects_explicit_null_without_changing_receipt(api_database, api_source, catalog, op, nested):
    import os

    from fastapi.testclient import TestClient
    from sqlalchemy import func, select

    from app.config import Settings
    from app.core.security import hash_password
    from app.main import create_app
    from app.models import Aggregate, User

    app = create_app(Settings(environment="test", database_url=os.environ["TEST_DATABASE_URL"]),
        database=api_database, ecs_catalog=catalog, start_consumers=False)
    username, key = uuid4().hex, uuid4().hex
    with api_database.session_factory.begin() as session:
        session.add(User(username=username, display_name=username, password_hash=hash_password("synthetic-password"),
            role_id=UUID("00000000-0000-4000-8000-000000000001")))
    expression = (leaf("count") if op == "formula" else {"ref": "n"})
    if nested:
        expression = {"op": "add", "args": [{"ref": "n"}, leaf("count") if op == "formula" else {"value": 1}]}
    body = aggregate_cases.request(api_source).model_dump(mode="json")
    body["aggregate_rule"] = {"features": [
        {"key": "n", "name": "N", "op": "count"},
        {"key": "r", "name": "R", "op": op, "expr": expression}]}
    with TestClient(app) as client:
        assert client.post("/api/v1/auth/local/login", json={"username": username, "password": "synthetic-password"}).status_code == 200
        headers = {"X-CSRF-Token": client.get("/api/v1/auth/session").json()["csrf_token"], "Idempotency-Key": key}
        validation = client.post("/api/v1/aggregates/validate", headers=headers,
            json={name: body[name] for name in ("source_id", "schedule", "aggregate_rule")})
        assert validation.status_code == 200, validation.text
        body.update(name="Replay contract", rule_hash=validation.json()["rule_hash"])
        created = client.post("/api/v1/aggregates", headers=headers, json=body)
        assert created.status_code == 201, created.text
        original_card = created.json()
        identity = UUID(original_card["id"])
        with api_database.session_factory() as session:
            stored = session.get(Aggregate, identity)
            receipt = (stored.request_key, stored.request_hash, copy.deepcopy(stored.rule))
        replay = client.post("/api/v1/aggregates", headers=headers, json=body)
        assert replay.status_code == 201 and replay.json()["id"] == str(identity)

        forbidden = copy.deepcopy(body)
        target = forbidden["aggregate_rule"]["features"][1]["expr"]
        if nested:
            target = target["args"][0]
        target["aggregate"] = None
        with pytest.raises(ValidationError):
            CreateAggregateRequest.model_validate(forbidden)
        assert client.post("/api/v1/aggregates/validate", headers=headers,
            json={name: forbidden[name] for name in ("source_id", "schedule", "aggregate_rule")}).status_code == 422
        assert client.post("/api/v1/aggregates", headers=headers, json=forbidden).status_code == 422

        replay = client.post("/api/v1/aggregates", headers=headers, json=body)
        assert replay.status_code == 201 and replay.json()["id"] == str(identity)
        card = client.get(f"/api/v1/aggregates/{identity}").json()
        assert card["aggregate_rule"] == original_card["aggregate_rule"]
        assert card["rule_hash"] == original_card["rule_hash"]
        with api_database.session_factory() as session:
            stored = session.get(Aggregate, identity)
            assert (stored.request_key, stored.request_hash, stored.rule) == receipt
            assert session.scalar(select(func.count()).select_from(Aggregate).where(Aggregate.source_identity == api_source)) == 1
