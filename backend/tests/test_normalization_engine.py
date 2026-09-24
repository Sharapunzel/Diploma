"""Synthetic public-format logs; no Kafka or storage is involved in normalization."""

from copy import deepcopy
from uuid import uuid4

import pytest

from app.ecs import PackagedEcsCatalog
from app.normalization.compiler import RuleValidationError
from app.normalization.engine import NormalizationEngine
from app.normalization.schema import NESTED_RULE_EXAMPLE, RULE_EXAMPLE

COLLECTED = "2025-06-01T12:00:00+00:00"


@pytest.fixture(scope="module")
def engine():
    return NormalizationEngine(PackagedEcsCatalog.load())


def mapping(key, target, source, required=True, transform=None):
    block = {"key": key, "kind": "map_ecs", "target": target,
             "source": source, "required": required}
    if transform is not None:
        block["transform"] = transform
    return block


def rule(*variants):
    return {"format_version": 1, "variants": list(variants)}


def variant(key, priority, when, blocks):
    return {"key": key, "priority": priority, "when": when, "blocks": blocks}


def sample(log, timestamp=COLLECTED):
    return {"timestamp": timestamp, "log": log}


@pytest.mark.parametrize(("log", "kind", "candidate", "maps", "expected"), [
    (
        '192.0.2.1 - alice [01/Jun/2025:12:00:00 +0000] "GET /x HTTP/1.1" 200 123',
        "regex", {"pattern": r'(?P<ip>\S+) \S+ (?P<user>\S+) \[(?P<time>[^]]+)\] "[^"]+" (?P<status>\d+) \d+'},
        [mapping("ip", "source.ip", {"ref": "parse.ip"}),
         mapping("user", "user.name", {"ref": "parse.user"}),
         mapping("status", "http.response.status_code", {"ref": "parse.status"}),
         mapping("time", "@timestamp", {"ref": "parse.time"},
                 transform={"date_format": "%d/%b/%Y:%H:%M:%S %z"})],
        ("complete", 200),
    ),
    (
        '2025/06/01 12:00:00 [error] 42#42: upstream timed out',
        "template", {"template": "%{time} [%{level}] %{message}"},
        [mapping("level", "log.level", {"ref": "parse.level"}),
         mapping("message", "message", {"ref": "parse.message"})],
        ("partial", None),
    ),
    (
        "2025-06-01 12:00:00 192.0.2.9 404",
        "columns", {"delimiter": " ", "columns": ["date", "time", "ip", "status"]},
        [mapping("ip", "source.ip", {"ref": "parse.ip"}),
         mapping("status", "http.response.status_code", {"ref": "parse.status"}),
         mapping("time", "@timestamp", {"refs": ["parse.date", "parse.time"], "join": " "},
                 transform={"date_format": "%Y-%m-%d %H:%M:%S", "timezone": "UTC"})],
        ("complete", 404),
    ),
    (
        '<34>1 2025-06-01T12:00:00Z host app 123 ID47 - user created',
        "regex", {"pattern": r'<\d+>1 (?P<time>\S+) \S+ \S+ \S+ \S+ - (?P<message>.*)'},
        [mapping("time", "@timestamp", {"ref": "parse.time"}),
         mapping("message", "message", {"ref": "parse.message"})],
        ("complete", None),
    ),
    (
        '<34>Jun  1 12:00:00 host app: user deleted',
        "regex", {"pattern": r'<\d+>(?P<time>\w+\s+\d+ \d\d:\d\d:\d\d) \S+ \S+: (?P<message>.*)'},
        [mapping("time", "@timestamp", {"ref": "parse.time"}, required=False,
                 transform={"date_format": "%b %d %H:%M:%S", "year": 2025, "timezone": "UTC"}),
         mapping("message", "message", {"ref": "parse.message"})],
        ("complete", None),
    ),
    (
        '{"event":{"action":"login"},"actor":{"name":"alice"}}',
        "json", {"requires": ["event.action"], "extract": {"action": "event.action", "user": "actor.name"}},
        [mapping("action", "event.action", {"ref": "parse.action"}),
         mapping("user", "user.name", {"ref": "parse.user"})],
        ("partial", None),
    ),
    (
        '{"action":"logout","user":"bob"}',
        "json", {"requires": ["action"], "extract": {"action": "action", "user": "user"}},
        [mapping("action", "event.action", {"ref": "parse.action"}),
         mapping("user", "user.name", {"ref": "parse.user"})],
        ("partial", None),
    ),
])
def test_public_formats(engine, log, kind, candidate, maps, expected):
    configured = rule(variant("format", 1, {"kind": "prefix", "value": log[0]},
                              [{"key": "parse", "kind": kind, "candidates": [candidate]}, *maps]))
    result = engine.normalize(configured, sample(log))
    assert result.status == expected[0], result.model_dump()
    assert result.ecs_data["event"]["original"] == log
    assert result.fluent_bit_collected_at.isoformat() == COLLECTED
    if expected[1] is not None:
        assert result.ecs_data["http"]["response"]["status_code"] == expected[1]
    if expected[0] == "partial":
        assert any(item.code == "event_time_fallback" for item in result.diagnostics)


def test_variant_priority_and_no_fallback_after_failed(engine):
    low = variant("fallback", 10, {"kind": "contains", "value": "user"},
                  [mapping("fallback_map", "message", {"ref": "log"})])
    first = variant("first", 1, {"kind": "prefix", "value": "user"}, [
        {"key": "parse", "kind": "regex", "candidates": [{"pattern": r"user (?P<name>\S+)"}]},
        mapping("required_map", "user.name", {"ref": "parse.name"}),
    ])
    configured = rule(low, first)
    assert engine.normalize(configured, sample("user alice")).variant_key == "first"
    failed = engine.normalize(configured, sample("user "))
    assert failed.status == "failed" and failed.variant_key == "first"
    assert failed.ecs_data is None
    assert engine.normalize(configured, sample("unrecognized")).diagnostics[0].code == "no_variant_matched"


def test_nested_extraction_and_candidate_order(engine):
    configured = rule(variant("nested", 1, {"kind": "contains", "value": "payload="}, [
        {"key": "outer", "kind": "regex", "candidates": [
            {"pattern": r"payload=(?P<body>\{.*\})"}]},
        {"key": "inner", "kind": "json", "input": "outer.body", "candidates": [
            {"requires": ["missing"], "extract": {"action": "missing"}},
            {"requires": ["action"], "extract": {"action": "action"}},
        ]},
        mapping("action", "event.action", {"ref": "inner.action"}),
    ]))
    result = engine.normalize(configured, sample('payload={"action":"created"}'))
    assert result.status == "partial"
    assert result.ecs_data["event"]["action"] == "created"
    assert result.trace[1].candidate == 1


def test_human_readable_create_delete_and_candidate_constants(engine):
    configured = rule(variant("accounts", 1, {"kind": "contains", "value": "user"}, [
        {"key": "parse", "kind": "regex", "candidates": [
            {"pattern": r"user (?P<name>\w+) created", "set": {"action": "creation"}},
            {"pattern": r"user (?P<name>\w+) deleted", "set": {"action": "deletion"}},
        ]},
        mapping("name", "user.name", {"ref": "parse.name"}),
        mapping("action", "event.action", {"ref": "parse.action"}),
    ]))
    first = engine.normalize(configured, sample("user alice created"))
    second = engine.normalize(configured, sample("user alice deleted"))
    assert first.ecs_data["event"]["action"] == "creation"
    assert second.ecs_data["event"]["action"] == "deletion"
    assert [first.trace[0].candidate, second.trace[0].candidate] == [0, 1]


def test_array_and_typed_conversion(engine):
    configured = rule(variant("v", 1, {"kind": "prefix", "value": "ip="}, [
        {"key": "parse", "kind": "regex", "candidates": [
            {"pattern": r"ip=(?P<first>\S+) (?P<second>\S+) (?P<status>\d+)"}]},
        mapping("addresses", "related.ip", {
            "refs": ["parse.first", "parse.second"], "as_array": True,
        }),
        mapping("status", "http.response.status_code", {"ref": "parse.status"}),
    ]))
    result = engine.normalize(configured, sample("ip=192.0.2.1 192.0.2.2 200"))
    assert result.ecs_data["related"]["ip"] == ["192.0.2.1", "192.0.2.2"]
    assert result.ecs_data["http"]["response"]["status_code"] == 200
    failed = engine.normalize(configured, sample("ip=bad 192.0.2.2 200"))
    assert failed.status == "failed" and failed.ecs_data is None
    assert any(item.code == "conversion_failed" for item in failed.diagnostics)


def test_optional_required_timestamp_and_invalid_envelope(engine):
    base = rule(variant("v", 1, {"kind": "prefix", "value": "user"}, [
        {"key": "parse", "kind": "regex", "candidates": [
            {"pattern": r"user (?P<date>.*)"}]},
        mapping("message", "message", {"ref": "log"}),
        mapping("time", "@timestamp", {"ref": "parse.date"}, required=False),
    ]))
    partial = engine.normalize(base, sample("user created"))
    assert partial.status == "partial" and partial.ecs_data["@timestamp"].startswith("2025")
    required = deepcopy(base)
    required["variants"][0]["blocks"][2]["required"] = True
    failed = engine.normalize(required, sample("user created"))
    assert failed.status == "failed" and failed.ecs_data is None
    assert engine.normalize(base, {"timestamp": "bad", "log": "user"}).diagnostics[0].code == "invalid_envelope"
    assert engine.normalize(base, sample("user" * 20_000)).diagnostics[0].code == "log_too_long"


@pytest.mark.parametrize("mutation", [
    lambda x: x.update(format_version=0),
    lambda x: x["variants"].append(deepcopy(x["variants"][0])),
    lambda x: x["variants"][0]["blocks"][0].update(target="event.original"),
    lambda x: x["variants"][0]["blocks"][0].update(target="not.ecs"),
    lambda x: x["variants"][0]["blocks"][0]["source"].update(ref="later.field"),
    lambda x: x["variants"][0]["blocks"][0].update(target="source.ip"),
])
def test_rule_validation_localized(engine, mutation):
    configured = rule(variant("v", 1, {"kind": "contains", "value": "user"}, [
        mapping("message", "message", {"ref": "log"}),
    ]))
    mutation(configured)
    with pytest.raises(RuleValidationError):
        engine.compile(configured)


def test_re2_rejects_backreference_and_cache_is_bounded(engine):
    configured = rule(variant("v", 1, {"kind": "regex", "value": r"(a)\1"}, [
        mapping("message", "message", {"ref": "log"}),
    ]))
    with pytest.raises(RuleValidationError) as error:
        engine.compile(configured)
    assert error.value.details == {"variant": "v", "parameter": "when.value"}
    configured["variants"][0]["when"] = {"kind": "prefix", "value": "a"}
    assert engine.compile(configured) is engine.compile(configured)


def test_rule_limits_and_invalid_references(engine):
    configured = rule(variant("v", 1, {"kind": "prefix", "value": "a"}, [
        mapping("message", "message", {"ref": "log"}),
    ]))
    oversized = deepcopy(configured)
    oversized["variants"][0]["when"]["value"] = "x" * 65_537
    with pytest.raises(RuleValidationError):
        engine.compile(oversized)
    cyclic = deepcopy(configured)
    cyclic["variants"][0]["blocks"].insert(0, {
        "key": "parse", "kind": "regex", "input": "parse.match",
        "candidates": [{"pattern": "a"}],
    })
    with pytest.raises(RuleValidationError) as error:
        engine.compile(cyclic)
    assert error.value.details["block"] == "parse"
    duplicate_target = deepcopy(configured)
    duplicate_target["variants"][0]["blocks"].append(
        mapping("other", "message", {"literal": "also"})
    )
    with pytest.raises(RuleValidationError) as error:
        engine.compile(duplicate_target)
    assert error.value.details["parameter"] == "target"


def test_saved_rule_cache_replaced_by_new_version(engine):
    configured = rule(variant("v", 1, {"kind": "prefix", "value": "first"}, [
        mapping("message", "message", {"ref": "log"}),
    ]))
    normalizer_id = uuid4()
    first = engine.compile(configured, normalizer_id=normalizer_id, version=1)
    changed = deepcopy(configured)
    changed["variants"][0]["when"]["value"] = "second"
    second = engine.compile(changed, normalizer_id=normalizer_id, version=2)
    assert first is not second
    assert len([key for key in engine._cache if key[0] == "saved" and key[1] == str(normalizer_id)]) == 1
    assert engine.normalize(changed, sample("second value"), normalizer_id=normalizer_id,
                            version=2).status == "partial"


def test_date_format_requires_explicit_year_and_timezone(engine):
    configured = rule(variant("v", 1, {"kind": "prefix", "value": "Jun"}, [
        {"key": "parse", "kind": "regex", "candidates": [
            {"pattern": r"(?P<time>Jun 1 12:00:00)"}]},
        mapping("time", "@timestamp", {"ref": "parse.time"}, transform={
            "date_format": "%b %d %H:%M:%S",
        }),
    ]))
    with pytest.raises(RuleValidationError) as error:
        engine.compile(configured)
    assert error.value.details["parameter"] == "transform.year"
    configured["variants"][0]["blocks"][1]["transform"]["year"] = 2025
    with pytest.raises(RuleValidationError) as error:
        engine.compile(configured)
    assert error.value.details["parameter"] == "transform.timezone"
    configured["variants"][0]["blocks"][1]["transform"]["timezone"] = "UTC"
    assert engine.normalize(configured, sample("Jun 1 12:00:00")).status == "complete"


def test_rfc3164_without_year_uses_explicit_fallback(engine):
    configured = rule(variant("legacy_syslog", 1, {"kind": "prefix", "value": "<34>"}, [
        {"key": "parse", "kind": "regex", "candidates": [{
            "pattern": r"<\d+>\w+\s+\d+ \d\d:\d\d:\d\d \S+ \S+: (?P<message>.*)",
        }]},
        mapping("message", "message", {"ref": "parse.message"}),
    ]))
    result = engine.normalize(configured, sample("<34>Jun  1 12:00:00 host app: user deleted"))
    assert result.status == "partial"
    assert result.ecs_data["@timestamp"] == "2025-06-01T12:00:00Z"
    assert any(item.code == "event_time_fallback" for item in result.diagnostics)


def test_rule_error_is_not_a_nonmatching_log(engine):
    invalid = {"format_version": 0, "legacy_rule_text": "opaque"}
    result = engine.normalize(invalid, sample("anything"))
    assert result.status == "failed"
    assert [item.code for item in result.diagnostics] == ["rule_invalid"]


def test_incomplete_event_time_is_rejected_without_fallback(engine):
    configured = rule(variant("v", 1, {"kind": "prefix", "value": "year="}, [
        mapping("time", "@timestamp", {"literal": "2025"}, required=True,
                transform={"date_format": "%Y", "timezone": "UTC"}),
    ]))
    with pytest.raises(RuleValidationError) as error:
        engine.compile(configured)
    assert error.value.details["parameter"] == "transform.date_format"

    optional = rule(variant("v", 1, {"kind": "prefix", "value": "year="}, [
        mapping("message", "message", {"ref": "log"}),
        {"key": "parse", "kind": "regex", "candidates": [{"pattern": r"year=(?P<value>.*)"}]},
        mapping("time", "@timestamp", {"ref": "parse.value"}, required=False,
                transform={"date_format": "%Y-%m-%d %H:%M:%S", "timezone": "UTC"}),
    ]))
    result = engine.normalize(optional, sample("year=anything"))
    assert result.status == "partial" and result.ecs_data is not None
    assert any(item.code == "conversion_failed" for item in result.diagnostics)


def test_openapi_rule_examples_execute(engine):
    for configured, log in (
        (RULE_EXAMPLE, "2025-06-01 12:00:00 192.0.2.9 404"),
        (NESTED_RULE_EXAMPLE, 'payload={"event":{"action":"login"}}'),
    ):
        assert engine.normalize(configured, sample(log)).status == "partial"
