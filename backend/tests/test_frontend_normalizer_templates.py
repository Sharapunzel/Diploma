"""Keep the shipped starter rules executable against the real ECS catalog."""

import json
from pathlib import Path

import pytest

from app.ecs import PackagedEcsCatalog
from app.normalization.engine import NormalizationEngine
from app.normalization.schema import RuleV1


TEMPLATES = Path(__file__).resolve().parents[2] / "frontend" / "src" / "pages" / "normalizers" / "templates"
SAMPLES = {
    "nginx": ('192.0.2.10 - alice [01/Jan/2026:12:00:00 +0000] "GET /health HTTP/1.1" 200 12 "-" "Demo/1.0"', "source", "ip"),
    "iis": ("2026-01-01 12:00:00 192.0.2.1 GET /health - 443 - 198.51.100.4 Demo - 200 0 0 3", "source", "ip"),
    "syslog": ("Jan  1 12:00:00 host-a demo: Service started", "host", "name"),
    "json": ('{"event":{"action":"login"},"message":"Demo event"}', "event", "action"),
}


@pytest.mark.parametrize("name", sorted(SAMPLES))
def test_starter_template_compiles_and_maps_synthetic_log(name):
    rule = json.loads((TEMPLATES / f"{name}.json").read_text(encoding="utf-8"))
    engine = NormalizationEngine(PackagedEcsCatalog.load())
    engine.compile(rule)
    log, group, field = SAMPLES[name]
    result = engine.normalize(rule, {"timestamp": "2026-01-01T12:00:00Z", "log": log})
    assert result.status in {"complete", "partial"}
    assert result.ecs_data[group][field]
    assert result.ecs_data["event"]["original"] == log


def test_ai_prompt_block_examples_match_rule_v1_and_compile():
    prompt = (TEMPLATES.parent / "rules.ts").read_text(encoding="utf-8")
    blocks = {}
    for line in prompt.splitlines():
        if line.startswith('{"key":'):
            block = json.loads(line)
            blocks[block["kind"]] = block
    assert set(blocks) == {"regex", "template", "columns", "json", "map_ecs"}
    for kind in ("regex", "template", "columns", "json"):
        block = blocks[kind]
        assert set(block) == {"key", "kind", "input", "candidates"}
        RuleV1.model_validate({"format_version": 1, "variants": [{"key": "example", "priority": 10, "when": {"kind": "contains", "value": "demo"}, "blocks": [block]}]})

    assert blocks["json"]["input"] == "log"
    assert "request.path\",\"candidates\"" not in prompt
    assert "when.regex проверяют совпадение, но не создают выходов block.field" in prompt
    assert "priority: 100" in prompt and "независимо от порядка в массиве" in prompt
    assert "priority не обозначает тип или важность события" in prompt
    assert "ECS 9.4.0" in prompt
    assert "Сначала выясните недостающие сведения" in prompt
    assert "Режим разбора ошибки" in prompt

    lines = prompt.splitlines()
    example = json.loads(lines[lines.index("Связный пример RuleV1 (полный JSON)") + 1])
    sample = lines[lines.index("Связный пример RuleV1 (полный JSON)") + 2].removeprefix("Синтетический log: ")
    assert sample == "01/Jan/2026:12:00:00 +0000 GET /health HTTP/1.1"
    assert "Ожидаемый status: complete" in prompt
    assert example["variants"][0]["priority"] == 100
    assert example["variants"][0]["blocks"][1]["source"] == {"ref": "access.time"}
    assert example["variants"][0]["blocks"][0]["key"] == "access"
    RuleV1.model_validate(example)
    engine = NormalizationEngine(PackagedEcsCatalog.load())
    engine.compile(example)
    result = engine.normalize(example, {"timestamp": "2026-01-01T12:01:00Z", "log": sample})
    assert result.status == "complete", result.diagnostics
    assert result.variant_key == "access"
    assert result.ecs_data["@timestamp"] == "2026-01-01T12:00:00Z"
    assert result.ecs_data["http"]["request"]["method"] == "GET"
    assert result.ecs_data["url"]["path"] == "/health"

    nested = json.loads(next(line for line in lines if line.startswith('{"format_version":1,"variants":[{"key":"nested"')))
    RuleV1.model_validate(nested)
    engine.compile(nested)
    nested_result = engine.normalize(nested, {
        "timestamp": "2026-01-01T12:01:00Z",
        "log": 'payload={"event":{"action":"login"}}',
    })
    assert nested_result.status == "partial", nested_result.diagnostics
    assert nested_result.ecs_data["event"]["action"] == "login"


@pytest.mark.parametrize(
    ("source_date", "date_format", "timezone"),
    [
        ("2026-10-12 14:30:05 +0000", "%Y-%m-%d %H:%M:%S %z", None),
        ("2026-10-12 14:30:05", "%Y-%m-%d %H:%M:%S", "UTC"),
    ],
)
def test_date_preview_uses_explicit_source_zone_or_chosen_utc(source_date, date_format, timezone):
    transform = {"date_format": date_format}
    if timezone is not None:
        transform["timezone"] = timezone
    rule = {
        "format_version": 1,
        "variants": [{
            "key": "event", "priority": 10, "when": {"kind": "contains", "value": "2026"},
            "blocks": [
                {"key": "parsed", "kind": "regex", "input": "log", "candidates": [{"pattern": "^(?P<time>.+)$"}]},
                {"key": "time", "kind": "map_ecs", "target": "@timestamp",
                 "source": {"ref": "parsed.time"}, "required": True, "transform": transform},
            ],
        }],
    }
    engine = NormalizationEngine(PackagedEcsCatalog.load())
    engine.compile(rule)
    result = engine.normalize(rule, {"timestamp": "2026-10-12T15:00:00Z", "log": source_date})
    assert result.status == "complete", result.diagnostics
    assert result.ecs_data["@timestamp"] == "2026-10-12T14:30:05Z"
