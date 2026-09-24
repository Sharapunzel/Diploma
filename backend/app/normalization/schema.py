from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, RootModel, StringConstraints

Key = Annotated[str, StringConstraints(pattern=r"^[A-Za-z][A-Za-z0-9_]{0,63}$")]
ShortText = Annotated[str, StringConstraints(min_length=1, max_length=2048)]

MAX_RULE_BYTES = 65_536
MAX_LOG_BYTES = 65_536
MAX_JSON_DEPTH = 16
MAX_VARIANTS = 16
MAX_BLOCKS = 32
MAX_CANDIDATES = 8
MAX_PATTERN_LENGTH = 1024
MAX_CACHE_ENTRIES = 128

RULE_EXAMPLE = {
    "format_version": 1,
    "variants": [{
        "key": "web", "priority": 10,
        "when": {"kind": "prefix", "value": "2025-"},
        "blocks": [
            {"key": "columns", "kind": "columns", "candidates": [{
                "delimiter": " ", "columns": ["date", "time", "ip", "status"],
            }]},
            {"key": "source_ip", "kind": "map_ecs", "target": "source.ip",
             "source": {"ref": "columns.ip"}, "required": True},
            {"key": "status", "kind": "map_ecs", "target": "http.response.status_code",
             "source": {"ref": "columns.status"}, "required": True},
        ],
    }],
}

NESTED_RULE_EXAMPLE = {
    "format_version": 1,
    "variants": [{
        "key": "nested", "priority": 1,
        "when": {"kind": "contains", "value": "payload="},
        "blocks": [
            {"key": "outer", "kind": "regex", "candidates": [{
                "pattern": r"payload=(?P<body>\{.*\})",
            }]},
            {"key": "inner", "kind": "json", "input": "outer.body", "candidates": [{
                "requires": ["event.action"], "extract": {"action": "event.action"},
            }]},
            {"key": "action", "kind": "map_ecs", "target": "event.action",
             "source": {"ref": "inner.action"}, "required": True},
        ],
    }],
}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Condition(StrictModel):
    kind: Literal["prefix", "contains", "regex"]
    value: ShortText


class CandidateBase(StrictModel):
    set: dict[Key, str | int | float | bool] = Field(default_factory=dict, max_length=32)


class RegexCandidate(CandidateBase):
    pattern: str = Field(min_length=1, max_length=MAX_PATTERN_LENGTH)


class TemplateCandidate(CandidateBase):
    template: ShortText


class ColumnsCandidate(CandidateBase):
    delimiter: Literal[" ", ",", ";", "|", "\t"]
    columns: list[Key] = Field(min_length=1, max_length=64)


class JsonCandidate(CandidateBase):
    requires: list[ShortText] = Field(default_factory=list, max_length=32)
    extract: dict[Key, ShortText] = Field(min_length=1, max_length=32)


class ExtractBase(StrictModel):
    key: Key
    input: ShortText = "log"


class RegexBlock(ExtractBase):
    kind: Literal["regex"]
    candidates: list[RegexCandidate] = Field(min_length=1, max_length=MAX_CANDIDATES)


class TemplateBlock(ExtractBase):
    kind: Literal["template"]
    candidates: list[TemplateCandidate] = Field(min_length=1, max_length=MAX_CANDIDATES)


class ColumnsBlock(ExtractBase):
    kind: Literal["columns"]
    candidates: list[ColumnsCandidate] = Field(min_length=1, max_length=MAX_CANDIDATES)


class JsonBlock(ExtractBase):
    kind: Literal["json"]
    candidates: list[JsonCandidate] = Field(min_length=1, max_length=MAX_CANDIDATES)


class RefSource(StrictModel):
    ref: ShortText


class LiteralSource(StrictModel):
    literal: str | int | float | bool | list[Any] | dict[str, Any]


class JoinedRefsSource(StrictModel):
    refs: list[ShortText] = Field(min_length=1, max_length=32)
    join: str = Field(max_length=64)


class ArrayRefsSource(StrictModel):
    refs: list[ShortText] = Field(min_length=1, max_length=32)
    as_array: Literal[True]


class ValueSource(RootModel[RefSource | LiteralSource | JoinedRefsSource | ArrayRefsSource]):
    @classmethod
    def __get_pydantic_json_schema__(cls, core_schema, handler):
        schema = handler(core_schema)
        # The four strict object shapes cannot overlap, so oneOf expresses the
        # same validation as the generated union while documenting exclusivity.
        schema["oneOf"] = schema.pop("anyOf")
        return schema


class DateTransform(StrictModel):
    date_format: str = Field(min_length=1, max_length=128)
    timezone: Literal["UTC"] | None = None
    year: int | None = Field(default=None, ge=1, le=9999)


class MapBlock(StrictModel):
    key: Key
    kind: Literal["map_ecs"]
    target: ShortText
    source: ValueSource
    required: bool
    transform: DateTransform | None = None


Block = Annotated[
    RegexBlock | TemplateBlock | ColumnsBlock | JsonBlock | MapBlock,
    Field(discriminator="kind"),
]


class Variant(StrictModel):
    key: Key
    priority: int = Field(ge=0, le=1_000_000)
    when: Condition
    blocks: list[Block] = Field(min_length=1, max_length=MAX_BLOCKS)


class RuleV1(StrictModel):
    format_version: Literal[1]
    variants: list[Variant] = Field(min_length=1, max_length=MAX_VARIANTS)


class Diagnostic(StrictModel):
    code: str
    variant: str | None = None
    block: str | None = None
    field: str | None = None


class TraceStep(StrictModel):
    variant: str
    block: str
    candidate: int | None = None
    result: Literal["matched", "missing", "mapped", "conversion_failed"]


class NormalizationResult(StrictModel):
    status: Literal["complete", "partial", "failed"]
    variant_key: str | None = None
    ecs_data: dict[str, Any] | None = None
    fluent_bit_collected_at: datetime | None = None
    diagnostics: list[Diagnostic] = Field(default_factory=list)
    trace: list[TraceStep] = Field(default_factory=list)


class PreviewRequest(StrictModel):
    rule: RuleV1 = Field(json_schema_extra={"examples": [RULE_EXAMPLE, NESTED_RULE_EXAMPLE]})
    sample: dict[str, Any] = Field(json_schema_extra={"examples": [{
        "timestamp": "2025-06-01T12:01:00Z", "log": "2025-06-01 12:00:00 192.0.2.9 404",
    }]})
