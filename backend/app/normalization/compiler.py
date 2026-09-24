from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import re2
from pydantic import ValidationError

from ..ecs import STRING_TYPES, EcsCatalog
from .convert import ConversionError, convert_value, date_format_directives
from .schema import (
    MAX_JSON_DEPTH,
    MAX_PATTERN_LENGTH,
    MAX_RULE_BYTES,
    ArrayRefsSource,
    ColumnsBlock,
    JoinedRefsSource,
    JsonBlock,
    LiteralSource,
    MapBlock,
    RefSource,
    RegexBlock,
    RuleV1,
    TemplateBlock,
)


class RuleValidationError(ValueError):
    def __init__(
        self, message: str, *, variant: str | None = None,
        block: str | None = None, parameter: str | None = None,
    ):
        super().__init__(message)
        self.details = {
            key: value for key, value in (
                ("variant", variant), ("block", block), ("parameter", parameter)
            ) if value is not None
        }


@dataclass(frozen=True)
class CompiledRule:
    rule: RuleV1
    conditions: dict[str, Any]
    patterns: dict[tuple[str, str, int], Any]
    fields: dict[tuple[str, str], Any]


def _bounded_depth(value: object, *, limit: int = MAX_JSON_DEPTH) -> bool:
    pending = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if depth > limit:
            return False
        if isinstance(item, dict):
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
    return True


def _compile_regex(pattern: str, variant: str, block: str | None, parameter: str):
    if len(pattern) > MAX_PATTERN_LENGTH:
        raise RuleValidationError(
            "RE2 pattern exceeds the length limit",
            variant=variant, block=block, parameter=parameter,
        )
    options = re2.Options()
    options.max_mem = 1_048_576
    options.log_errors = False
    try:
        return re2.compile(pattern, options=options)
    except re2.error as error:
        raise RuleValidationError(
            "RE2 pattern is invalid or unsupported",
            variant=variant, block=block, parameter=parameter,
        ) from error


def _template_pattern(template: str, variant: str, block: str) -> tuple[str, set[str]]:
    result = ["^"]
    names: set[str] = set()
    cursor = 0
    while cursor < len(template):
        marker = template.find("%{", cursor)
        if marker < 0:
            result.append(re2.escape(template[cursor:]))
            break
        result.append(re2.escape(template[cursor:marker]))
        end = template.find("}", marker + 2)
        name = template[marker + 2:end] if end >= 0 else ""
        if (
            not name or len(name) > 64 or not name[0].isalpha()
            or not all(character.isalnum() or character == "_" for character in name)
            or name in names
        ):
            raise RuleValidationError(
                "Template placeholder is invalid or repeated",
                variant=variant, block=block, parameter="candidates.template",
            )
        names.add(name)
        result.append(f"(?P<{name}>.*?)")
        cursor = end + 1
    result.append("$")
    return "".join(result), names


def _path_valid(path: str) -> bool:
    parts = path.split(".")
    return 1 <= len(parts) <= 8 and all(
        part and len(part) <= 64 and part[0].isalpha()
        and all(character.isalnum() or character == "_" for character in part)
        for part in parts
    )


def _localized_pydantic_error(error: ValidationError, raw: dict[str, Any]) -> RuleValidationError:
    first = error.errors()[0]
    location = first["loc"]
    variant = None
    block = None
    if len(location) > 1 and location[0] == "variants" and isinstance(location[1], int):
        variants = raw.get("variants")
        if isinstance(variants, list) and location[1] < len(variants):
            selected = variants[location[1]]
            if isinstance(selected, dict):
                variant = selected.get("key")
                if len(location) > 3 and location[2] == "blocks" and isinstance(location[3], int):
                    blocks = selected.get("blocks")
                    if isinstance(blocks, list) and location[3] < len(blocks):
                        selected_block = blocks[location[3]]
                        if isinstance(selected_block, dict):
                            block = selected_block.get("key")
    parameter = ".".join(str(part) for part in location if not isinstance(part, int))
    return RuleValidationError(
        "Rule structure is invalid",
        variant=variant if isinstance(variant, str) else None,
        block=block if isinstance(block, str) else None,
        parameter=parameter,
    )


def _check_reference(
    reference: str, outputs: dict[str, set[str]], variant: str,
    block: str, parameter: str,
) -> None:
    if reference == "log":
        return
    parts = reference.split(".")
    if len(parts) != 2 or parts[1] not in outputs.get(parts[0], set()):
        raise RuleValidationError(
            "Reference must point to a field of an earlier extraction block",
            variant=variant, block=block, parameter=parameter,
        )


def _validate_map(
    block: MapBlock, variant_key: str, catalog: EcsCatalog,
    outputs: dict[str, set[str]], used_sources: set[str], targets: set[str],
):
    field = catalog.get(block.target)
    if field is None:
        raise RuleValidationError(
            "ECS field is unknown", variant=variant_key, block=block.key,
            parameter="target",
        )
    if not field.mappable:
        raise RuleValidationError(
            f"ECS field cannot be mapped: {field.mappable_reason}",
            variant=variant_key, block=block.key, parameter="target",
        )
    if block.target in targets:
        raise RuleValidationError(
            "ECS target is mapped more than once",
            variant=variant_key, block=block.key, parameter="target",
        )
    targets.add(block.target)
    source = block.source.root
    if isinstance(source, RefSource):
        references = [source.ref]
    elif isinstance(source, (JoinedRefsSource, ArrayRefsSource)):
        references = source.refs
    else:
        references = []
    for reference in references:
        _check_reference(reference, outputs, variant_key, block.key, "source")
        if reference != "log":
            if reference in used_sources:
                raise RuleValidationError(
                    "Extracted field is mapped to multiple ECS targets",
                    variant=variant_key, block=block.key, parameter="source",
                )
            used_sources.add(reference)
    if len(references) != len(set(references)):
        raise RuleValidationError(
            "Source references must be distinct",
            variant=variant_key, block=block.key, parameter="source.refs",
        )
    if "log" in references and field.type not in STRING_TYPES:
        raise RuleValidationError(
            "Direct log source is allowed only for string ECS fields",
            variant=variant_key, block=block.key, parameter="source",
        )
    if isinstance(source, JoinedRefsSource) and (
        (field.type not in STRING_TYPES and not (field.type == "date" and block.transform))
        or field.is_array
    ):
        raise RuleValidationError(
            "Concatenation requires a scalar string ECS field",
            variant=variant_key, block=block.key, parameter="source.join",
        )
    if isinstance(source, ArrayRefsSource) and not field.is_array:
        raise RuleValidationError(
            "Array combination requires an ECS array field",
            variant=variant_key, block=block.key, parameter="source.as_array",
        )
    if block.transform is not None:
        if field.type != "date":
            raise RuleValidationError(
                "Date transform requires an ECS date field",
                variant=variant_key, block=block.key, parameter="transform",
            )
        date_format = block.transform.date_format
        directives = date_format_directives(date_format)
        if directives is None:
            raise RuleValidationError(
                "Date format contains an unsupported directive",
                variant=variant_key, block=block.key, parameter="transform.date_format",
            )
        if (
            not ({"m", "b"} & directives)
            or "d" not in directives
            or not {"H", "M", "S"} <= directives
            or "Y" not in directives and block.transform.year is None
        ):
            parameter = (
                "transform.year" if "Y" not in directives and block.transform.year is None
                else "transform.date_format"
            )
            raise RuleValidationError(
                "Date format must contain year, month, day, hour, minute and second",
                variant=variant_key, block=block.key, parameter=parameter,
            )
        if "y" in directives or ("Y" not in directives and block.transform.year is None):
            raise RuleValidationError(
                "Date format requires a full year or explicit year policy",
                variant=variant_key, block=block.key, parameter="transform.year",
            )
        if "z" not in directives and block.transform.timezone is None:
            raise RuleValidationError(
                "Date format requires a timezone or explicit UTC policy",
                variant=variant_key, block=block.key, parameter="transform.timezone",
            )
    if isinstance(source, LiteralSource):
        try:
            convert_value(field, source.literal, block.transform)
        except ConversionError as error:
            raise RuleValidationError(
                "Constant cannot be converted to target ECS type",
                variant=variant_key, block=block.key, parameter="source.literal",
            ) from error
    return field


def compile_rule(raw: dict[str, Any], catalog: EcsCatalog) -> CompiledRule:
    if not isinstance(raw, dict) or not _bounded_depth(raw):
        raise RuleValidationError("Rule must be an object within nesting limits")
    try:
        serialized = json.dumps(raw, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError, OverflowError, RecursionError) as error:
        raise RuleValidationError("Rule must contain finite JSON values") from error
    if len(serialized.encode("utf-8")) > MAX_RULE_BYTES:
        raise RuleValidationError("Rule exceeds size or nesting limits")
    if any(isinstance(item, str) and len(item) > 2048 for item in _walk_values(raw)):
        raise RuleValidationError("Rule string exceeds the 2048 character limit")
    try:
        rule = RuleV1.model_validate(raw)
    except ValidationError as error:
        raise _localized_pydantic_error(error, raw) from error

    priorities: set[int] = set()
    variant_keys: set[str] = set()
    conditions: dict[str, Any] = {}
    patterns: dict[tuple[str, str, int], Any] = {}
    fields: dict[tuple[str, str], Any] = {}
    for variant in rule.variants:
        if variant.key in variant_keys or variant.priority in priorities:
            raise RuleValidationError(
                "Variant keys and priorities must be unique",
                variant=variant.key, parameter="key/priority",
            )
        variant_keys.add(variant.key)
        priorities.add(variant.priority)
        if variant.when.kind == "regex":
            conditions[variant.key] = _compile_regex(
                variant.when.value, variant.key, None, "when.value"
            )
        outputs: dict[str, set[str]] = {}
        seen_blocks: set[str] = set()
        used_sources: set[str] = set()
        targets: set[str] = set()
        for block in variant.blocks:
            if block.key in seen_blocks:
                raise RuleValidationError(
                    "Block keys must be unique", variant=variant.key,
                    block=block.key, parameter="key",
                )
            seen_blocks.add(block.key)
            if isinstance(block, MapBlock):
                fields[(variant.key, block.key)] = _validate_map(
                    block, variant.key, catalog, outputs, used_sources, targets
                )
                continue
            _check_reference(block.input, outputs, variant.key, block.key, "input")
            names: set[str] = set()
            for index, candidate in enumerate(block.candidates):
                candidate_names: set[str]
                if isinstance(block, RegexBlock):
                    pattern = _compile_regex(
                        candidate.pattern, variant.key, block.key,
                        f"candidates.{index}.pattern",
                    )
                    patterns[(variant.key, block.key, index)] = pattern
                    candidate_names = set(pattern.groupindex) | {"match"}
                elif isinstance(block, TemplateBlock):
                    expression, candidate_names = _template_pattern(
                        candidate.template, variant.key, block.key
                    )
                    patterns[(variant.key, block.key, index)] = _compile_regex(
                        expression, variant.key, block.key,
                        f"candidates.{index}.template",
                    )
                    candidate_names.add("match")
                elif isinstance(block, ColumnsBlock):
                    if len(candidate.columns) != len(set(candidate.columns)):
                        raise RuleValidationError(
                            "Column names must be unique", variant=variant.key,
                            block=block.key, parameter=f"candidates.{index}.columns",
                        )
                    candidate_names = set(candidate.columns)
                elif isinstance(block, JsonBlock):
                    paths = [*candidate.requires, *candidate.extract.values()]
                    if not all(_path_valid(path) for path in paths):
                        raise RuleValidationError(
                            "JSON path is invalid", variant=variant.key,
                            block=block.key, parameter=f"candidates.{index}.extract",
                        )
                    candidate_names = set(candidate.extract)
                else:
                    raise TypeError("unexpected extraction block")
                if candidate_names & set(candidate.set):
                    raise RuleValidationError(
                        "Candidate constants conflict with extracted names",
                        variant=variant.key, block=block.key,
                        parameter=f"candidates.{index}.set",
                    )
                names.update(candidate_names)
                names.update(candidate.set)
            outputs[block.key] = names
        if not targets:
            raise RuleValidationError(
                "Variant requires at least one ECS mapping",
                variant=variant.key, parameter="blocks",
            )
    return CompiledRule(rule, conditions, patterns, fields)


def _walk_values(value: object):
    pending = [value]
    while pending:
        item = pending.pop()
        yield item
        if isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
