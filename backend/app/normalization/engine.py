from __future__ import annotations

import csv
import hashlib
import json
from collections import OrderedDict
from datetime import datetime
from threading import RLock
from typing import Any
from uuid import UUID

from ..ecs import EcsCatalog
from .compiler import (
    CompiledRule,
    RuleValidationError,
    _bounded_depth,
    compile_rule,
)
from .convert import ConversionError, convert_value, parse_timestamp
from .schema import (
    MAX_CACHE_ENTRIES,
    MAX_JSON_DEPTH,
    MAX_LOG_BYTES,
    MAX_RULE_BYTES,
    ArrayRefsSource,
    ColumnsBlock,
    Diagnostic,
    JoinedRefsSource,
    JsonBlock,
    LiteralSource,
    MapBlock,
    NormalizationResult,
    RefSource,
    RegexBlock,
    TemplateBlock,
    TraceStep,
)


def _get_path(value: Any, path: str) -> Any:
    for segment in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(segment)
    return value


def _put_path(value: dict[str, Any], path: str, item: Any) -> None:
    parts = path.split(".")
    cursor = value
    for part in parts[:-1]:
        cursor = cursor.setdefault(part, {})
    cursor[parts[-1]] = item


def _missing(value: Any) -> bool:
    return value is None or value == "" or value == "-"


class NormalizationEngine:
    def __init__(self, catalog: EcsCatalog):
        self.catalog = catalog
        self._cache: OrderedDict[tuple[str, str, int | None, str], CompiledRule] = (
            OrderedDict()
        )
        self._lock = RLock()

    def compile(
        self, raw: dict[str, Any], *, normalizer_id: UUID | None = None,
        version: int | None = None,
    ) -> CompiledRule:
        if not isinstance(raw, dict) or not _bounded_depth(raw):
            raise RuleValidationError("Rule must be an object within nesting limits")
        try:
            serialized = json.dumps(raw, sort_keys=True, ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError, OverflowError, RecursionError) as error:
            raise RuleValidationError("Rule must contain finite JSON values") from error
        encoded = serialized.encode("utf-8")
        if len(encoded) > MAX_RULE_BYTES:
            raise RuleValidationError("Rule exceeds size or nesting limits")
        digest = hashlib.sha256(encoded).hexdigest()
        key = (
            "saved" if normalizer_id is not None else "preview",
            str(normalizer_id) if normalizer_id is not None else "",
            version,
            digest,
        )
        with self._lock:
            existing = self._cache.get(key)
            if existing is not None:
                self._cache.move_to_end(key)
                return existing
            compiled = compile_rule(raw, self.catalog)
            if normalizer_id is not None:
                for old_key in tuple(self._cache):
                    if old_key[0] == "saved" and old_key[1] == str(normalizer_id):
                        del self._cache[old_key]
            self._cache[key] = compiled
            while len(self._cache) > MAX_CACHE_ENTRIES:
                self._cache.popitem(last=False)
            return compiled

    def normalize(
        self, raw: dict[str, Any], envelope: object, *,
        normalizer_id: UUID | None = None, version: int | None = None,
    ) -> NormalizationResult:
        try:
            compiled = self.compile(raw, normalizer_id=normalizer_id, version=version)
        except RuleValidationError:
            return NormalizationResult(
                status="failed", diagnostics=[Diagnostic(code="rule_invalid")]
            )
        if not isinstance(envelope, dict) or set(envelope) != {"timestamp", "log"}:
            return NormalizationResult(
                status="failed", diagnostics=[Diagnostic(code="invalid_envelope")]
            )
        log = envelope["log"]
        if not isinstance(log, str):
            return NormalizationResult(
                status="failed", diagnostics=[Diagnostic(code="invalid_envelope")]
            )
        if len(log.encode("utf-8")) > MAX_LOG_BYTES:
            return NormalizationResult(
                status="failed", diagnostics=[Diagnostic(code="log_too_long")]
            )
        try:
            collected_at = parse_timestamp(envelope["timestamp"])
        except ConversionError:
            return NormalizationResult(
                status="failed", diagnostics=[Diagnostic(code="invalid_envelope")]
            )
        for variant in sorted(compiled.rule.variants, key=lambda item: item.priority):
            when = variant.when
            matches = (
                log.startswith(when.value) if when.kind == "prefix" else
                when.value in log if when.kind == "contains" else
                compiled.conditions[variant.key].search(log) is not None
            )
            if matches:
                return self._run_variant(compiled, variant, log, collected_at)
        return NormalizationResult(
            status="failed", fluent_bit_collected_at=collected_at,
            diagnostics=[Diagnostic(code="no_variant_matched")],
        )

    def _run_variant(
        self, compiled: CompiledRule, variant, log: str, collected_at: datetime,
    ) -> NormalizationResult:
        context: dict[str, dict[str, Any]] = {}
        ecs: dict[str, Any] = {
            "ecs": {"version": self.catalog.version},
            "event": {"original": log},
        }
        diagnostics: list[Diagnostic] = []
        trace: list[TraceStep] = []
        valid_mappings = 0
        failed = False
        for block in variant.blocks:
            if not isinstance(block, MapBlock):
                input_value = self._reference(block.input, log, context)
                output, candidate = self._extract(
                    compiled, variant.key, block, input_value
                )
                if output is not None:
                    context[block.key] = output
                trace.append(TraceStep(
                    variant=variant.key, block=block.key, candidate=candidate,
                    result="matched" if output is not None else "missing",
                ))
                continue
            raw_value = self._source_value(block, log, context)
            if _missing(raw_value):
                code = "required_mapping_missing" if block.required else "optional_mapping_missing"
                diagnostics.append(Diagnostic(
                    code=code, variant=variant.key, block=block.key, field=block.target,
                ))
                trace.append(TraceStep(
                    variant=variant.key, block=block.key, result="missing"
                ))
                failed |= block.required
                continue
            try:
                field = compiled.fields[(variant.key, block.key)]
                converted = convert_value(field, raw_value, block.transform)
            except ConversionError:
                diagnostics.append(Diagnostic(
                    code="conversion_failed", variant=variant.key,
                    block=block.key, field=block.target,
                ))
                trace.append(TraceStep(
                    variant=variant.key, block=block.key, result="conversion_failed"
                ))
                failed |= block.required
                continue
            _put_path(ecs, block.target, converted)
            valid_mappings += 1
            trace.append(TraceStep(
                variant=variant.key, block=block.key, result="mapped"
            ))
        if valid_mappings == 0:
            diagnostics.append(Diagnostic(code="empty_ecs", variant=variant.key))
            failed = True
        if failed:
            return NormalizationResult(
                status="failed", variant_key=variant.key,
                fluent_bit_collected_at=collected_at,
                diagnostics=diagnostics, trace=trace,
            )
        if "@timestamp" not in ecs:
            ecs["@timestamp"] = collected_at.isoformat().replace("+00:00", "Z")
            diagnostics.append(Diagnostic(code="event_time_fallback", variant=variant.key))
        return NormalizationResult(
            status="partial" if diagnostics else "complete",
            variant_key=variant.key, ecs_data=ecs,
            fluent_bit_collected_at=collected_at,
            diagnostics=diagnostics, trace=trace,
        )

    @staticmethod
    def _reference(reference: str, log: str, context: dict[str, dict[str, Any]]) -> Any:
        if reference == "log":
            return log
        block, field = reference.split(".", 1)
        return context.get(block, {}).get(field)

    def _source_value(self, block: MapBlock, log: str, context) -> Any:
        source = block.source.root
        if isinstance(source, LiteralSource):
            return source.literal
        if isinstance(source, RefSource):
            return self._reference(source.ref, log, context)
        values = [self._reference(ref, log, context) for ref in source.refs]
        if any(_missing(value) for value in values):
            return None
        if isinstance(source, ArrayRefsSource):
            return values
        if not all(isinstance(value, str) for value in values):
            return None
        assert isinstance(source, JoinedRefsSource)
        return source.join.join(values)

    def _extract(self, compiled: CompiledRule, variant_key: str, block, value):
        if not isinstance(value, str):
            return None, None
        if isinstance(block, JsonBlock):
            try:
                parsed = json.loads(value)
            except (json.JSONDecodeError, RecursionError):
                return None, None
            if not isinstance(parsed, dict) or not _bounded_depth(parsed, limit=MAX_JSON_DEPTH):
                return None, None
            for index, candidate in enumerate(block.candidates):
                if all(not _missing(_get_path(parsed, path)) for path in candidate.requires):
                    return {
                        **{name: _get_path(parsed, path) for name, path in candidate.extract.items()},
                        **candidate.set,
                    }, index
            return None, None
        for index, candidate in enumerate(block.candidates):
            if isinstance(block, (RegexBlock, TemplateBlock)):
                match = compiled.patterns[(variant_key, block.key, index)].search(value)
                if match is not None:
                    return {**match.groupdict(), "match": match.group(), **candidate.set}, index
            elif isinstance(block, ColumnsBlock):
                try:
                    row = next(csv.reader(
                        [value], delimiter=candidate.delimiter,
                        skipinitialspace=candidate.delimiter == " ", strict=True,
                    ))
                except (csv.Error, StopIteration):
                    continue
                if len(row) == len(candidate.columns):
                    return {**dict(zip(candidate.columns, row, strict=True)), **candidate.set}, index
        return None, None
