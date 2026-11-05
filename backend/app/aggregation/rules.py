"""Closed AST and extensible operator contracts for aggregate rules."""

import hashlib
import ipaddress
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictFloat, StrictInt, StrictStr

from ..ecs import NUMBER_TYPES, EcsCatalog, EcsField

LiteralValue = StrictStr | StrictBool | StrictInt | StrictFloat
STRING_TYPES = frozenset({"keyword", "constant_keyword", "wildcard", "text", "match_only_text"})
SCALAR_TYPES = STRING_TYPES | {"boolean", "date", "ip"} | NUMBER_TYPES
DATE_LOCAL_PATTERN = r"[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]{1,6})?"
DATE_OFFSET_PATTERN = r"(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9](\.[0-9]{1,6})?)?)"
DATE_PATTERN = "^" + DATE_LOCAL_PATTERN + DATE_OFFSET_PATTERN + "$"
DATE_UTC_MIN = datetime.min.replace(tzinfo=UTC)
DATE_UTC_MAX = datetime.max.replace(tzinfo=UTC)
INTEGER_TYPES = frozenset({"long", "integer", "short", "byte"})
INTEGER_BITS = {"long": 64, "integer": 32, "short": 16, "byte": 8}
LEGACY_OPERATORS = frozenset({"all", "any", "eq", "in", "exists", "not_exists", "has_any",
    "count", "distinct_count", "sum", "avg", "min", "max", "derive",
    "add", "subtract", "multiply", "divide"})


class RuleError(ValueError):
    def __init__(self, code: str, path: str, hint: str = "") -> None:
        self.code, self.path, self.hint = code, path, hint
        super().__init__(f"{code}: {path}" + (f" ({hint})" if hint else ""))


class Node(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AggregateCondition(Node):
    op: str
    field: str | None = None
    value: LiteralValue | list[LiteralValue] | None = None
    args: list["AggregateCondition"] | None = None


class Expression(Node):
    op: str | None = None
    args: list["Expression"] | None = None
    ref: str | None = None
    value: StrictInt | StrictFloat | None = None


class Feature(Node):
    key: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")]
    name: Annotated[str, Field(min_length=1, max_length=200)]
    op: str
    field: str | None = None
    where: AggregateCondition | None = None
    expr: Expression | None = None
    percentile: StrictInt | StrictFloat | None = None


class AggregateRule(Node):
    where: AggregateCondition | None = None
    features: Annotated[list[Feature], Field(min_length=1)]


class FeatureResult(Node):
    value: int | float | None
    reason: str | None
    valid_count: int = Field(ge=0)
    invalid_count: int = Field(ge=0)


@dataclass(frozen=True)
class Operator:
    name: str
    kind: Literal["condition", "feature", "expression"]
    inputs: tuple[str, ...]
    parameters: tuple[str, ...]
    weight: int
    result: str


# Validation and compiler handlers share a closed registry; no unknown-op fallback.
OPERATORS = {
    op.name: op for op in [
        *[Operator(n, "condition", ("condition",), ("args",), 1, "boolean")
          for n in ("all", "any")],
        *[Operator(n, "condition", ("scalar",), ("field", "value"), 1, "boolean")
          for n in ("eq", "in")],
        *[Operator(n, "condition", ("scalar", "typed_array"), ("field",), 1, "boolean")
          for n in ("exists", "not_exists")],
        Operator("has_any", "condition", ("typed_array",), ("field", "value"), 1, "boolean"),
        Operator("count", "feature", ("records",), (), 1, "number"),
        Operator("distinct_count", "feature", ("scalar",), ("field",), 8, "number"),
        *[Operator(n, "feature", ("number",), ("field",), 2, "number")
          for n in ("sum", "avg", "min", "max")],
        Operator("derive", "feature", ("expression",), ("expr",), 0, "number"),
        *[Operator(n, "expression", ("number",), ("args",), 1, "number")
          for n in ("add", "subtract", "multiply", "divide")],
    ]
}

for names, kind, inputs, parameters, weight in [
    (("not",), "condition", ("condition",), ("args",), 1),
    (("neq", "not_in"), "condition", ("scalar",), ("field", "value"), 1),
    (("gt", "gte", "lt", "lte", "between"), "condition", ("number", "date"), ("field", "value"), 1),
    (("contains", "starts_with"), "condition", ("string",), ("field", "value"), 2),
    (("in_cidr",), "condition", ("ip",), ("field", "value"), 2),
    (("has_all", "has_none"), "condition", ("typed_array",), ("field", "value"), 2),
    (("present_count", "missing_count"), "feature", ("scalar", "typed_array"), ("field",), 1),
    (("true_count", "false_count"), "feature", ("boolean",), ("field",), 1),
    (("range", "variance", "stddev"), "feature", ("number",), ("field",), 4),
    (("median",), "feature", ("number",), ("field",), 12),
    (("percentile",), "feature", ("number",), ("field", "percentile"), 12),
    (("mode_count", "mode_share"), "feature", ("scalar",), ("field",), 12),
    (("ratio", "percent", "per_second", "abs", "clamp", "min_of", "max_of"),
     "expression", ("number",), ("args",), 1),
]:
    for name in names:
        OPERATORS[name] = Operator(name, kind, inputs, parameters, weight, "boolean" if kind == "condition" else "number")


def compatible(op: Operator, field: EcsField) -> bool:
    if "field" not in op.parameters:
        return False
    if field.is_array:
        return "typed_array" in op.inputs and field.type in SCALAR_TYPES
    return ("scalar" in op.inputs and field.type in SCALAR_TYPES
            or "number" in op.inputs and field.type in NUMBER_TYPES
            or "string" in op.inputs and field.type in STRING_TYPES
            or field.type in op.inputs)


def arity(name: str) -> tuple[int, int | None]:
    if name in {"not", "percent", "per_second", "abs"}:
        return 1, 1
    if name == "clamp":
        return 3, 3
    if name in {"all", "any"}:
        return 1, None
    if name in {"min_of", "max_of"}:
        return 2, None
    return 2, 2


@dataclass(frozen=True)
class Limits:
    json_bytes: int = 65536
    depth: int = 8
    literals: int = 256
    preview_rows: int = 100000
    cost: int = 50000000
    preview_timeout_ms: int = 10000
    working_timeout_ms: int = 30000
    work_mem_mb: int = 32
    total_memory_mb: int = 256
    array_elements: int = 4096


@dataclass(frozen=True)
class CompiledRule:
    rule: AggregateRule
    fields: dict[str, EcsField]
    canonical: dict
    sha256: str
    weight: int
    array_elements: int = 4096
    extended_input: bool = False


def operator_catalog() -> list[dict]:
    result = []
    for op in OPERATORS.values():
        types = {"field": "catalogued_ecs_field", "value": "strict_field_typed_literal",
                 "args": "condition[]" if op.kind == "condition" else "expression[]",
                 "expr": "expression", "percentile": "number[0,1]"}
        if op.name in {"in", "not_in", "has_any", "has_all", "has_none"}:
            types["value"] = "nonempty_homogeneous_field_typed_literal[]"
        if op.name == "between":
            types["value"] = "ordered_pair_inclusive"
        if op.name == "in_cidr":
            types["value"] = "IPv4_or_IPv6_CIDR"
        low, high = arity(op.name)
        result.append({**asdict(op), "parameter_types": {name: types[name] for name in op.parameters},
                       "weight_formula": ("weight * len(value)" if op.name in {"has_all", "has_none"} else
                           "weight for keyword/constant_keyword; weight * len(value) for other element types"
                           if op.name == "has_any" else "weight"),
                       "arity": {"min": low, "max": high} if "args" in op.parameters else None,
                       "compatible_fields": [{"type": t, "is_array": a}
                           for t in sorted(SCALAR_TYPES) for a in (False, True)
                           if compatible(op, EcsField("", t, "", "", a, "", False, ()))],
                       "result_nullable": op.kind in {"feature", "expression"}})
    return result


def scalar_key(value: object, ecs_type: str) -> object:
    """Typed category identity independent of local timezone and DST fold."""
    if ecs_type == "date":
        return date_utc(value)
    return value


def date_utc(value: str | datetime) -> datetime:
    """Return a representable absolute moment, never a local-time fallback."""
    moment = datetime.fromisoformat(value) if isinstance(value, str) else value
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("date_requires_offset")
    try:
        return moment.astimezone(UTC)
    except (OverflowError, ValueError) as error:
        raise ValueError("date_utc_out_of_range") from error


def _shape(node: Node, required: set[str], optional: set[str], path: str) -> None:
    keys = {key for key in node.model_fields_set if getattr(node, key) is not None}
    if not required <= keys or keys - required - optional:
        raise RuleError("invalid_arguments", path)


def literal_matches(value: object, field: EcsField) -> bool:
    if field.type in INTEGER_TYPES:
        bits = INTEGER_BITS[field.type]
        return type(value) is int and -(2**(bits - 1)) <= value < 2**(bits - 1)
    if field.type in NUMBER_TYPES:
        try:
            return type(value) in (int, float) and math.isfinite(value)
        except OverflowError:
            return False
    if field.type == "boolean":
        return type(value) is bool
    if field.type == "date":
        if not isinstance(value, str) or not re.fullmatch(DATE_PATTERN, value):
            return False
        try:
            date_utc(value)
            return True
        except ValueError:
            return False
    if field.type == "ip":
        try:
            return isinstance(value, str) and "%" not in value and bool(ipaddress.ip_address(value))
        except ValueError:
            return False
    return type(value) is str


def field_support_reason(field: EcsField, catalog: EcsCatalog) -> str | None:
    if field.type not in SCALAR_TYPES:
        return "unsupported_field_type"
    parts = field.name.split(".")
    for i in range(1, len(parts)):
        parent = catalog.get(".".join(parts[:i]))
        if parent and (parent.is_array or parent.type in {"nested", "flattened"}):
            return "unsupported_parent_field_type"
    return None


def compile_rule(rule: AggregateRule, catalog: EcsCatalog,
                 limits: Limits = Limits()) -> CompiledRule:
    canonical = rule.model_dump(exclude_none=True)
    try:
        payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (ValueError, OverflowError) as error:
        raise RuleError("non_finite_literal", "aggregate_rule") from error
    if len(payload) > limits.json_bytes:
        raise RuleError("json_budget_exceeded", "aggregate_rule", "Уменьшите размер правила")
    fields: dict[str, EcsField] = {}
    weight = 0
    extended_input = False

    def operator(name: str, kind: str, path: str) -> None:
        nonlocal weight, extended_input
        entry = OPERATORS.get(name)
        if entry is None or entry.kind != kind:
            raise RuleError("unknown_operator", path)
        weight += entry.weight
        extended_input |= name not in LEGACY_OPERATORS

    def field(name: str | None, op: str, path: str) -> EcsField:
        nonlocal extended_input
        found = catalog.get(name) if name else None
        if found is None:
            raise RuleError("unknown_field", path)
        reason = field_support_reason(found, catalog)
        if reason:
            raise RuleError(reason, path, found.name)
        if not compatible(OPERATORS[op], found):
            raise RuleError("unsupported_field_type", path, found.name)
        fields[found.name] = found
        extended_input |= found.type not in ({"keyword", "constant_keyword", "boolean"} | NUMBER_TYPES)
        extended_input |= found.is_array and found.type not in {"keyword", "constant_keyword"}
        return found

    def condition(node: AggregateCondition, depth: int, path: str) -> None:
        nonlocal weight
        if depth > limits.depth:
            raise RuleError("depth_budget_exceeded", path)
        operator(node.op, "condition", path + ".op")
        if node.op in {"all", "any", "not"}:
            _shape(node, {"op", "args"}, set(), path)
            if not node.args:
                raise RuleError("empty_conditions", path)
            if node.op == "not" and len(node.args) != 1:
                raise RuleError("invalid_arity", path + ".args")
            for i, arg in enumerate(node.args):
                condition(arg, depth + 1, f"{path}.args[{i}]")
            return
        has_value = node.op not in {"exists", "not_exists"}
        _shape(node, {"op", "field"} | ({"value"} if has_value else set()), set(), path)
        selected = field(node.field, node.op, path + ".field")
        if not has_value:
            return
        if node.op == "in_cidr":
            try:
                if not isinstance(node.value, str) or "/" not in node.value or "%" in node.value:
                    raise ValueError()
                ipaddress.ip_network(node.value, strict=False)
            except ValueError as error:
                raise RuleError("invalid_cidr", path + ".value") from error
            return
        values = node.value if node.op in {"in", "not_in", "has_any", "has_all", "has_none", "between"} else [node.value]
        if not isinstance(values, list) or not 1 <= len(values) <= limits.literals:
            raise RuleError("literal_budget_exceeded", path + ".value")
        if not all(literal_matches(v, selected) for v in values):
            raise RuleError("invalid_literal_type", path + ".value", selected.name)
        if node.op != "between" and len({type(v) for v in values}) > 1:
            raise RuleError("mixed_literal_types", path + ".value")
        # One EXISTS/array scan is compiled for each literal. Preserve the
        # accepted keyword has_any budget; account for every scan of new forms.
        if (node.op in {"has_all", "has_none"} or
                node.op == "has_any" and selected.type not in {"keyword", "constant_keyword"}):
            weight += OPERATORS[node.op].weight * (len(values) - 1)
        if node.op in {"contains", "starts_with"} and not node.value:
            raise RuleError("empty_string", path + ".value")
        if node.op == "between":
            ordered = [datetime.fromisoformat(v) for v in values] if selected.type == "date" else values
            if len(values) != 2 or ordered[0] > ordered[1]:
                raise RuleError("invalid_range", path + ".value")

    previous: set[str] = set()

    def expression(node: Expression, depth: int, path: str) -> None:
        nonlocal weight
        if depth > limits.depth:
            raise RuleError("depth_budget_exceeded", path)
        weight += 1 if node.op is None else 0
        if node.ref is not None:
            _shape(node, {"ref"}, set(), path)
            if node.ref not in previous:
                raise RuleError("unknown_or_forward_reference", path + ".ref")
        elif node.value is not None:
            _shape(node, {"value"}, set(), path)
            try:
                finite = math.isfinite(node.value)
            except OverflowError:
                finite = False
            if not finite:
                raise RuleError("non_finite_constant", path + ".value")
        else:
            operator(node.op, "expression", path + ".op")
            _shape(node, {"op", "args"}, set(), path)
            low, high = arity(node.op)
            if node.args is None or len(node.args) < low or high is not None and len(node.args) > high:
                raise RuleError("binary_expression_required" if (low, high) == (2, 2) else "invalid_arity", path)
            for i, arg in enumerate(node.args):
                expression(arg, depth + 1, f"{path}.args[{i}]")

    if rule.where is not None:
        condition(rule.where, 1, "where")
    for i, feature in enumerate(rule.features):
        path = f"features[{i}]"
        if feature.key in previous:
            raise RuleError("duplicate_feature_key", path + ".key")
        operator(feature.op, "feature", path + ".op")
        required = {"key", "name", "op"}
        if feature.op == "derive":
            _shape(feature, required | {"expr"}, set(), path)
            if feature.expr is None:
                raise RuleError("expression_required", path)
            expression(feature.expr, 1, path + ".expr")
        else:
            needs_field = feature.op != "count"
            _shape(feature, required | ({"field"} if needs_field else set()) |
                   ({"percentile"} if feature.op == "percentile" else set()), {"where"}, path)
            if needs_field:
                field(feature.field, feature.op, path + ".field")
            if feature.op == "percentile" and not 0 <= feature.percentile <= 1:
                raise RuleError("invalid_percentile", path + ".percentile")
            if feature.where is not None:
                condition(feature.where, 1, path + ".where")
        previous.add(feature.key)
    return CompiledRule(rule, fields, canonical, hashlib.sha256(payload).hexdigest(), weight,
                        limits.array_elements, extended_input)


def evaluate_expression(node: Expression, results: dict[str, FeatureResult], duration: int = 1) -> FeatureResult:
    def result(value=None, reason=None):
        return FeatureResult(value=value, reason=reason, valid_count=int(reason is None),
                             invalid_count=int(reason is not None))
    def operand(value):
        if value is None:
            return result(reason="missing_operand")
        if type(value) not in (int, float):
            return result(reason="non_numeric_operand")
        try:
            if not math.isfinite(value):
                return result(reason="non_finite_operand")
        except OverflowError:
            return result(reason="non_finite_operand")
        return result(value)
    if node.ref is not None:
        referenced = results[node.ref]
        return operand(referenced.value)
    if node.value is not None:
        return operand(node.value)
    operands = [evaluate_expression(arg, results, duration) for arg in node.args]
    if any(arg.value is None for arg in operands):
        return result(reason=next(arg.reason for arg in operands if arg.value is None))
    values = [arg.value for arg in operands]
    a, b = values[0], values[1] if len(values) > 1 else None
    if node.op in {"divide", "ratio"} and b == 0:
        return result(reason="division_by_zero")
    if node.op == "clamp" and b > values[2]:
        return result(reason="invalid_clamp_bounds")
    try:
        value = {"add": lambda: a + b, "subtract": lambda: a - b,
                 "multiply": lambda: a * b, "divide": lambda: a / b,
                 "ratio": lambda: a / b, "percent": lambda: a * 100,
                 "per_second": lambda: a / duration, "abs": lambda: abs(a),
                 "clamp": lambda: min(max(a, b), values[2]),
                 "min_of": lambda: min(values), "max_of": lambda: max(values)}[node.op]()
        return result(value) if math.isfinite(value) else result(reason="non_finite_result")
    except (OverflowError, ValueError):
        return result(reason="non_finite_result")
