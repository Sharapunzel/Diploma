"""Closed AST and extensible operator contracts for aggregate rules."""

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictFloat, StrictInt, StrictStr

from ..ecs import NUMBER_TYPES, EcsCatalog, EcsField

LiteralValue = StrictStr | StrictBool | StrictInt | StrictFloat
SCALAR_TYPES = frozenset({"keyword", "constant_keyword", "boolean"}) | NUMBER_TYPES
INTEGER_TYPES = frozenset({"long", "integer", "short", "byte"})
INTEGER_BITS = {"long": 64, "integer": 32, "short": 16, "byte": 8}


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


# Both validation and SQL dispatch consult this registry. TASK-32 extends the
# registry and compiler handlers together; there is no unknown-op fallback.
OPERATORS = {
    op.name: op for op in [
        *[Operator(n, "condition", ("condition",), ("args",), 1, "boolean")
          for n in ("all", "any")],
        *[Operator(n, "condition", ("scalar",), ("field", "value"), 1, "boolean")
          for n in ("eq", "in")],
        *[Operator(n, "condition", ("scalar",), ("field",), 1, "boolean")
          for n in ("exists", "not_exists")],
        Operator("has_any", "condition", ("keyword_array",), ("field", "value"), 1, "boolean"),
        Operator("count", "feature", ("records",), (), 1, "number"),
        Operator("distinct_count", "feature", ("scalar",), ("field",), 8, "number"),
        *[Operator(n, "feature", ("number",), ("field",), 2, "number")
          for n in ("sum", "avg", "min", "max")],
        Operator("derive", "feature", ("expression",), ("expr",), 0, "number"),
        *[Operator(n, "expression", ("number",), ("args",), 1, "number")
          for n in ("add", "subtract", "multiply", "divide")],
    ]
}


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


@dataclass(frozen=True)
class CompiledRule:
    rule: AggregateRule
    fields: dict[str, EcsField]
    canonical: dict
    sha256: str
    weight: int


def operator_catalog() -> list[dict]:
    result = []
    for op in OPERATORS.values():
        types = {"field": "catalogued_ecs_field", "value": "strict_field_typed_literal",
                 "args": "condition[]" if op.kind == "condition" else "expression[2]",
                 "expr": "expression"}
        if op.name in {"in", "has_any"}:
            types["value"] = "nonempty_homogeneous_field_typed_literal[]"
        result.append({**asdict(op), "parameter_types": {name: types[name] for name in op.parameters},
                       "result_nullable": op.kind in {"feature", "expression"}})
    return result


def _shape(node: Node, required: set[str], optional: set[str], path: str) -> None:
    keys = {key for key in node.model_fields_set if getattr(node, key) is not None}
    if not required <= keys or keys - required - optional:
        raise RuleError("invalid_arguments", path)


def literal_matches(value: object, field: EcsField) -> bool:
    if field.type in INTEGER_TYPES:
        bits = INTEGER_BITS[field.type]
        return type(value) is int and -(2**(bits - 1)) <= value < 2**(bits - 1)
    if field.type in NUMBER_TYPES:
        return type(value) in (int, float) and math.isfinite(value)
    if field.type == "boolean":
        return type(value) is bool
    return type(value) is str


def field_support_reason(field: EcsField, catalog: EcsCatalog) -> str | None:
    if (field.type not in SCALAR_TYPES or
            (field.is_array and field.type not in {"keyword", "constant_keyword"})):
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

    def operator(name: str, kind: str, path: str) -> None:
        nonlocal weight
        entry = OPERATORS.get(name)
        if entry is None or entry.kind != kind:
            raise RuleError("unknown_operator", path)
        weight += entry.weight

    def field(name: str | None, array: bool, numeric: bool, path: str) -> EcsField:
        found = catalog.get(name) if name else None
        if found is None:
            raise RuleError("unknown_field", path)
        reason = field_support_reason(found, catalog)
        if reason:
            raise RuleError(reason, path, found.name)
        if (found.is_array != array or found.type not in SCALAR_TYPES
                or (numeric and found.type not in NUMBER_TYPES)
                or (array and found.type not in {"keyword", "constant_keyword"})):
            raise RuleError("unsupported_field_type", path, found.name)
        fields[found.name] = found
        return found

    def condition(node: AggregateCondition, depth: int, path: str) -> None:
        if depth > limits.depth:
            raise RuleError("depth_budget_exceeded", path)
        operator(node.op, "condition", path + ".op")
        if node.op in {"all", "any"}:
            _shape(node, {"op", "args"}, set(), path)
            if not node.args:
                raise RuleError("empty_conditions", path)
            for i, arg in enumerate(node.args):
                condition(arg, depth + 1, f"{path}.args[{i}]")
            return
        has_value = node.op not in {"exists", "not_exists"}
        _shape(node, {"op", "field"} | ({"value"} if has_value else set()), set(), path)
        selected = field(node.field, node.op == "has_any", False, path + ".field")
        if not has_value:
            return
        values = node.value if node.op in {"in", "has_any"} else [node.value]
        if not isinstance(values, list) or not 1 <= len(values) <= limits.literals:
            raise RuleError("literal_budget_exceeded", path + ".value")
        if not all(literal_matches(v, selected) for v in values):
            raise RuleError("invalid_literal_type", path + ".value", selected.name)
        if len({type(v) for v in values}) > 1:
            raise RuleError("mixed_literal_types", path + ".value")

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
            if not math.isfinite(node.value):
                raise RuleError("non_finite_constant", path + ".value")
        else:
            operator(node.op, "expression", path + ".op")
            _shape(node, {"op", "args"}, set(), path)
            if node.args is None or len(node.args) != 2:
                raise RuleError("binary_expression_required", path)
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
            _shape(feature, required | ({"field"} if needs_field else set()), {"where"}, path)
            if needs_field:
                field(feature.field, False, feature.op != "distinct_count", path + ".field")
            if feature.where is not None:
                condition(feature.where, 1, path + ".where")
        previous.add(feature.key)
    return CompiledRule(rule, fields, canonical, hashlib.sha256(payload).hexdigest(), weight)


def evaluate_expression(node: Expression, results: dict[str, FeatureResult]) -> FeatureResult:
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
    operands = [evaluate_expression(arg, results) for arg in node.args]
    if any(arg.value is None for arg in operands):
        return result(reason=next(arg.reason for arg in operands if arg.value is None))
    a, b = (arg.value for arg in operands)
    if node.op == "divide" and b == 0:
        return result(reason="division_by_zero")
    try:
        value = {"add": lambda: a + b, "subtract": lambda: a - b,
                 "multiply": lambda: a * b, "divide": lambda: a / b}[node.op]()
        return result(value) if math.isfinite(value) else result(reason="non_finite_result")
    except (OverflowError, ValueError):
        return result(reason="non_finite_result")
