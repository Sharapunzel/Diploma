"""Deterministic dependency closure and window-statistic lowering; no storage IO."""

import heapq
import json
from dataclasses import dataclass

from .rules import OPERATORS, AggregateCondition, AggregateRule, Expression, Feature, RuleError


@dataclass(frozen=True)
class CalculationPlan:
    steps: tuple[Feature, ...]
    outputs: tuple[str, ...]


def plan_metrics(plan: CalculationPlan, where, fields):
    """Charge the lowered operations; input shape/depth was already validated.

    Generated names and effective AND nodes are not a second user JSON document.
    Only reachable field definitions may affect runtime array/type diagnostics.
    """
    needed_fields = set()

    def condition(node):
        if node is None:
            return 0
        weight = OPERATORS[node.op].weight
        if node.field is not None:
            needed_fields.add(node.field)
        if (node.op in {"has_all", "has_none"} or node.op == "has_any"
                and fields[node.field].type not in {"keyword", "constant_keyword"}):
            weight *= len(node.value)
        return weight + sum(condition(arg) for arg in node.args or [])

    def expression(node):
        return 1 + sum(expression(arg) for arg in node.args or [])

    weight = condition(where)
    for feature in plan.steps:
        weight += OPERATORS[feature.op].weight + condition(feature.where)
        if feature.field is not None:
            needed_fields.add(feature.field)
        if feature.expr is not None:
            weight += expression(feature.expr)
    return {name: fields[name] for name in sorted(needed_fields)}, weight


def references(node: Expression | None):
    if node is None:
        return
    if node.ref is not None:
        yield node.ref
    for arg in node.args or []:
        yield from references(arg)


def build_plan(rule: AggregateRule, definitions: list[tuple[str, Feature]]) -> CalculationPlan:
    indices = {feature.key: i for i, (_, feature) in enumerate(definitions)}
    dependencies = {feature.key: set(references(feature.expr)) for _, feature in definitions}
    children = {key: [] for key in indices}
    for key, refs in dependencies.items():
        for ref in refs:
            children[ref].append(key)
    indegree = {key: len(refs) for key, refs in dependencies.items()}
    ready = [indices[key] for key, degree in indegree.items() if degree == 0]
    heapq.heapify(ready)
    order = []
    while ready:
        index = heapq.heappop(ready)
        feature = definitions[index][1]
        order.append(feature)
        for child in children[feature.key]:
            indegree[child] -= 1
            if indegree[child] == 0:
                heapq.heappush(ready, indices[child])
    if len(order) != len(definitions):
        key = next(key for key in indices if indegree[key])
        path = definitions[indices[key]][0]
        raise RuleError("cyclic_reference", path + ".expr", key)

    needed = set()
    pending = [feature.key for feature in rule.features]
    while pending:
        key = pending.pop()
        if key not in needed:
            needed.add(key)
            pending.extend(dependencies[key])
    steps: list[Feature] = []
    leaves: dict[str, str] = {}

    def lower(node, where):
        if node.aggregate is not None:
            operand = node.aggregate
            conditions = [condition for condition in (where, operand.where) if condition is not None]
            effective = (conditions[0] if len(conditions) == 1 else
                AggregateCondition(op="all", args=conditions) if conditions else None)
            definition = operand.model_dump(exclude_none=True)
            definition.pop("where", None)
            if effective is not None:
                definition["where"] = effective.model_dump(exclude_none=True)
            signature = json.dumps(definition, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
            if signature not in leaves:
                key = f"__aggregate_{len(leaves)}"
                leaves[signature] = key
                steps.append(Feature.model_construct(key=key, name=key, op=operand.op,
                    field=operand.field, percentile=operand.percentile, where=effective))
            return Expression(ref=leaves[signature])
        if node.args is not None:
            return Expression(op=node.op, args=[lower(arg, where) for arg in node.args])
        return node

    for feature in order:
        if feature.key not in needed:
            continue
        if feature.op == "formula":
            expression = lower(feature.expr, feature.where)
            steps.append(Feature(key=feature.key, name=feature.name, op="derive", expr=expression))
        else:
            steps.append(feature)
    return CalculationPlan(tuple(steps), tuple(feature.key for feature in rule.features))
