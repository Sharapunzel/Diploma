"""SQL dispatch for validated rules. All JSON paths and literals are bound values.

Every statistic, invalid count and timestamp bound is selected by one statement.
CASE guards casts; SQL boolean evaluation order is never used to protect a cast.
"""

from decimal import Decimal

from sqlalchemy import (
    Boolean,
    DateTime,
    Numeric,
    Text,
    and_,
    case,
    distinct,
    false,
    func,
    literal,
    not_,
    or_,
    select,
    true,
)
from sqlalchemy.dialects.postgresql import JSONB

from ...aggregation.results import AggregateStatistics, FeatureStatistics
from ...aggregation.rules import (
    INTEGER_BITS,
    INTEGER_TYPES,
    AggregateCondition,
    CompiledRule,
)
from ...ecs import NUMBER_TYPES


def _field(data, name, definition):
    raw = data
    for part in name.split("."):
        raw = raw[part]
    present = and_(raw.is_not(None), func.jsonb_typeof(raw) != "null")
    kind = ("number" if definition.type in NUMBER_TYPES else
            "boolean" if definition.type == "boolean" else "string")
    valid = func.coalesce(func.jsonb_typeof(raw) == kind, false())
    if definition.type in NUMBER_TYPES:
        value = case((valid, raw.astext.cast(Numeric())), else_=None)
        if definition.type in INTEGER_TYPES:
            bound = 2 ** (INTEGER_BITS[definition.type] - 1)
            valid = and_(valid, raw.astext.op("~")(r"^-?(0|[1-9][0-9]*)$"),
                         value >= literal(Decimal(-bound), type_=Numeric()),
                         value < literal(Decimal(bound), type_=Numeric()))
        else:
            valid = and_(valid, value >= -1.7976931348623157e308,
                         value <= 1.7976931348623157e308)
    elif definition.type == "boolean":
        value = case((valid, raw.astext.cast(Boolean())), else_=None)
    else:
        value = case((valid, raw.astext), else_=None)
    return raw, present, valid, value


def compile_condition(node: AggregateCondition | None, data, fields):
    """Return (matches, invalid) without hiding invalid children by short circuit."""
    if node is None:
        return true(), false()
    if node.op in {"all", "any"}:
        pairs = [compile_condition(arg, data, fields) for arg in node.args]
        join = and_ if node.op == "all" else or_
        return join(*(p[0] for p in pairs)), or_(*(p[1] for p in pairs))
    definition = fields[node.field]
    raw, present, valid, value = _field(data, node.field, definition)
    if node.op == "has_any":
        array = func.coalesce(func.jsonb_typeof(raw) == "array", false())
        safe_array = case((array, raw), else_=literal([], type_=JSONB))
        elements = func.jsonb_array_elements(safe_array).table_valued("value").alias()
        heterogeneous = select(literal(1)).select_from(elements).where(
            func.jsonb_typeof(elements.c.value) != "string").exists()
        valid = and_(array, not_(heterogeneous))
        matches = or_(*(raw.contains([v]) for v in node.value))
        return and_(present, valid, matches), and_(present, not_(valid))
    invalid = and_(present, not_(valid))
    if node.op == "exists":
        return present, invalid
    if node.op == "not_exists":
        return not_(present), invalid
    matches = value == node.value if node.op == "eq" else value.in_(node.value)
    return func.coalesce(and_(present, valid, matches), false()), invalid


def aggregate_statement(base, compiled: CompiledRule):
    """Base is the source/time-filtered ECS relation, before rule filtering."""
    data = base.c.ecs_data
    matches, invalid = compile_condition(compiled.rule.where, data, compiled.fields)
    occurred = data["@timestamp"].astext.cast(DateTime(timezone=True))
    columns = [func.count().label("observed_rows"),
               func.max(func.octet_length(data.cast(Text))).label("observed_width"),
               func.count().filter(matches).label("event_count"),
               func.min(occurred).filter(matches).label("event_time_min"),
               func.max(occurred).filter(matches).label("event_time_max")]
    for i, feature in enumerate(compiled.rule.features):
        if feature.op == "derive":
            continue
        local, local_invalid = compile_condition(feature.where, data, compiled.fields)
        selected = and_(matches, local)
        bad = or_(invalid, and_(matches, local_invalid))
        if feature.op == "count":
            value = func.count().filter(selected)
            valid_count = func.count().filter(and_(selected, not_(bad)))
        else:
            _, present, valid, scalar = _field(data, feature.field, compiled.fields[feature.field])
            bad = or_(bad, and_(selected, present, not_(valid)))
            eligible = and_(selected, present, valid, not_(bad))
            valid_count = func.count().filter(eligible)
            if feature.op == "distinct_count":
                value = func.count(distinct(scalar)).filter(eligible)
            else:
                value = {"sum": func.sum, "avg": func.avg, "min": func.min,
                         "max": func.max}[feature.op](scalar).filter(eligible)
        columns.extend([value.label(f"v{i}"), valid_count.label(f"n{i}"),
                        func.count().filter(bad).label(f"b{i}")])
    return select(*columns).select_from(base)


def read_statistics(row, compiled: CompiledRule) -> AggregateStatistics:
    """Adapt one SQL row to raw statistics without interpreting feature results."""
    features = {}
    for i, feature in enumerate(compiled.rule.features):
        if feature.op != "derive":
            features[feature.key] = FeatureStatistics(row[f"v{i}"], row[f"n{i}"], row[f"b{i}"])
    return AggregateStatistics(
        observed_rows=row["observed_rows"], event_count=row["event_count"],
        observed_width=(row.get("observed_width") or 0) + 64,
        event_time_min=row["event_time_min"], event_time_max=row["event_time_max"],
        features=features)
