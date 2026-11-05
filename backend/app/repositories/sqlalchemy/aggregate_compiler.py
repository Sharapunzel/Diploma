"""SQL compiler for the closed aggregate AST; casts never rely on AND order."""

import ipaddress
from datetime import UTC
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    DateTime,
    Numeric,
    Text,
    and_,
    case,
    column,
    distinct,
    false,
    func,
    literal,
    not_,
    or_,
    select,
    true,
)
from sqlalchemy.dialects.postgresql import INET, INTERVAL, JSONB

from ...aggregation.results import AggregateStatistics, FeatureStatistics
from ...aggregation.rules import (
    DATE_PATTERN,
    DATE_UTC_MAX,
    DATE_UTC_MIN,
    INTEGER_BITS,
    INTEGER_TYPES,
    AggregateCondition,
    CompiledRule,
    RuleError,
    date_utc,
)
from ...ecs import NUMBER_TYPES, EcsField


def _scalar(raw, definition):
    present = and_(raw.is_not(None), func.jsonb_typeof(raw) != "null")
    kind = ("number" if definition.type in NUMBER_TYPES else
            "boolean" if definition.type == "boolean" else "string")
    valid = func.coalesce(func.jsonb_typeof(raw) == kind, false())
    text_value = raw[()].astext
    if definition.type in NUMBER_TYPES:
        value = case((valid, text_value.cast(Numeric())), else_=None)
        if definition.type in INTEGER_TYPES:
            bound = 2 ** (INTEGER_BITS[definition.type] - 1)
            valid = and_(valid, text_value.op("~")(r"^-?(0|[1-9][0-9]*)$"),
                         value >= literal(Decimal(-bound), type_=Numeric()),
                         value < literal(Decimal(bound), type_=Numeric()))
        else:
            valid = and_(valid, value >= -1.7976931348623157e308,
                         value <= 1.7976931348623157e308)
    elif definition.type == "boolean":
        value = case((valid, text_value.cast(Boolean())), else_=None)
    elif definition.type == "date":
        # Parse the normalizer's explicit offset as an interval, including
        # seconds/microseconds and offsets beyond PostgreSQL's zone-name range.
        # Use timestamp WITHOUT time zone as a technical UTC carrier. A valid
        # UTC boundary must not become BC/year 10000 in the driver's session TZ.
        local_text = func.concat(func.split_part(text_value, "T", 1), "T", func.substring(
            func.split_part(text_value, "T", 2), r"^[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?"))
        offset_text = case((func.right(text_value, 1) == "Z", literal("00:00")),
            else_=func.substring(func.split_part(text_value, "T", 2),
                r"[+-][0-9]{2}:[0-9]{2}(?::[0-9]{2}(?:\.[0-9]{1,6})?)?$"))
        valid = and_(valid, text_value.op("~")(DATE_PATTERN),
                     func.pg_input_is_valid(local_text, "timestamp without time zone"),
                     func.pg_input_is_valid(offset_text, "interval"))
        utc_value = case((valid, local_text.cast(DateTime(timezone=False))
                          - offset_text.cast(INTERVAL())), else_=None)
        # PostgreSQL can represent the intermediate BC/year 10000 values;
        # reject them before they reach psycopg's Python datetime adapter.
        valid = and_(valid,
            utc_value >= literal(DATE_UTC_MIN.replace(tzinfo=None), type_=DateTime(timezone=False)),
            utc_value <= literal(DATE_UTC_MAX.replace(tzinfo=None), type_=DateTime(timezone=False)))
        value = case((valid, utc_value), else_=None)
    elif definition.type == "ip":
        valid = and_(valid, not_(text_value.op("~")(r"[/%]")),
                     not_(text_value.op("~")(r"(^|:|\.)0[0-9]+\.|\.0[0-9]+$")),
                     text_value.op("~")(r"^([0-9]{1,3}\.){3}[0-9]{1,3}$|^[0-9A-Fa-f:.]*:[0-9A-Fa-f:.]+$"),
                     func.pg_input_is_valid(text_value, "inet"))
        value = case((valid, text_value.cast(INET)), else_=None)
    else:
        value = case((valid, text_value), else_=None)
    return present, func.coalesce(valid, false()), value


def _field(data, name, definition):
    raw = data
    for part in name.split("."):
        raw = raw[part]
    if not definition.is_array:
        return raw, *_scalar(raw, definition)
    present = and_(raw.is_not(None), func.jsonb_typeof(raw) != "null")
    array = func.coalesce(func.jsonb_typeof(raw) == "array", false())
    safe = case((array, raw), else_=literal([], type_=JSONB))
    elements = func.jsonb_array_elements(safe, type_=JSONB).table_valued(
        # Typed columns retain JSONB operators in the nested validity check.
        column("value", JSONB)).alias()
    _, valid_element, _ = _scalar(elements.c.value, definition)
    invalid = select(literal(1)).select_from(elements).where(not_(valid_element)).exists()
    return raw, present, and_(array, not_(invalid)), raw


def _literal(value, definition):
    if definition.type in NUMBER_TYPES:
        return literal(Decimal(str(value)), type_=Numeric())
    if definition.type == "date":
        return literal(date_utc(value).replace(tzinfo=None), type_=DateTime(timezone=False))
    if definition.type == "ip":
        return literal(value).cast(INET)
    return literal(value)


def compile_condition(node: AggregateCondition | None, data, fields):
    """Return matches and independent invalidity, including every child."""
    if node is None:
        return true(), false()
    if node.op in {"all", "any", "not"}:
        pairs = [compile_condition(arg, data, fields) for arg in node.args]
        invalid = or_(*(p[1] for p in pairs))
        matches = (not_(pairs[0][0]) if node.op == "not" else
                   (and_ if node.op == "all" else or_)(*(p[0] for p in pairs)))
        return (and_(matches, not_(invalid)) if node.op == "not" else matches), invalid
    definition = fields[node.field]
    raw, present, valid, value = _field(data, node.field, definition)
    invalid = and_(present, not_(valid))
    if node.op == "exists":
        return present, invalid
    if node.op == "not_exists":
        return not_(present), invalid
    if node.op in {"has_any", "has_all", "has_none"}:
        # Compare typed values (date offsets and IPv6 spellings normalize).
        safe = case((func.jsonb_typeof(raw) == "array", raw), else_=literal([], type_=JSONB))
        elements = func.jsonb_array_elements(safe, type_=JSONB).table_valued(
            column("value", JSONB)).alias()
        _, _, element = _scalar(elements.c.value, definition)
        predicates = [select(literal(1)).select_from(elements).where(
            element == _literal(v, definition)).exists() for v in node.value]
        matches = (and_(*predicates) if node.op == "has_all" else
                   not_(or_(*predicates)) if node.op == "has_none" else or_(*predicates))
    elif node.op in {"in", "not_in"}:
        matches = value.in_([_literal(v, definition) for v in node.value])
        if node.op == "not_in":
            matches = not_(matches)
    elif node.op == "between":
        matches = value.between(*[_literal(v, definition) for v in node.value])
    elif node.op == "contains":
        matches = func.strpos(value, node.value) > 0
    elif node.op == "starts_with":
        matches = func.starts_with(value, node.value)
    elif node.op == "in_cidr":
        matches = value.op("<<=")(literal(str(ipaddress.ip_network(node.value, strict=False))).cast(INET))
    else:
        other = _literal(node.value, definition)
        matches = {"eq": lambda: value == other, "neq": lambda: value != other,
                   "gt": lambda: value > other, "gte": lambda: value >= other,
                   "lt": lambda: value < other, "lte": lambda: value <= other}[node.op]()
    return func.coalesce(and_(present, valid, matches), false()), invalid


def aggregate_statement(base, compiled: CompiledRule):
    data = base.c.ecs_data
    matches, invalid = compile_condition(compiled.rule.where, data, compiled.fields)
    if compiled.extended_input and compiled.rule.where is not None:
        # Evaluate the shared predicate once per event, not once per statistic
        # and diagnostic column. MATERIALIZED also prevents planner inlining
        # from recreating the enormous repeated guarded expression tree.
        base = select(data, matches.label("rule_matches"), invalid.label("rule_invalid")).select_from(base).cte().prefix_with("MATERIALIZED")
        data, matches, invalid = base.c.ecs_data, base.c.rule_matches, base.c.rule_invalid
    # The normalized timestamp is still guarded against malformed stored JSON.
    _, _, occurred = _scalar(data["@timestamp"], EcsField("@timestamp", "date", "", "", False, "", False, ()))
    columns = [func.count().label("observed_rows"),
               func.max(func.octet_length(data.cast(Text))).label("observed_width"),
               func.count().filter(matches).label("event_count"),
               func.min(occurred).filter(matches).label("event_time_min"),
               func.max(occurred).filter(matches).label("event_time_max")]
    array_sizes = []
    for name, definition in compiled.fields.items():
        if definition.is_array:
            raw = data
            for part in name.split("."):
                raw = raw[part]
            array_sizes.append(case((func.jsonb_typeof(raw) == "array", func.jsonb_array_length(raw)), else_=0))
    if array_sizes:
        # Pack both scan metrics into the existing width column's slot. An
        # additional output column would reject valid legacy 1664-column plans.
        columns[1] = func.jsonb_build_array(
            func.max(func.octet_length(data.cast(Text))),
            func.max(func.greatest(*array_sizes, literal(0)))).label("observed_metrics")
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
            if feature.op in {"present_count", "missing_count", "true_count", "false_count"}:
                participating = (and_(selected, not_(present)) if feature.op == "missing_count" else
                                 and_(eligible, scalar == (feature.op == "true_count"))
                                 if feature.op in {"true_count", "false_count"} else eligible)
                value = valid_count = func.count().filter(participating)
            else:
                valid_count = func.count().filter(eligible)
                if feature.op == "distinct_count":
                    value = func.count(distinct(scalar)).filter(eligible)
                elif feature.op in {"median", "percentile", "mode_count", "mode_share"}:
                    # Full bounded input, never a sample. Pure interpretation
                    # performs numeric Decimal interpolation and exact frequencies.
                    value = func.array_agg(scalar).filter(eligible)
                elif feature.op == "range":
                    value = func.max(scalar).filter(eligible) - func.min(scalar).filter(eligible)
                else:
                    value = {"sum": func.sum, "avg": func.avg, "min": func.min,
                             "max": func.max, "variance": func.var_pop,
                             "stddev": func.stddev_pop}[feature.op](scalar).filter(eligible)
        columns.extend([value.label(f"v{i}"), valid_count.label(f"n{i}"),
                        func.count().filter(bad).label(f"b{i}")])
    return select(*columns).select_from(base)


def read_statistics(row, compiled: CompiledRule) -> AggregateStatistics:
    width, array_size = row.get("observed_metrics") or (row.get("observed_width"), 0)
    if (array_size or 0) > compiled.array_elements:
        raise RuleError("resource_budget_exceeded", "array_elements",
                        f"Массив превышает {compiled.array_elements} элементов; разделите вход или увеличьте AGGREGATE_ARRAY_ELEMENTS")
    features = {}
    for i, feature in enumerate(compiled.rule.features):
        if feature.op != "derive":
            value = row[f"v{i}"]
            if feature.op in {"mode_count", "mode_share"} and compiled.fields[feature.field].type == "date" and value is not None:
                value = [item.replace(tzinfo=UTC) for item in value]
            features[feature.key] = FeatureStatistics(value, row[f"n{i}"], row[f"b{i}"])
    return AggregateStatistics(
        observed_rows=row["observed_rows"], event_count=row["event_count"],
        observed_width=(width or 0) + 64,
        event_time_min=(row["event_time_min"].replace(tzinfo=UTC) if row["event_time_min"] is not None else None),
        event_time_max=(row["event_time_max"].replace(tzinfo=UTC) if row["event_time_max"] is not None else None),
        features=features)
