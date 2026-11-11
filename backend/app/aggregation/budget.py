"""Explicit, configurable startup budgets, not a throughput guarantee."""

from .rules import CompiledRule, Limits, RuleError


def estimate(compiled: CompiledRule, rows: int, source_rows: int, duration: int,
             limits: Limits, preview: bool, row_width: int = 256) -> dict:
    width = len(compiled.rule.features)
    internal = compiled.calculation_features
    distinct = sum(f.op == "distinct_count" for f in internal)
    materialized = sum(f.op in {"median", "percentile", "mode_count", "mode_share"}
                       for f in internal)
    statement_columns = 5 + 3 * sum(f.op != "derive" for f in internal)
    # Conservative uncompressed exact-set allowance. PostgreSQL's work_mem is
    # per operation; account separately for concurrent aggregate transition state.
    row_width = max(256, row_width)
    # Include SQL array state, driver values, sort/Counter and serialization;
    # this is deliberately more conservative than the existing distinct weight.
    memory = (distinct + 4 * materialized) * rows * row_width + width * 1024
    if compiled.plan is not None:
        memory += len(internal) * 1024
    # New input operations account for full strings/arrays, rather than
    # treating a megabyte record as one scalar. Legacy estimates stay stable.
    input_units = (row_width + 255) // 256 if compiled.extended_input else 1
    memory += 4 * row_width if compiled.extended_input and rows else 0
    if compiled.extended_input and compiled.rule.where is not None:
        memory += min(rows * (row_width + 16), limits.work_mem_mb * 1024**2)
    cost = rows * compiled.weight * input_units
    estimate = {"estimated_rows": rows, "source_rows": source_rows,
                "operator_weight": compiled.weight, "cost": cost,
                "vector_width": width, "estimated_memory_bytes": memory,
                "exact_materialized_features": materialized,
                "array_elements_limit": limits.array_elements,
                "statement_columns": statement_columns,
                "estimated_row_width_bytes": row_width,
                "work_mem_mb_per_operation": limits.work_mem_mb,
                "rcf_preliminary_bytes": width * 100 * 256 * 8,
                "window_seconds": duration,
                "time_guarantee": False}
    checks = [(preview and rows > limits.preview_rows, "preview_rows", "Сузьте окно"),
              (statement_columns > 1664, "statement_width",
               f"План требует {statement_columns} колонок при пределе PostgreSQL 1664; разделите базовые признаки между агрегатами"),
              (cost > limits.cost, "operator_cost", "Сузьте окно, сократите списки массивных условий или упростите правило"),
              (memory > limits.total_memory_mb * 1024**2, "total_memory",
               "Сузьте окно, сократите строки или число точных distinct_count, квантилей и частот")]
    for exceeded, factor, hint in checks:
        if exceeded:
            raise RuleError("resource_budget_exceeded", factor, hint)
    return estimate
