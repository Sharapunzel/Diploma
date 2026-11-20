"""Permanent real-SQL maintenance diagnostics and selective index assertions."""

import json
import time
from contextlib import contextmanager

from sqlalchemy import event


@contextmanager
def maintenance_trace(database):
    # Keep failure evidence useful when maintain intentionally sanitizes DB
    # exceptions. Failure reports omit query parameters and message payloads;
    # bounded SELECT parameters are kept only for the real EXPLAIN replay.
    trace = {"errors": [], "durations": [], "queries": {}}

    def before(_connection, _cursor, statement, parameters, context, _many):
        if "set_config" in statement and isinstance(parameters, dict):
            values = list(parameters.values())
            if "statement_timeout" in values:
                value = values[values.index("statement_timeout") + 1]
                if isinstance(value, str) and value.isdecimal():
                    trace["statement_timeout_ms"] = int(value)
        context._retention_started = time.monotonic()
        context._retention_timeout_ms = trace.get("statement_timeout_ms")
        for table, column in (("aggregate_memberships", "event_id"), ("detector_outcomes", "id")):
            if statement.startswith(f"SELECT logs.{table}.{column} "):
                trace["queries"][table] = statement, parameters.copy()

    def after(_connection, _cursor, statement, _parameters, context, _many):
        if any(table in statement for table in ("aggregate_memberships", "detector_outcomes")):
            trace["durations"].append({"kind": statement.split()[0],
                "elapsed": time.monotonic() - context._retention_started,
                "rows": _cursor.rowcount, "budget_ms": context._retention_timeout_ms})

    def error(context):
        started = getattr(context.execution_context, "_retention_started", time.monotonic())
        trace["errors"].append({"sqlstate": getattr(context.original_exception, "sqlstate", None),
            "exception": type(context.original_exception).__name__,
            "statement_kind": context.statement.split()[0] if context.statement else None,
            "outcomes": "detector_outcomes" in (context.statement or ""),
            "elapsed": time.monotonic() - started,
            "budget_ms": getattr(context.execution_context, "_retention_timeout_ms", None)})

    event.listen(database.engine, "before_cursor_execute", before)
    event.listen(database.engine, "after_cursor_execute", after)
    event.listen(database.engine, "handle_error", error)
    try:
        yield trace
    finally:
        event.remove(database.engine, "handle_error", error)
        event.remove(database.engine, "after_cursor_execute", after)
        event.remove(database.engine, "before_cursor_execute", before)


def assert_maintenance_result(result, trace):
    if result["status"] == "error" or trace["errors"]:
        print("Retention maintenance diagnostic: " + json.dumps({"result": result,
            "errors": trace["errors"], "durations": trace["durations"]}, default=str, sort_keys=True), flush=True)
    assert result["status"] != "error", (result, trace["errors"], trace["durations"])


def assert_remaining_index(database, trace, table, index):
    """EXPLAIN the repository's actual query; allow PostgreSQL's natural choice."""
    assert table in {"aggregate_memberships", "detector_outcomes"}
    statement, parameters = trace["queries"][table]
    with database.engine.begin() as connection:
        connection.exec_driver_sql(f"ANALYZE logs.{table}")
        plan = connection.exec_driver_sql("EXPLAIN (FORMAT JSON) " + statement, parameters).scalar_one()

    def names(node):
        return ([node["Index Name"]] if "Index Name" in node else []) + [
            name for child in node.get("Plans", []) for name in names(child)]

    assert index in names(plan[0]["Plan"]), plan
