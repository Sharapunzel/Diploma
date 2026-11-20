"""One short-lived, read-only canonical count for the isolated browser profile."""

import json
import math
import os
import signal
import sys
import time
from pathlib import Path
from uuid import UUID

COUNT_SQL = """
    SELECT count(*),
        count(*) FILTER (WHERE calculation_status = 'computed'),
        count(*) FILTER (WHERE calculation_status = 'uncomputed')
    FROM logs.aggregate_closures
    WHERE aggregate_id = %s AND window_index BETWEEN 0 AND 519
        AND origin = 'initial_history' AND time_basis = 'event_time'
"""


def database_url():
    path = os.environ.get("DATABASE_URL_FILE")
    return (Path(path).read_text(encoding="utf-8") if path else os.environ["DATABASE_URL"]).strip()


def count_closures(url, identity, deadline, *, database, host, port):
    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    conninfo = conninfo_to_dict(url.replace("postgresql+psycopg://", "postgresql://", 1))
    if (set(conninfo) - {"dbname", "host", "port", "user", "password"}
            or conninfo.get("dbname") != database or conninfo.get("host") != host
            or conninfo.get("port", "5432") != port or not conninfo.get("user")
            or os.environ.get("PGHOSTADDR") or os.environ.get("PGSERVICE")):
        raise ValueError("Canonical readiness requires its isolated profile database")
    started = time.monotonic()
    remaining = deadline - started
    if remaining < .001:
        raise TimeoutError("Canonical readiness budget exhausted")
    # libpq accepts whole seconds, with a minimum effective connection timeout
    # of two seconds. The process timer also covers connect and transaction exit.
    with psycopg.connect(**conninfo, connect_timeout=max(2, min(5, math.ceil(remaining))),
            options=f"-c statement_timeout={min(3000, int(remaining * 1000))}",
            autocommit=True) as connection:
        connected = time.monotonic()
        connection.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        connection.read_only = True
        with connection.transaction():
            sql_started = time.monotonic()
            count, computed, uncomputed = connection.execute(COUNT_SQL, (identity,)).fetchone()
            sql_seconds = time.monotonic() - sql_started
    if time.monotonic() >= deadline:
        raise TimeoutError("Canonical readiness budget exhausted")
    return {"canonical_history_count": count, "computed_count": computed,
        "uncomputed_count": uncomputed, "connect_seconds": connected - started,
        "sql_seconds": sql_seconds}


def main(*, database="diploma_test", host="postgres", port="5432"):
    try:
        identity = UUID(sys.argv[1])
        budget = float(sys.argv[2])
        if not 0 < budget <= 15 or not math.isfinite(budget):
            raise ValueError("Invalid canonical readiness budget")
        deadline = time.monotonic() + budget
        # Docker can outlive its local CLI waiter. The default Linux signal
        # action terminates even a blocked libpq call and closes its socket.
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
        signal.setitimer(signal.ITIMER_REAL, max(.000001, deadline - time.monotonic()))
        try:
            fields = count_closures(database_url(), identity, deadline, database=database,
                host=host, port=port)
            print(json.dumps(fields, allow_nan=False), flush=True)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
    except Exception as error:  # noqa: BLE001 - CLI must not echo DSNs or libpq error text.
        print(f"Canonical readiness failed: {type(error).__name__}", file=sys.stderr, flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
