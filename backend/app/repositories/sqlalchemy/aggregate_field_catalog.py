"""Bounded JSONB object-path discovery; never transports event values to Python."""

from sqlalchemy import Text, func, select, text

from ...models import Normalizer, ParsedLog


class SqlAlchemyFieldCatalogQueries:
    def __init__(self, session):
        self.session = session

    def catalog_limits(self, milliseconds, work_mem_mb):
        # Values are parameters, and limits apply to lock waits as well as work.
        self.session.execute(select(
            func.set_config("statement_timeout", str(milliseconds), True),
            func.set_config("lock_timeout", str(milliseconds), True),
            func.set_config("work_mem", f"{work_mem_mb}MB", True),
            func.set_config("jit", "off", True)))

    def catalog_preflight(self, source_id, max_events):
        bounded = select(ParsedLog.ecs_data).where(ParsedLog.source_id == source_id).limit(max_events + 1).subquery()
        return self.session.execute(select(func.count(),
            func.coalesce(func.sum(func.octet_length(bounded.c.ecs_data.cast(Text))), 0))
            .select_from(bounded)).one()

    def catalog_normalizer_size(self, normalizer_id):
        return self.session.execute(select(Normalizer.id, Normalizer.version,
            func.octet_length(Normalizer.rule.cast(Text))).where(Normalizer.id == normalizer_id)).one_or_none()

    def catalog_normalizer_rule(self, normalizer_id):
        return self.session.scalar(select(Normalizer.rule).where(Normalizer.id == normalizer_id))

    def catalog_paths(self, source_id, limits, before_read):
        # No DISTINCT, ORDER BY or aggregate above the recursive traversal:
        # LIMIT stops extraction at nodes+1, including repeated/unknown/root nodes.
        query = text("""
            WITH RECURSIVE walk(path, value) AS (
                SELECT ARRAY[]::text[], ecs_data FROM logs.parsed_logs WHERE source_id = :source_id
                UNION ALL
                SELECT walk.path || entry.key, entry.value
                FROM walk CROSS JOIN LATERAL jsonb_each(
                    CASE WHEN jsonb_typeof(walk.value) = 'object'
                              AND cardinality(walk.path) <= :depth
                         THEN walk.value ELSE '{}'::jsonb END
                ) AS entry
            )
            SELECT path FROM walk LIMIT :node_limit
        """)
        before_read()
        result = self.session.execute(query, {"source_id": source_id, "depth": limits.depth,
            "node_limit": limits.nodes + 1}, execution_options={"stream_results": True, "max_row_buffer": 256})
        try:
            while True:
                before_read()
                batch = result.fetchmany(256)
                if not batch:
                    break
                for row in batch:
                    yield tuple(row[0])
        finally:
            result.close()
