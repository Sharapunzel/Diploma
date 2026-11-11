"""One bounded read-only RR scenario for source-specific ECS field metadata."""

import time

from sqlalchemy.exc import DBAPIError

from ...aggregation.field_catalog import (
    FieldCatalogBudgetError,
    FieldCatalogLimits,
    catalog_digest,
    check_limit,
    field_metadata,
)
from ...core.errors import DomainError
from ...normalization.compiler import RuleValidationError, compile_rule
from ...normalization.schema import MapBlock


class AggregateFieldCatalogService:
    def __init__(self, repository, uow, catalog, limits: FieldCatalogLimits):
        self.repository, self.uow, self.catalog, self.limits = repository, uow, catalog, limits

    def read(self, source_id, q, limit, offset):
        deadline = time.monotonic() + self.limits.deadline_seconds

        def remaining_budget():
            remaining = int((deadline - time.monotonic()) * 1000)
            if remaining < 1:
                raise DomainError("field_catalog_timeout", "Каталог полей не рассчитан за отведённое время", 503)
            return remaining

        def before_read():
            self.repository.catalog_limits(remaining_budget(), self.limits.work_mem_mb)

        try:
            with self.uow.read_snapshot():
                before_read()
                snapshot_at = self.repository.now()
                before_read()
                source = self.repository.source(source_id)
                if source is None:
                    raise DomainError("source_not_found", "Источник не найден", 404)
                if source.source_type != "kafka":
                    raise DomainError("own_source_required", "Каталог доступен только для собственного ECS-источника", 422)
                normalizer_id = source.normalizer_id
                before_read()
                rows, size = self.repository.catalog_preflight(source_id, self.limits.events)
                check_limit(rows, self.limits.events, "events")
                check_limit(size, self.limits.json_bytes, "json_bytes")
                fields = {tuple(name.split(".")): field for name, field in self.catalog.fields.items()}
                observed = set()
                visited = 0
                for path in self.repository.catalog_paths(source_id, self.limits, before_read):
                    visited += 1
                    check_limit(visited, self.limits.nodes, "nodes")
                    check_limit(len(path), self.limits.depth, "depth")
                    if path in fields:
                        observed.add(fields[path].name)
                        check_limit(len(observed), self.limits.paths, "paths")
                predicted = {}
                identity = None
                if normalizer_id is not None:
                    before_read()
                    metadata = self.repository.catalog_normalizer_size(normalizer_id)
                    if metadata is None:
                        raise DomainError("source_normalizer_invalid", "Назначенный нормализатор не прошёл проверку", 422)
                    identity = {"id": str(metadata[0]), "version": metadata[1]}
                    # SQL JSONB text bounds transport, not the compiler's Python
                    # JSON size: PostgreSQL expands small floats to decimals.
                    check_limit(size + metadata[2], self.limits.json_bytes, "json_bytes")
                    before_read()
                    raw = self.repository.catalog_normalizer_rule(normalizer_id)
                    remaining_budget()
                    # The normalizer part is bounded too, including repeated
                    # declarations and config literals before schema compilation.
                    # Keep iterators, not all siblings: stack memory is bounded
                    # by depth, and only the next budget-detection node is read.
                    pending = [(iter((raw,)), 0)]
                    while pending:
                        if visited % 256 == 0:
                            remaining_budget()
                        children, depth = pending[-1]
                        try:
                            item = next(children)
                        except StopIteration:
                            pending.pop()
                            continue
                        visited += 1
                        check_limit(visited, self.limits.nodes, "nodes")
                        check_limit(depth, self.limits.depth, "depth")
                        if isinstance(item, dict):
                            pending.append((iter(item.values()), depth + 1))
                        elif isinstance(item, list):
                            pending.append((iter(item), depth + 1))
                    before_read()
                    try:
                        compiled = compile_rule(raw, self.catalog)
                    except (RuleValidationError, TypeError, ValueError):
                        # Never use the exception's text or user-defined paths.
                        raise DomainError("source_normalizer_invalid", "Назначенный нормализатор не прошёл проверку", 422) from None
                    for variant in compiled.rule.variants:
                        for block in variant.blocks:
                            if isinstance(block, MapBlock):
                                predicted.setdefault(block.target, set()).add("mapping")
                    for name in ("@timestamp", "ecs.version", "event.original"):
                        predicted.setdefault(name, set()).add("system")
                    for name in predicted:
                        check_limit(len(name.split(".")), self.limits.depth, "depth")
                names = observed | predicted.keys()
                check_limit(len(names), self.limits.paths, "paths")
                items = []
                for name in sorted(names):
                    item = field_metadata(self.catalog.get(name), self.catalog)
                    item["provenance"] = (["observed"] if name in observed else []) + (["normalizer"] if name in predicted else [])
                    item["normalizer_origin"] = sorted(predicted.get(name, ()))
                    items.append(item)
                digest = catalog_digest(items, self.catalog, identity)
                selected = [item for item in items if not q or q.casefold() in item["name"].casefold()]
                before_read()
                return {"items": selected[offset:offset + limit], "total": len(selected),
                    "limit": limit, "offset": offset, "snapshot_at": snapshot_at, "catalog_hash": digest,
                    "observation": {"scope": "all_retained_events", "complete": True, "rows": rows},
                    "normalizer": identity}
        except FieldCatalogBudgetError as error:
            raise DomainError("field_catalog_budget_exceeded", "Полный каталог превышает бюджет; уменьшите объём сохранённых данных или сложность нормализатора", 422,
                {"factor": error.factor}) from None
        except DBAPIError as error:
            code = getattr(error.orig, "sqlstate", None)
            if code in {"57014", "55P03"}:
                raise DomainError("field_catalog_timeout", "Каталог полей не рассчитан за отведённое время", 503) from None
            raise
