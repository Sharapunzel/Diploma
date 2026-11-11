"""Read-only discovery budgets and deterministic public field metadata."""

import hashlib
import json
from dataclasses import dataclass

from .rules import OPERATORS, compatible, field_support_reason


@dataclass(frozen=True)
class FieldCatalogLimits:
    deadline_seconds: float = 10
    events: int = 100000
    json_bytes: int = 64 * 1024**2
    nodes: int = 2000000
    depth: int = 16
    paths: int = 8192
    work_mem_mb: int = 16


class FieldCatalogBudgetError(ValueError):
    def __init__(self, factor):
        self.factor = factor
        super().__init__(factor)


def check_limit(value, maximum, factor):
    if value > maximum:
        raise FieldCatalogBudgetError(factor)


def field_metadata(field, catalog):
    reason = field_support_reason(field, catalog)
    return {"name": field.name, "type": field.type, "is_array": field.is_array,
        "description": field.description, "supported": reason is None, "reason": reason,
        "operators": [op.name for op in OPERATORS.values() if reason is None and compatible(op, field)]}


def catalog_digest(items, catalog, normalizer):
    document = {"ecs_version": catalog.version, "normalizer": normalizer, "items": items}
    return hashlib.sha256(json.dumps(document, sort_keys=True, ensure_ascii=False,
        separators=(",", ":")).encode()).hexdigest()
