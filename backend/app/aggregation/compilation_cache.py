"""Bounded pure rule-plan reuse; never retains events or snapshot results."""

import copy
import json
from collections import OrderedDict
from threading import RLock

MAX_ENTRIES = 16
MAX_BYTES = 4 * 1024 * 1024
_entries = OrderedDict()
_bytes = 0
_lock = RLock()


def compile_cached(rule, catalog, limits, compiler):
    global _bytes
    document = rule.model_dump(exclude_none=True)
    # Keep the compiler's validation/error ordering for invalid transport input.
    try:
        payload = json.dumps(document, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False).encode()
    except (ValueError, OverflowError):
        return compiler(rule, catalog, limits)
    if len(payload) > limits.json_bytes:
        return compiler(rule, catalog, limits)
    names = set()

    def visit(value):
        if isinstance(value, dict):
            name = value.get("field")
            if isinstance(name, str):
                parts = name.split(".")
                names.update(".".join(parts[:length]) for length in range(1, len(parts) + 1))
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(document)
    # Every used definition and ancestor is part of catalog identity. A custom
    # catalog changing a field under the same advertised version cannot hit.
    definitions = tuple((name, catalog.get(name)) for name in sorted(names))
    identity = (getattr(catalog, "version", None), definitions, limits, payload)
    with _lock:
        entry = _entries.get(identity)
        if entry is not None:
            _entries.move_to_end(identity)
            return copy.deepcopy(entry[0])
    compiled = compiler(rule, catalog, limits)
    size = 8 * len(payload) + len(compiled.calculation_features) * 1024
    size += sum(len(repr(definition).encode()) * 2 for _, definition in definitions)
    if size <= MAX_BYTES:
        cached = copy.deepcopy(compiled)
        with _lock:
            old = _entries.pop(identity, None)
            if old is not None:
                _bytes -= old[1]
            _entries[identity] = cached, size
            _bytes += size
            while len(_entries) > MAX_ENTRIES or _bytes > MAX_BYTES:
                _, (_, removed_size) = _entries.popitem(last=False)
                _bytes -= removed_size
    return compiled
