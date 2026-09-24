from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from importlib.resources import files
from types import MappingProxyType
from typing import Protocol

import yaml

LOGGER = logging.getLogger("app.ecs")

STRING_TYPES = frozenset({"keyword", "constant_keyword", "wildcard", "text", "match_only_text"})
NUMBER_TYPES = frozenset({"integer", "long", "float", "double", "scaled_float", "short", "byte"})
SUPPORTED_TYPES = STRING_TYPES | NUMBER_TYPES | {"date", "boolean", "ip"}


@dataclass(frozen=True)
class EcsField:
    name: str
    type: str
    level: str
    description: str
    is_array: bool
    field_set: str
    filterable: bool
    operators: tuple[str, ...]

    @property
    def mappable(self) -> bool:
        return self.mappable_reason is None

    @property
    def mappable_reason(self) -> str | None:
        if self.name in {"event.original", "ecs.version"}:
            return "system_managed"
        if self.type not in SUPPORTED_TYPES:
            return "unsupported_ecs_type"
        return None


@dataclass(frozen=True)
class EcsProvenance:
    upstream: str
    tag: str
    source_file: str
    retrieved_on: str
    sha256: str


@dataclass(frozen=True)
class ValidatedEcsFilter:
    field: EcsField
    operator: str
    value: object | None


class EcsCatalog(Protocol):
    version: str
    provenance: EcsProvenance
    fields: Mapping[str, EcsField]

    def get(self, name: str) -> EcsField | None: ...
    def list_fields(
        self, query: str | None, field_type: str | None, level: str | None,
        filterable: bool | None, limit: int, offset: int,
        mappable: bool | None = None,
    ) -> tuple[list[EcsField], int]: ...


class PackagedEcsCatalog:
    version = "9.4.0"

    def __init__(self, fields: Mapping[str, EcsField], provenance: EcsProvenance):
        self._fields = MappingProxyType(dict(fields))
        self.fields = self._fields
        self.provenance = provenance

    @classmethod
    def load(cls) -> PackagedEcsCatalog:
        resource_root = files("app.resources.ecs")
        artifact = resource_root.joinpath("ecs_flat_v9.4.0.yml").read_bytes()
        provenance_data = json.loads(
            resource_root.joinpath("provenance_v9.4.0.json").read_text(encoding="utf-8")
        )
        digest = hashlib.sha256(artifact).hexdigest()
        if digest != provenance_data["sha256"]:
            raise RuntimeError("Packaged ECS catalog checksum does not match provenance")
        raw_fields = yaml.safe_load(artifact)
        fields: dict[str, EcsField] = {}
        unknown_types: set[str] = set()
        for artifact_key, definition in raw_fields.items():
            name = definition.get("flat_name") or artifact_key
            field_type = str(definition.get("type", "unknown"))
            operators = _operators(field_type, "array" in definition.get("normalize", []))
            if not operators:
                unknown_types.add(field_type)
            parts = name.split(".")
            field_set = definition.get("original_fieldset")
            if not field_set:
                field_set = "base" if len(parts) == 1 else parts[0]
            fields[name] = EcsField(
                name=name,
                type=field_type,
                level=str(definition.get("level", "unknown")),
                description=str(definition.get("description") or definition.get("short") or ""),
                is_array="array" in definition.get("normalize", []),
                field_set=str(field_set),
                filterable=bool(operators),
                operators=operators,
            )
        for field_type in sorted(unknown_types):
            LOGGER.warning("ecs_fields_non_filterable", extra={"field_type": field_type})
        provenance = EcsProvenance(
            upstream=provenance_data["upstream"],
            tag=provenance_data["tag"],
            source_file=provenance_data["source_file"],
            retrieved_on=provenance_data["retrieved_on"],
            sha256=digest,
        )
        return cls(fields, provenance)

    def get(self, name: str) -> EcsField | None:
        return self._fields.get(name)

    def list_fields(
        self, query: str | None, field_type: str | None, level: str | None,
        filterable: bool | None, limit: int, offset: int,
        mappable: bool | None = None,
    ) -> tuple[list[EcsField], int]:
        normalized_query = query.casefold() if query else None
        matches = [
            field for field in self._fields.values()
            if (normalized_query is None or normalized_query in field.name.casefold()
                or normalized_query in field.description.casefold())
            and (field_type is None or field.type == field_type)
            and (level is None or field.level == level)
            and (filterable is None or field.filterable is filterable)
            and (mappable is None or field.mappable is mappable)
        ]
        matches.sort(key=lambda field: field.name)
        return matches[offset:offset + limit], len(matches)


def _operators(field_type: str, is_array: bool) -> tuple[str, ...]:
    if field_type in STRING_TYPES:
        result = ["eq", "neq", "contains", "exists", "not_exists"]
    elif field_type in NUMBER_TYPES:
        result = ["eq", "neq", "gt", "gte", "lt", "lte", "exists", "not_exists"]
    elif field_type == "date":
        result = ["eq", "gt", "gte", "lt", "lte", "exists", "not_exists"]
    elif field_type == "boolean":
        result = ["eq", "exists", "not_exists"]
    elif field_type == "ip":
        result = ["eq", "neq", "exists", "not_exists"]
    else:
        return ()
    if is_array:
        return ("eq", "neq", "contains", "exists", "not_exists")
    return tuple(result)
