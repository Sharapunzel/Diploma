from __future__ import annotations

import ipaddress
import logging
import math
from datetime import datetime
from uuid import UUID

from ...core.errors import DomainError
from ...ecs import EcsCatalog, ValidatedEcsFilter
from ...repositories.protocols import UnitOfWork
from ...repositories.protocols.parsed_logs import ParsedLogRepository
from ...schemas.parsed_logs import (
    EcsFieldDTO,
    EcsFieldPage,
    EcsSchemaDTO,
    ParsedLogBulkDeleteRequest,
    ParsedLogBulkDeleteResponse,
    ParsedLogDetail,
    ParsedLogSearchRequest,
    ParsedLogSearchResponse,
    ParsedLogSummary,
)

LOGGER = logging.getLogger("app.parsed_logs")
from ...services.protocols.parsed_logs import CursorCodec


class EcsCatalogServiceImpl:
    def __init__(self, catalog: EcsCatalog):
        self.catalog = catalog

    def schema(self) -> EcsSchemaDTO:
        return EcsSchemaDTO(
            version=self.catalog.version,
            field_count=len(self.catalog.fields),
            upstream=self.catalog.provenance.upstream,
            tag=self.catalog.provenance.tag,
            source_file=self.catalog.provenance.source_file,
            retrieved_on=self.catalog.provenance.retrieved_on,
            sha256=self.catalog.provenance.sha256,
        )

    def fields(
        self, query: str | None, field_type: str | None, level: str | None,
        filterable: bool | None, limit: int, offset: int,
    ) -> EcsFieldPage:
        fields, total = self.catalog.list_fields(
            query, field_type, level, filterable, limit, offset
        )
        return EcsFieldPage(
            items=[EcsFieldDTO.from_field(field) for field in fields],
            total=total, limit=limit, offset=offset,
        )

    def field(self, field_name: str) -> EcsFieldDTO:
        field = self.catalog.get(field_name)
        if field is None:
            raise DomainError("ecs_field_not_found", "ECS field not found", 404)
        return EcsFieldDTO.from_field(field)


class ParsedLogServiceImpl:
    def __init__(
        self,
        repository: ParsedLogRepository,
        unit_of_work: UnitOfWork,
        catalog: EcsCatalog,
        cursor_codec: CursorCodec,
    ):
        self.repository = repository
        self.unit_of_work = unit_of_work
        self.catalog = catalog
        self.cursor_codec = cursor_codec

    def search(self, request: ParsedLogSearchRequest) -> ParsedLogSearchResponse:
        filters: list[ValidatedEcsFilter] = []
        for index, item in enumerate(request.ecs_filters):
            field = self.catalog.get(item.field)
            if field is None:
                raise DomainError(
                    "ecs_filter_invalid", "ECS filter field is unknown", 422,
                    {"index": index, "field": item.field, "reason": "unknown_field"},
                )
            if not field.filterable or item.operator not in field.operators:
                raise DomainError(
                    "ecs_filter_invalid", "ECS filter is not supported", 422,
                    {
                        "index": index, "field": item.field,
                        "reason": "non_filterable_field" if not field.filterable else "unsupported_operator",
                    },
                )
            try:
                value = self._validate_filter_value(field.type, item.operator, item.value)
            except DomainError as error:
                raise DomainError(
                    "ecs_filter_invalid", error.message, 422,
                    {"index": index, "field": item.field, "reason": "invalid_value_type"},
                ) from None
            filters.append(ValidatedEcsFilter(field, item.operator, value))

        try:
            cursor = self.cursor_codec.decode(request.cursor, request)
            snapshot_boundary = (
                cursor[2] if cursor is not None else self.repository.snapshot_boundary()
            )
            rows = self.repository.search(
                source_ids=request.source_ids,
                connection_ids=request.connection_ids,
                normalizer_ids=request.normalizer_ids,
                kafka_topics=request.kafka_topics,
                kafka_partitions=request.kafka_partitions,
                raw_query=request.raw_query,
                processed_from=request.processed_from,
                processed_to=request.processed_to,
                collected_from=request.collected_from,
                collected_to=request.collected_to,
                ecs_filters=filters,
                after=cursor[:2] if cursor is not None else None,
                snapshot_boundary=snapshot_boundary,
                limit=request.limit + 1,
            )
        except DomainError:
            self.unit_of_work.rollback()
            raise
        except Exception as error:  # noqa: BLE001 -- rollback and sanitize adapter failures.
            self.unit_of_work.rollback()
            LOGGER.error(
                "parsed_log_search_failed",
                extra={"error_type": type(error).__name__},
            )
            raise DomainError("internal_error", "Internal server error", 500) from None
        has_more = len(rows) > request.limit
        rows = rows[:request.limit]
        summaries = [
            ParsedLogSummary.from_search_row(log, preview, version)
            for log, preview, version in rows
        ]
        next_cursor = None
        if has_more and rows:
            last_log = rows[-1][0]
            next_cursor = self.cursor_codec.encode(
                last_log.backend_processed_at, last_log.id, snapshot_boundary, request
            )
        return ParsedLogSearchResponse(
            items=summaries, has_more=has_more, next_cursor=next_cursor
        )

    @staticmethod
    def _validate_filter_value(field_type: str, operator: str, value: object) -> object:
        if operator in {"exists", "not_exists"}:
            return None
        valid = True
        if field_type in {"integer", "long", "short", "byte"}:
            valid = isinstance(value, int) and not isinstance(value, bool)
        elif field_type in {"float", "double", "scaled_float", "half_float", "unsigned_long"}:
            valid = (
                isinstance(value, (int, float))
                and not isinstance(value, bool)
                and (not isinstance(value, float) or math.isfinite(value))
            )
        elif field_type == "boolean":
            valid = isinstance(value, bool)
        elif field_type == "date":
            valid = isinstance(value, str)
            if valid:
                try:
                    parsed = datetime.fromisoformat(value)
                    valid = parsed.tzinfo is not None and parsed.utcoffset() is not None
                    if valid:
                        value = parsed
                except ValueError:
                    valid = False
        elif field_type == "ip":
            if isinstance(value, str):
                try:
                    value = str(ipaddress.ip_address(value))
                except ValueError:
                    valid = False
            else:
                valid = False
        elif field_type in {"keyword", "constant_keyword", "wildcard", "text", "match_only_text"}:
            valid = isinstance(value, str)
        if not valid:
            raise DomainError("ecs_filter_value_invalid", "ECS filter value has the wrong type", 422)
        return value

    def get(self, log_id: UUID) -> ParsedLogDetail:
        log = self.repository.find_by_id(log_id)
        if log is None:
            raise DomainError("parsed_log_not_found", "Parsed log not found", 404)
        return ParsedLogDetail.from_model(log)

    def delete(self, log_id: UUID) -> None:
        try:
            log = self.repository.find_by_id(log_id)
            if log is None:
                raise DomainError("parsed_log_not_found", "Parsed log not found", 404)
            self.repository.delete(log)
            self.unit_of_work.commit()
        except DomainError:
            self.unit_of_work.rollback()
            raise
        except Exception as error:  # noqa: BLE001 -- rollback and sanitize all adapter failures.
            self.unit_of_work.rollback()
            LOGGER.error(
                "parsed_log_delete_failed",
                extra={"error_type": type(error).__name__},
            )
            raise DomainError("internal_error", "Internal server error", 500) from None

    def delete_many(
        self, request: ParsedLogBulkDeleteRequest
    ) -> ParsedLogBulkDeleteResponse:
        if request.source_id is None and request.processed_from is None:
            raise DomainError(
                "deletion_scope_required", "A source or time range is required", 422
            )
        try:
            deleted = self.repository.delete_many(
                request.source_id, request.processed_from, request.processed_to
            )
            self.unit_of_work.commit()
        except Exception as error:  # noqa: BLE001 -- rollback and sanitize all adapter failures.
            self.unit_of_work.rollback()
            LOGGER.error(
                "parsed_log_bulk_delete_failed",
                extra={"error_type": type(error).__name__},
            )
            raise DomainError("internal_error", "Internal server error", 500) from None
        return ParsedLogBulkDeleteResponse(deleted_count=deleted)
