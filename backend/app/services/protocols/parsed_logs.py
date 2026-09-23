from datetime import datetime
from typing import Protocol
from uuid import UUID

from ...schemas.parsed_logs import (
    EcsFieldDTO,
    EcsFieldPage,
    EcsSchemaDTO,
    ParsedLogBulkDeleteRequest,
    ParsedLogBulkDeleteResponse,
    ParsedLogDetail,
    ParsedLogSearchRequest,
    ParsedLogSearchResponse,
)


class ParsedLogService(Protocol):
    def search(self, request: ParsedLogSearchRequest) -> ParsedLogSearchResponse: ...
    def get(self, log_id: UUID) -> ParsedLogDetail: ...
    def delete(self, log_id: UUID) -> None: ...
    def delete_many(
        self, request: ParsedLogBulkDeleteRequest
    ) -> ParsedLogBulkDeleteResponse: ...


class CursorCodec(Protocol):
    def encode(
        self, processed_at: datetime, log_id: UUID, snapshot_boundary: datetime,
        request: ParsedLogSearchRequest,
    ) -> str: ...
    def decode(
        self, token: str | None, request: ParsedLogSearchRequest
    ) -> tuple[datetime, UUID, datetime] | None: ...


class EcsCatalogService(Protocol):
    def schema(self) -> EcsSchemaDTO: ...
    def fields(
        self, query: str | None, field_type: str | None, level: str | None,
        filterable: bool | None, limit: int, offset: int,
    ) -> EcsFieldPage: ...
    def field(self, field_name: str) -> EcsFieldDTO: ...
