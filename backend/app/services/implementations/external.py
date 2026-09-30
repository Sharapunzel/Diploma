from datetime import UTC, datetime
from uuid import UUID

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy.exc import IntegrityError

from ...core.errors import DomainError
from ...indexer import IndexerError, IndexerMetadataAdapter, matching_indices, validate_ca
from ...models import ExternalConnection, Source
from ...repositories.protocols import UnitOfWork
from ...repositories.protocols.external import ExternalRepository
from ...schemas.external import (
    TARGET_FIELDS,
    ExternalConnectionCreate,
    ExternalConnectionPatch,
    ExternalSourceCreate,
    ExternalSourcePatch,
)


def failure(code: str, message: str, status: int = 400):
    raise DomainError(code, message, status)


class ExternalServiceImpl:
    def __init__(self, repository: ExternalRepository, uow: UnitOfWork,
                 adapter: IndexerMetadataAdapter, secret_key: str | None):
        self.repository, self.uow, self.adapter = repository, uow, adapter
        self.secret_key = secret_key

    def _cipher(self):
        try:
            if self.secret_key is None:
                raise ValueError
            return Fernet(self.secret_key.encode("ascii"))
        except (ValueError, UnicodeError):
            failure("external_key_unavailable", "External encryption key is missing or invalid", 503)

    def _encrypt(self, password: str):
        return self._cipher().encrypt(password.encode("utf-8")).decode("ascii")

    def _decrypt(self, encrypted: str):
        try:
            return self._cipher().decrypt(encrypted.encode("ascii")).decode("utf-8")
        except (InvalidToken, UnicodeError, ValueError):
            failure("external_key_mismatch", "External credential cannot be decrypted", 503)

    def _commit(self):
        try:
            self.uow.commit()
        except IntegrityError:
            self.uow.rollback()
            failure("external_conflict", "External configuration conflicts with existing data", 409)

    def connections(self, limit, offset):
        return self.repository.connections(limit, offset)

    def connection(self, identity):
        item = self.repository.connection(identity)
        if item is None:
            failure("external_connection_not_found", "External connection not found", 404)
        return item

    def create_connection(self, data: ExternalConnectionCreate):
        item = ExternalConnection(name=data.name, base_url=data.base_url,
                                  username=data.username,
                                  encrypted_password=self._encrypt(data.password.get_secret_value()))
        try:
            self.repository.add(item)
            self._commit()
            return item
        except IntegrityError:
            self.uow.rollback()
            failure("external_conflict", "External connection already exists", 409)

    def update_connection(self, identity: UUID, data: ExternalConnectionPatch):
        item = self.repository.connection(identity, True)
        if item is None:
            failure("external_connection_not_found", "External connection not found", 404)
        changed = False
        for key, value in data.model_dump(exclude_unset=True).items():
            if key == "password":
                value = self._encrypt(data.password.get_secret_value())
            key = "encrypted_password" if key == "password" else key
            if getattr(item, key) != value:
                setattr(item, key, value)
                changed = True
        if changed:
            item.updated_at = datetime.now(UTC)
            self.repository.disable_sources(identity)
        self._commit()
        return item

    def delete_connection(self, identity: UUID):
        item = self.repository.connection(identity, True)
        if item is None:
            failure("external_connection_not_found", "External connection not found", 404)
        self.repository.delete(item)
        self._commit()

    def set_ca(self, identity: UUID, content: bytes):
        try:
            pem = validate_ca(content)
        except ValueError:
            failure("invalid_ca", "Valid PEM CA certificate or chain required", 422)
        item = self.repository.connection(identity, True)
        if item is None:
            failure("external_connection_not_found", "External connection not found", 404)
        item.ca_pem = pem
        item.updated_at = datetime.now(UTC)
        self.repository.disable_sources(identity)
        self._commit()

    def delete_ca(self, identity: UUID):
        item = self.repository.connection(identity, True)
        if item is None:
            failure("external_connection_not_found", "External connection not found", 404)
        item.ca_pem = None
        item.updated_at = datetime.now(UTC)
        self.repository.disable_sources(identity)
        self._commit()

    def _names(self, connection: ExternalConnection, data_streams: bool = False):
        password = self._decrypt(connection.encrypted_password)
        try:
            method = self.adapter.data_streams if data_streams else self.adapter.indices
            return method(connection.base_url, connection.username, password, connection.ca_pem)
        except IndexerError as error:
            kinds = {
                "access_denied": ("indexer_access_denied", "Indexer authentication or permissions failed", 403),
                "untrusted_certificate": ("indexer_untrusted_certificate", "Indexer certificate is not trusted", 502),
                "hostname_mismatch": ("indexer_hostname_mismatch", "Indexer certificate hostname does not match", 502),
                "timeout": ("indexer_timeout", "Indexer request timed out", 504),
                "unavailable": ("indexer_unavailable", "Indexer is unavailable", 503),
                "response_too_large": ("indexer_response_too_large", "Indexer metadata exceeds limit", 502),
            }
            code, message, status = kinds.get(error.kind, ("indexer_invalid_response", "Invalid indexer response", 502))
            failure(code, message, status)

    def test(self, identity: UUID):
        self._names(self.connection(identity))
        return {"status": "ok"}

    def indices(self, identity: UUID, limit: int, offset: int):
        names = self._names(self.connection(identity))
        return names[offset:offset + limit], len(names)

    def data_streams(self, identity: UUID, limit: int, offset: int):
        names = self._names(self.connection(identity), True)
        return names[offset:offset + limit], len(names)

    def sources(self, limit, offset):
        return self.repository.sources(limit, offset)

    def source(self, identity):
        item = self.repository.source(identity)
        if item is None:
            failure("external_source_not_found", "External source not found", 404)
        return item

    def create_source(self, data: ExternalSourceCreate):
        if self.repository.connection(data.external_connection_id) is None:
            failure("external_connection_not_found", "External connection not found", 404)
        item = Source(name=data.name, source_type="external", connection_id=None,
                      external_connection_id=data.external_connection_id,
                      topic_name=None, normalizer_id=None, kafka_topic_identity=None,
                      target_type=data.target_type, index_name=data.index_name,
                      index_pattern=data.index_pattern, data_stream_name=data.data_stream_name,
                      data_stream_pattern=data.data_stream_pattern,
                      is_archived=False, is_enabled=False)
        try:
            self.repository.add(item)
            self._commit()
            return item
        except IntegrityError:
            self.uow.rollback()
            failure("external_source_conflict", "External source target already registered", 409)

    def update_source(self, identity: UUID, data: ExternalSourcePatch):
        item = self.repository.source(identity, True)
        if item is None:
            failure("external_source_not_found", "External source not found", 404)
        values = data.model_dump(exclude_unset=True)
        if "external_connection_id" in values and self.repository.connection(values["external_connection_id"]) is None:
            failure("external_connection_not_found", "External connection not found", 404)
        if "target_type" in values:
            for key in TARGET_FIELDS:
                values.setdefault(key, None)
        changed_target = any(key in values and getattr(item, key) != value for key, value in values.items() if key != "name")
        for key, value in values.items():
            setattr(item, key, value)
        if changed_target:
            item.is_enabled = False
        item.updated_at = datetime.now(UTC)
        self._commit()
        return item

    def delete_source(self, identity: UUID):
        item = self.repository.source(identity, True)
        if item is None:
            failure("external_source_not_found", "External source not found", 404)
        self.repository.delete(item)
        self._commit()

    def enable(self, identity: UUID):
        item = self.source(identity)
        connection = self.connection(item.external_connection_id)
        snapshot = (item.external_connection_id, item.target_type, item.index_name,
                    item.index_pattern, item.data_stream_name, item.data_stream_pattern,
                    connection.base_url, connection.username, connection.encrypted_password,
                    connection.ca_pem)
        stream = item.target_type in ("data_stream", "data_stream_pattern")
        names = self._names(connection, stream)
        name = item.data_stream_name if stream else item.index_name
        pattern = item.data_stream_pattern if stream else item.index_pattern
        if not matching_indices(names, name, pattern):
            failure("index_not_found", "No accessible target matches source", 409)
        connection = self.repository.connection(item.external_connection_id, True)
        item = self.repository.source(identity, True)
        if connection is None or item is None or snapshot != (
                item.external_connection_id, item.target_type, item.index_name,
                item.index_pattern, item.data_stream_name, item.data_stream_pattern,
                connection.base_url, connection.username, connection.encrypted_password,
                connection.ca_pem):
            failure("external_configuration_changed", "External configuration changed during check", 409)
        item.is_enabled = True
        item.updated_at = datetime.now(UTC)
        self._commit()
        return item

    def disable(self, identity: UUID):
        item = self.repository.source(identity, True)
        if item is None:
            failure("external_source_not_found", "External source not found", 404)
        item.is_enabled = False
        item.updated_at = datetime.now(UTC)
        self._commit()
        return item
