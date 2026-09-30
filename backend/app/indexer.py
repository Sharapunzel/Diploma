"""Bounded, read-only OpenSearch metadata access."""

import json
import re
import ssl
import time
from fnmatch import fnmatchcase
from typing import Protocol

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import serialization


class IndexerError(Exception):
    def __init__(self, kind: str):
        self.kind = kind


def validate_ca(content: bytes) -> str:
    if not content or len(content) > 65536:
        raise ValueError("CA must be 1 to 65536 bytes")
    try:
        pem = content.decode("ascii")
        blocks = re.findall(
            r"-----BEGIN CERTIFICATE-----\s+[A-Za-z0-9+/=\s]+?-----END CERTIFICATE-----",
            pem,
        )
        if not blocks or re.sub(r"-----BEGIN CERTIFICATE-----\s+[A-Za-z0-9+/=\s]+?-----END CERTIFICATE-----", "", pem).strip():
            raise ValueError("invalid CA PEM")
        for block in blocks:
            certificate = x509.load_pem_x509_certificate(block.encode("ascii"))
            certificate.public_bytes(serialization.Encoding.PEM)
            constraints = certificate.extensions.get_extension_for_class(x509.BasicConstraints).value
            if not constraints.ca:
                raise ValueError("certificate is not a CA")
        return "\n".join(blocks) + "\n"
    except (UnicodeError, ValueError, x509.ExtensionNotFound) as error:
        raise ValueError("invalid CA PEM") from error


class IndexerMetadataAdapter(Protocol):
    def indices(self, base_url: str, username: str, password: str,
                ca_pem: str | None) -> list[str]: ...
    def data_streams(self, base_url: str, username: str, password: str,
                     ca_pem: str | None) -> list[str]: ...


class HttpIndexerMetadataAdapter:
    MAX_RESPONSE = 1_048_576
    MAX_INDICES = 10_000

    def data_streams(self, base_url: str, username: str, password: str,
                     ca_pem: str | None) -> list[str]:
        data = self._request(base_url, username, password, ca_pem,
                             "/_resolve/index/*", {"expand_wildcards": "open"})
        try:
            streams = data["data_streams"]
            if not isinstance(streams, list) or len(streams) > self.MAX_INDICES:
                raise ValueError
            names = []
            for item in streams:
                if not isinstance(item, dict) or not isinstance(item.get("name"), str):
                    raise TypeError
                names.append(item["name"])
            return sorted(set(names))
        except (KeyError, TypeError, ValueError) as error:
            raise IndexerError("invalid_response") from error

    def indices(self, base_url: str, username: str, password: str,
                ca_pem: str | None) -> list[str]:
        data = self._request(base_url, username, password, ca_pem,
                             "/_cat/indices", {"format": "json", "h": "index",
                                               "expand_wildcards": "open"})
        try:
            if not isinstance(data, list) or len(data) > self.MAX_INDICES:
                raise ValueError
            names = []
            for item in data:
                if not isinstance(item, dict) or not isinstance(item.get("index"), str):
                    raise TypeError
                if not item["index"].startswith(".ds-"):
                    names.append(item["index"])
            return sorted(set(names))
        except (TypeError, ValueError, UnicodeError) as error:
            raise IndexerError("invalid_response") from error

    def _request(self, base_url, username, password, ca_pem, path, params):
        context = ssl.create_default_context()
        if ca_pem is not None:
            try:
                context.load_verify_locations(cadata=ca_pem)
            except ssl.SSLError as error:
                raise IndexerError("invalid_ca") from error
        try:
            deadline = time.monotonic() + 15
            with (
                httpx.Client(verify=context, auth=httpx.BasicAuth(username, password),
                             timeout=httpx.Timeout(5.0), follow_redirects=False,
                             trust_env=False) as client,
                client.stream("GET", base_url + path, params=params) as response,
            ):
                if response.status_code in (401, 403):
                    raise IndexerError("access_denied")
                if response.is_redirect or response.status_code >= 400:
                    raise IndexerError("invalid_response")
                content = bytearray()
                for chunk in response.iter_bytes():
                    if time.monotonic() > deadline:
                        raise IndexerError("timeout")
                    content.extend(chunk)
                    if len(content) > self.MAX_RESPONSE:
                        raise IndexerError("response_too_large")
        except httpx.TimeoutException as error:
            raise IndexerError("timeout") from error
        except (httpx.ConnectError, httpx.TransportError) as error:
            cause = error
            while cause.__cause__ is not None:
                cause = cause.__cause__
            message = str(cause).lower()
            if isinstance(cause, ssl.SSLCertVerificationError) and cause.verify_code == 62 or "hostname mismatch" in message or "not valid for" in message:
                kind = "hostname_mismatch"
            elif "certificate" in message or "ssl" in message:
                kind = "untrusted_certificate"
            else:
                kind = "unavailable"
            raise IndexerError(kind) from None
        try:
            return json.loads(content)
        except (TypeError, ValueError, UnicodeError) as error:
            raise IndexerError("invalid_response") from error


def matching_indices(names: list[str], name: str | None, pattern: str | None) -> list[str]:
    return [item for item in names if item == name or (pattern is not None and fnmatchcase(item, pattern))]
