from typing import Any
from urllib.parse import urlparse

import httpx
import httpx2
from authlib.integrations.starlette_client import OAuth

from .config import Settings


class OidcDevBackchannelTransport(httpx.AsyncBaseTransport, httpx2.AsyncBaseTransport):
    """Route selected public OIDC URLs to the same port on the development host."""

    def __init__(
        self, issuer: str, inner: httpx.AsyncBaseTransport | httpx2.AsyncBaseTransport,
        allowed_urls: set[str],
    ):
        self.issuer = urlparse(issuer)
        self.inner = inner
        self.allowed_urls = allowed_urls

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        source = request.url
        issuer_port = self.issuer.port or (443 if self.issuer.scheme == "https" else 80)
        if (source.scheme, source.host, source.port) != (
            self.issuer.scheme, self.issuer.hostname, issuer_port
        ):
            raise httpx.ConnectError("OIDC request is outside the configured issuer", request=request)
        public_url = str(source.copy_with(fragment=None))
        if public_url not in self.allowed_urls:
            raise httpx.ConnectError("OIDC request path is not allowed", request=request)
        host = self.issuer.hostname
        if self.issuer.port:
            host += f":{self.issuer.port}"
        request.headers["host"] = host
        request.url = source.copy_with(
            host="host.docker.internal",
        )
        return await self.inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self.inner.aclose()


class AuthlibOidcClient:
    def __init__(self, settings: Settings, transport=None, metadata=None, verify=None):
        self.redirect_uri = settings.oidc_redirect_uri
        self.oauth = OAuth()
        client_kwargs = {"scope": " ".join(settings.oidc_scopes)}
        if transport is not None:
            client_kwargs["transport"] = transport
        if verify is not None:
            client_kwargs["verify"] = verify
            client_kwargs["trust_env"] = False
        client_kwargs["follow_redirects"] = False
        self.oauth.register(
            name="oidc",
            server_metadata_url=(
                settings.oidc_issuer_url.rstrip("/") + "/.well-known/openid-configuration"
                if settings.oidc_issuer_url and metadata is None
                else None
            ),
            client_id=settings.oidc_client_id,
            client_secret=(
                settings.oidc_client_secret.get_secret_value()
                if settings.oidc_client_secret
                else None
            ),
            client_kwargs=client_kwargs,
        )
        if metadata is not None:
            self.oauth.oidc.server_metadata.update(metadata)

    async def authorization_redirect(self, request: Any, nonce: str):
        return await self.oauth.oidc.authorize_redirect(
            request,
            self.redirect_uri,
            nonce=nonce,
        )

    async def verified_claims(self, request: Any, nonce: str) -> dict[str, Any]:
        token = await self.oauth.oidc.authorize_access_token(request)
        if not token.get("id_token"):
            raise ValueError("OIDC response has no ID token")
        claims = await self.oauth.oidc.parse_id_token(token, nonce=nonce)
        result = dict(claims)
        if not result.get("iss") or not result.get("sub"):
            raise ValueError("OIDC ID token has no issuer or subject")
        return result

    def clear_handshake(self, request: Any) -> None:
        request.session.clear()
