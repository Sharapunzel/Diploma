"""Synthetic OIDC provider for the permanent isolated proxy smoke profiles."""
import json
import os
import ssl
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

from cryptography.hazmat.primitives.asymmetric import rsa
from joserfc import jwt
from joserfc.jwk import RSAKey

PORT = int(os.environ.get("OIDC_STUB_PORT", "19095"))
SCHEME = "https" if os.environ.get("OIDC_STUB_CERT_FILE") else "http"
ISSUER = f"{SCHEME}://{os.environ.get('OIDC_STUB_HOST', 'oidc-idp')}:{PORT}"
BROWSER_HOST = os.environ.get("OIDC_STUB_BROWSER_HOST", "oidc-idp")
KEY = RSAKey.import_key(rsa.generate_private_key(65537, 2048))
CODES = {}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def send_json(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/.well-known/openid-configuration":
            self.send_json({
                "issuer": ISSUER,
                "authorization_endpoint": f"{SCHEME}://{BROWSER_HOST}:{PORT}/authorize",
                "token_endpoint": f"{ISSUER}/token",
                "jwks_uri": f"{ISSUER}/jwks",
                "response_types_supported": ["code"],
                "id_token_signing_alg_values_supported": ["RS256"],
            })
        elif parsed.path == "/jwks":
            self.send_json({"keys": [KEY.as_dict(private=False, kid="smoke", use="sig", alg="RS256")]})
        elif parsed.path == "/authorize":
            params = parse_qs(parsed.query)
            if params.get("client_id") != ["smoke-client"] or not params.get("state") or not params.get("nonce"):
                self.send_error(400)
                return
            code = f"smoke-{len(CODES) + 1}"
            CODES[code] = params["nonce"][0]
            redirect = params["redirect_uri"][0]
            separator = "&" if "?" in redirect else "?"
            self.send_response(302)
            self.send_header("Location", redirect + separator + urlencode({"code": code, "state": params["state"][0]}))
            self.end_headers()
        else:
            self.send_error(404)

    def do_POST(self):
        if urlparse(self.path).path != "/token":
            self.send_error(404)
            return
        params = parse_qs(self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode())
        code = params.get("code", [None])[0]
        nonce = CODES.pop(code, None)
        if not nonce:
            self.send_error(400)
            return
        now = int(time.time())
        token = jwt.encode(
            {"alg": "RS256", "kid": "smoke"},
            {"iss": ISSUER, "sub": "synthetic-subject", "aud": "smoke-client", "iat": now, "exp": now + 300, "nonce": nonce, "name": "Тест OIDC"},
            KEY,
            algorithms=["RS256"],
        )
        self.send_json({"access_token": "synthetic-access", "token_type": "Bearer", "id_token": token})


if __name__ == "__main__":
    # This process runs only in the temporary smoke container on its internal network.
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    if SCHEME == "https":
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(os.environ["OIDC_STUB_CERT_FILE"], os.environ["OIDC_STUB_KEY_FILE"])
        server.socket = context.wrap_socket(server.socket, server_side=True)
    server.serve_forever()
