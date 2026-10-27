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
REALM_PATH = os.environ.get("OIDC_STUB_REALM_PATH", "")
ISSUER = os.environ.get("OIDC_STUB_PUBLIC_ISSUER") or ISSUER + REALM_PATH
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

    def public_host_ok(self):
        return not os.environ.get("OIDC_STUB_PUBLIC_ISSUER") or self.headers.get("Host") == urlparse(ISSUER).netloc

    def do_GET(self):
        if not self.public_host_ok():
            self.send_error(400)
            return
        parsed = urlparse(self.path)
        if parsed.path == REALM_PATH + "/.well-known/openid-configuration":
            self.send_json({
                "issuer": ISSUER,
                "authorization_endpoint": f"{ISSUER}/authorize",
                "token_endpoint": f"{ISSUER}/token",
                "jwks_uri": f"{ISSUER}/jwks",
                "response_types_supported": ["code"],
                "id_token_signing_alg_values_supported": ["RS256"],
            })
        elif parsed.path == REALM_PATH + "/jwks":
            self.send_json({"keys": [KEY.as_dict(private=False, kid="smoke", use="sig", alg="RS256")]})
        elif parsed.path == REALM_PATH + "/authorize":
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
        if not self.public_host_ok():
            self.send_error(400)
            return
        if urlparse(self.path).path != REALM_PATH + "/token":
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
