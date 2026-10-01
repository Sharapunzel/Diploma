import base64
import getpass
import json
import secrets
import ssl
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent
CERTS = ROOT / "runtime" / "certs"
PASSWORD_FILE = ROOT / "runtime" / "reader-password.txt"
BASE_URL = "https://localhost:19200"
USERNAME = "diploma_task12_reader"
ROLE = "diploma_task12_readonly"


def call(method, path, username, password, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    request = Request(
        BASE_URL + path,
        data=body,
        method=method,
        headers={"Authorization": f"Basic {token}", "Content-Type": "application/json"},
    )
    context = ssl.create_default_context(cafile=str(CERTS / "root-ca.pem"))
    with urlopen(request, context=context, timeout=5) as response:
        content = response.read(1_048_577)
        if len(content) > 1_048_576:
            raise RuntimeError("Unexpectedly large response")
        return json.loads(content) if content else None


def main():
    admin_password = getpass.getpass("Wazuh Indexer admin password: ")
    deadline = time.monotonic() + 180
    while True:
        try:
            call("GET", "/", "admin", admin_password)
            break
        except (URLError, TimeoutError):
            if time.monotonic() >= deadline:
                raise SystemExit("Indexer did not become ready within 180 seconds")
            time.sleep(2)

    role = {
        "cluster_permissions": ["cluster_monitor"],
        "index_permissions": [
            {
                "index_patterns": ["task12-*", "wazuh-findings-v5-*"],
                "dls": "",
                "fls": [],
                "masked_fields": [],
                "allowed_actions": [
                    "read",
                    "indices:admin/mappings/get",
                    "indices:data/read/point_in_time/create",
                    "indices:data/read/point_in_time/delete",
                ],
            },
            {
                "index_patterns": ["*"],
                "dls": "",
                "fls": [],
                "masked_fields": [],
                "allowed_actions": ["indices_monitor", "indices:admin/resolve/index"],
            },
        ],
        "tenant_permissions": [],
    }
    call("PUT", f"/_plugins/_security/api/roles/{ROLE}", "admin", admin_password, role)
    reader_password = secrets.token_urlsafe(32)
    call(
        "PUT",
        f"/_plugins/_security/api/internalusers/{USERNAME}",
        "admin",
        admin_password,
        {"password": reader_password, "backend_roles": [], "attributes": {}},
    )
    call(
        "PUT",
        f"/_plugins/_security/api/rolesmapping/{ROLE}",
        "admin",
        admin_password,
        {"users": [USERNAME], "backend_roles": [], "hosts": []},
    )
    PASSWORD_FILE.parent.mkdir(parents=True, exist_ok=True)
    PASSWORD_FILE.write_text(reader_password, encoding="utf-8")
    PASSWORD_FILE.chmod(0o600)
    print(f"Created read-only account {USERNAME}; its password is saved in ignored {PASSWORD_FILE}")


if __name__ == "__main__":
    main()
