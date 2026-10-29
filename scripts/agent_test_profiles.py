"""Isolated TASK-21 verification using the project's Compose services.

Run from the repository root: python scripts/agent_test_profiles.py PROFILE.
Profiles: frontend, backend-db, kafka-integration, end-to-end, smoke-development,
smoke-production. Docker profiles use a unique Compose project and remove it in finally.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import http.client
import http.cookiejar
import json
import os
import secrets
import shlex
import socket
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from ipaddress import ip_address
from pathlib import Path
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from cryptography import x509
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-f", str(ROOT / "docker-compose.yml")]
NPM = "npm.cmd" if os.name == "nt" else "npm"


def pytest_addopts(run: Path) -> str:
    """Keep pytest's temporary files inside the run directory on every host OS."""
    base_temp = (run / "pytest").as_posix()
    options = f'-p no:cacheprovider --basetemp="{base_temp}"'
    if f"--basetemp={base_temp}" not in shlex.split(options):
        raise RuntimeError("Pytest temporary path was not preserved")
    return options


def command(args: list[str], *, env: dict[str, str], cwd: Path = ROOT, capture=False,
            timeout: int | None = None):
    try:
        result = subprocess.run(args, cwd=cwd, env=env, check=False,
                                text=True, capture_output=capture, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"Command timed out after {timeout}s: {' '.join(args[:5])}") from exc
    if result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(args[:5])}")
    return result.stdout if capture else ""


def free_port() -> int:
    with socket.socket() as address:
        address.bind(("127.0.0.1", 0))
        return address.getsockname()[1]


def secret_file(run: Path, name: str, value: str) -> Path:
    path = run / name
    path.write_text(value, encoding="utf-8")
    return path


def certificates(run: Path) -> dict[str, Path]:
    """Create disposable CA and localhost certificates for the production profile."""
    now = datetime.now(UTC)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "TASK-21 test CA")])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - timedelta(minutes=5)).not_valid_after(now + timedelta(days=2))
          .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
          .sign(ca_key, hashes.SHA256()))
    ca_path = run / "ca.pem"
    ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    result = {"KAFKA_CA_FILE": ca_path}
    for service, names in (("kafka", ["localhost", "kafka"]), ("web", ["localhost"])):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, names[0])])
        san = [x509.DNSName(item) for item in names]
        san.append(x509.IPAddress(ip_address("127.0.0.1")))
        certificate = (x509.CertificateBuilder().subject_name(name).issuer_name(ca_name)
                       .public_key(key.public_key()).serial_number(x509.random_serial_number())
                       .not_valid_before(now - timedelta(minutes=5))
                       .not_valid_after(now + timedelta(days=2))
                       .add_extension(x509.SubjectAlternativeName(san), critical=False)
                       .sign(ca_key, hashes.SHA256()))
        pem = certificate.public_bytes(serialization.Encoding.PEM)
        key_pem = key.private_bytes(serialization.Encoding.PEM,
                                    serialization.PrivateFormat.PKCS8,
                                    serialization.NoEncryption())
        if service == "kafka":
            result["KAFKA_BROKER_PEM_FILE"] = run / "kafka_broker.pem"
            result["KAFKA_BROKER_PEM_FILE"].write_bytes(pem + key_pem)
        else:
            result["WEB_CERTIFICATE_FILE"] = run / "web_certificate.pem"
            result["WEB_PRIVATE_KEY_FILE"] = run / "web_private_key.pem"
            result["WEB_CERTIFICATE_FILE"].write_bytes(pem)
            result["WEB_PRIVATE_KEY_FILE"].write_bytes(key_pem)
    return result


def oidc_certificates(run: Path) -> dict[str, Path]:
    """A provider-only CA and certificate; neither is trusted globally."""
    now = datetime.now(UTC)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "OIDC smoke CA")])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - timedelta(minutes=5)).not_valid_after(now + timedelta(days=2))
          .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
          .sign(ca_key, hashes.SHA256()))
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "oidc-idp")])
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(ca_name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=5)).not_valid_after(now + timedelta(days=2))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("oidc-idp")]), critical=False)
            .sign(ca_key, hashes.SHA256()))
    paths = {"OIDC_STUB_CA_FILE": run / "oidc_ca.pem",
             "OIDC_STUB_CERT_FILE": run / "oidc_cert.pem",
             "OIDC_STUB_KEY_FILE": run / "oidc_key.pem"}
    paths["OIDC_STUB_CA_FILE"].write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    paths["OIDC_STUB_CERT_FILE"].write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    paths["OIDC_STUB_KEY_FILE"].write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption()))
    return paths


def inventory(env: dict[str, str]) -> dict:
    return {
        "containers": command(["docker", "ps", "-a", "--format", "{{.ID}} {{.Names}}"], env=env, capture=True).splitlines(),
        "networks": command(["docker", "network", "ls", "--format", "{{.ID}} {{.Name}}"], env=env, capture=True).splitlines(),
        "volumes": command(["docker", "volume", "ls", "--format", "{{.Name}}"], env=env, capture=True).splitlines(),
        "images": command(["docker", "image", "ls", "-a", "--no-trunc", "--format", "{{.ID}} {{.Repository}}:{{.Tag}}"], env=env, capture=True).splitlines(),
        "builders": sorted(set(command(["docker", "buildx", "ls", "--format", "{{.Name}}"], env=env, capture=True).splitlines())),
        "default_cache": sorted(
            item["ID"] for line in command(["docker", "buildx", "du", "--format", "json"], env=env, capture=True).splitlines()
            if (item := json.loads(line)).get("ID")
        ),
    }


def remove_new_images(before: dict, env: dict[str, str], project: str,
                      profile: str) -> None:
    prior = set(before["images"])
    prior_ids = {entry.split(" ", 1)[0] for entry in prior}
    allowed = (f"{project}-", "fluent/fluent-bit:4.2.3", "moby/buildkit:buildx-stable-1",
               "node:20.19.5-bookworm-slim",
               "mcr.microsoft.com/playwright:v1.63.0-noble",
               "python:3.12-slim", "node:20.19.5-alpine", "nginx:1.30.5-alpine",
               "postgres:18", "apache/kafka:4.2.0")
    if profile == "external-end-to-end":
        allowed += ("wazuh/wazuh-indexer:",)
    for entry in inventory(env)["images"]:
        if entry in prior:
            continue
        image_id, tag = entry.split(" ", 1)
        if image_id in prior_ids:
            continue
        if not tag.startswith(allowed):
            raise RuntimeError(f"New image has unclear ownership: {tag}")
        attached = command(["docker", "ps", "-a", "--filter", f"ancestor={image_id}",
                            "--format", "{{.ID}}"], env=env, capture=True).strip()
        if attached:
            raise RuntimeError(f"New image is still attached to a container: {tag}")
        command(["docker", "image", "rm", image_id], env=env)


def compare_inventory(before: dict, after: dict, project: str) -> None:
    for kind in ("containers", "volumes", "images", "builders", "default_cache"):
        added = set(after[kind]) - set(before[kind])
        if added:
            raise RuntimeError(f"Resources remain after {project}: {kind}={sorted(added)[:8]}")
    builtin_networks = {"bridge", "host", "none"}
    before_networks = {
        item for item in before["networks"] if item.split(" ", 1)[1] not in builtin_networks
    }
    after_networks = {
        item for item in after["networks"] if item.split(" ", 1)[1] not in builtin_networks
    }
    added_networks = after_networks - before_networks
    if added_networks:
        raise RuntimeError(
            f"Resources remain after {project}: networks={sorted(added_networks)[:8]}"
        )


def frontend_artifact_snapshot() -> dict[str, tuple[int, str]]:
    frontend = ROOT / "frontend"
    paths = (
        frontend / "dist",
        frontend / "tsconfig.tsbuildinfo",
        frontend / "node_modules" / ".vite",
        frontend / "node_modules" / ".vite-temp",
        frontend / "node_modules" / ".cache",
        frontend / "test-results",
        frontend / "coverage",
    )
    snapshot: dict[str, tuple[int, str]] = {}
    for path in paths:
        if not path.exists():
            continue
        files = [path] if path.is_file() else sorted(
            item for item in path.rglob("*") if item.is_file()
        )
        for item in files:
            relative = str(item.relative_to(frontend))
            snapshot[relative] = (
                item.stat().st_size,
                hashlib.sha256(item.read_bytes()).hexdigest(),
            )
    return snapshot


def oidc_compose_overlay(run: Path, env: dict[str, str], project: str) -> Path:
    """Build a temporary IdP image and keep its listener on an internal project network."""
    context = run / "oidc-idp"
    context.mkdir()
    (context / "oidc_proxy_stub.py").write_bytes(
        (ROOT / "backend/tests/oidc_proxy_stub.py").read_bytes()
    )
    versions = subprocess.run(
        [sys.executable, "-c", (
            "import importlib.metadata as m; "
            "print(m.version('cryptography')); print(m.version('joserfc'))"
        )],
        cwd=ROOT, env=env, check=True, text=True, capture_output=True,
    ).stdout.splitlines()
    if len(versions) != 2:
        raise RuntimeError("Unable to resolve the pinned synthetic IdP dependencies")
    (context / "Dockerfile").write_text(
        "FROM python:3.12-slim\n"
        f"RUN python -m pip install --no-cache-dir cryptography=={versions[0]} joserfc=={versions[1]}\n"
        "WORKDIR /app\nCOPY oidc_proxy_stub.py ./oidc_proxy_stub.py\n"
        "CMD [\"python\", \"/app/oidc_proxy_stub.py\"]\n",
        encoding="utf-8",
    )
    dev_http = env.get("OIDC_STUB_SCHEME") == "http"
    overlay = {
        "services": {
            "oidc-idp": {
                "image": f"{project}-oidc-idp",
                "build": {"context": str(context), "dockerfile": "Dockerfile"},
                "environment": {
                    "OIDC_STUB_PORT": env["OIDC_STUB_PORT"],
                    "OIDC_STUB_HOST": "oidc-idp",
                    "OIDC_STUB_BROWSER_HOST": "oidc-idp",
                    "OIDC_STUB_REALM_PATH": "/realms/master" if dev_http else "",
                    **({"OIDC_STUB_PUBLIC_ISSUER": env["OIDC_DEV_HTTP_ISSUER"]} if dev_http else {}),
                    **({} if dev_http else {
                        "OIDC_STUB_CERT_FILE": "/run/oidc/cert.pem",
                        "OIDC_STUB_KEY_FILE": "/run/oidc/key.pem",
                    }),
                },
                "volumes": [] if dev_http else [
                    f"{Path(env['OIDC_STUB_CERT_FILE']).resolve()}:/run/oidc/cert.pem:ro",
                    f"{Path(env['OIDC_STUB_KEY_FILE']).resolve()}:/run/oidc/key.pem:ro",
                ],
                **({"ports": [f"127.0.0.1:{env['OIDC_STUB_PUBLIC_PORT']}:{env['OIDC_STUB_PORT']}"]}
                   if dev_http else {}),
                "networks": ({"oidc-private": {"aliases": ["host.docker.internal"]},
                              "oidc-browser": {}}
                             if dev_http else ["oidc-private"]),
                "healthcheck": {
                    "test": ["CMD", "python", "-c",
                             f"import socket; s=socket.create_connection(('127.0.0.1',{env['OIDC_STUB_PORT']}),3); s.close()"],
                    "interval": "1s", "timeout": "3s", "retries": 30,
                },
                "mem_limit": "256m", "cpus": 1.0,
                "restart": "no",
            },
            "api": {
                "depends_on": {"oidc-idp": {"condition": "service_healthy"}},
                "networks": ["internal", "oidc-private"],
            },
            "proxy": {
                "networks": {
                    "internal": {"ipv4_address": env["PROXY_IP"]},
                    "oidc-private": {},
                },
            },
        },
        "networks": {"oidc-private": {"driver": "bridge", "internal": True},
                     **({"oidc-browser": {"driver": "bridge"}} if dev_http else {})},
    }
    path = run / "oidc-compose.json"
    path.write_text(json.dumps(overlay, indent=2) + "\n", encoding="utf-8")
    return path


def wazuh_compose_overlay(run: Path, env: dict[str, str]) -> Path:
    """Attach the pinned Wazuh Indexer to the same isolated Compose project."""
    source = ROOT / "backend" / "tests" / "opensearch-wazuh5"
    image = "wazuh/wazuh-indexer@sha256:70d7c42518e4e295a73947ef916fc26e67f81658dd0c892d7032018b296607ce"
    config = run / "wazuh-opensearch.yml"
    config.write_bytes((source / "opensearch.yml").read_bytes())
    password_file = Path(env["WAZUH_TEST_ADMIN_PASSWORD_FILE"])
    password_hash = command([
        "docker", "run", "--rm", "--entrypoint", "sh",
        "-v", f"{password_file.resolve()}:/tmp/admin-password:ro", image,
        "-c", "OPENSEARCH_JAVA_HOME=/usr/share/wazuh-indexer/jdk "
              "/usr/share/wazuh-indexer/plugins/opensearch-security/tools/hash.sh "
              "-p \"$(cat /tmp/admin-password)\"",
    ], env=env, capture=True, timeout=60).strip()
    if not password_hash.startswith(("$2a$", "$2y$")):
        raise RuntimeError("Wazuh did not return a bcrypt admin hash")
    users = run / "wazuh-internal-users.yml"
    users.write_text(
        "_meta:\n  type: internalusers\n  config_version: 2\n"
        f"admin:\n  hash: '{password_hash}'\n  reserved: true\n"
        "  backend_roles:\n    - admin\n",
        encoding="utf-8",
    )
    overlay = {
        "services": {
            "postgres": {"ports": [env["POSTGRES_BIND"]]},
            "indexer": {
                "image": image,
                "environment": {
                    "OPENSEARCH_JAVA_OPTS": "-Xms1g -Xmx1g",
                },
                "ports": [f"127.0.0.1:{env['WAZUH_TEST_PORT']}:9200"],
                "volumes": [
                    f"{config.resolve()}:/usr/share/wazuh-indexer/config/opensearch.yml:ro",
                    f"{(Path(env['WAZUH_TEST_RUNTIME']) / 'certs').resolve()}:/usr/share/wazuh-indexer/config/certs:ro",
                    f"{users.resolve()}:/usr/share/wazuh-indexer/config/opensearch-security/internal_users.yml:ro",
                    "indexer_data:/var/lib/wazuh-indexer",
                ],
                "ulimits": {"memlock": {"soft": -1, "hard": -1}},
                "healthcheck": {
                    "test": ["CMD-SHELL", "test \"$(curl --silent --output /dev/null --write-out '%{http_code}' --cacert /usr/share/wazuh-indexer/config/certs/root-ca.pem https://localhost:9200/)\" = 401"],
                    "interval": "10s", "timeout": "5s", "retries": 18,
                    "start_period": "90s",
                },
                "mem_limit": "2g", "cpus": 2.0, "restart": "no",
                "networks": ["internal"],
            },
        },
        "volumes": {"indexer_data": {}},
    }
    path = run / "wazuh-compose.json"
    path.write_text(json.dumps(overlay, indent=2) + "\n", encoding="utf-8")
    return path


def prepare(run: Path, profile: str) -> tuple[dict[str, str], list[str], str]:
    suffix = uuid4().hex[:10]
    project = f"diploma_t21_{suffix}"
    db_port, kafka_port, proxy_port, https_port = (free_port() for _ in range(4))
    subnet = f"172.31.{100 + int(suffix[:2], 16) % 100}.0/24"
    db_password = secrets.token_urlsafe(24)
    files = {
        "POSTGRES_PASSWORD_FILE": secret_file(run, "postgres_password", db_password),
        "KAFKA_ADMIN_PASSWORD_FILE": secret_file(run, "kafka_admin_password", secrets.token_urlsafe(32)),
        "KAFKA_READER_PASSWORD_FILE": secret_file(run, "kafka_reader_password", secrets.token_urlsafe(32)),
        "AGENT_CREDENTIALS_KEY_FILE": secret_file(run, "agent_credentials_key", Fernet.generate_key().decode()),
    }
    production = profile in {"smoke-production", "external-end-to-end"}
    if profile in {"smoke-development", "smoke-production"}:
        files.update(oidc_certificates(run))
    if production:
        files.update(certificates(run))
        files.update({
            "DATABASE_URL_FILE": secret_file(
                run, "database_url",
                f"postgresql+psycopg://diploma_user:{db_password}@postgres:5432/diploma_test",
            ),
            "OIDC_STATE_SECRET_FILE": secret_file(run, "oidc_state_secret", secrets.token_urlsafe(40)),
            "OIDC_CLIENT_SECRET_FILE": secret_file(run, "oidc_client_secret", secrets.token_urlsafe(40)),
            "EXTERNAL_SECRET_KEY_FILE": secret_file(run, "external_secret_key", Fernet.generate_key().decode()),
        })
    env = {**os.environ, **{name: str(path) for name, path in files.items()},
           "COMPOSE_PROJECT_NAME": project, "POSTGRES_PASSWORD": db_password,
           "POSTGRES_DB": "diploma_test", "POSTGRES_BIND": f"127.0.0.1:{db_port}:5432",
           "KAFKA_BIND_ADDRESS": "127.0.0.1", "KAFKA_PUBLIC_HOST": "127.0.0.1",
           "KAFKA_HOST_PORT": str(kafka_port), "KAFKA_EXTERNAL_PORT": str(kafka_port),
           "PROXY_BIND": f"127.0.0.1:{proxy_port}:8080",
           "PUBLIC_ORIGIN": f"http://localhost:{proxy_port}",
           "COMPOSE_SUBNET": subnet, "PROXY_IP": subnet.replace("0/24", "10"),
           "TEMP": str(run), "TMP": str(run), "TMPDIR": str(run),
           "PYTHONDONTWRITEBYTECODE": "1", "PIP_CACHE_DIR": str(run / "pip-cache"),
           "npm_config_cache": str(run / "npm-cache"),
           "PYTEST_ADDOPTS": pytest_addopts(run),
           "TEST_DATABASE_URL": f"postgresql+psycopg://diploma_user:{db_password}@127.0.0.1:{db_port}/diploma_test",
           "KAFKA_TEST_BOOTSTRAP_SERVERS": f"127.0.0.1:{kafka_port}",
           "KAFKA_TEST_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
           "KAFKA_TEST_READER_PASSWORD_FILE": str(files["KAFKA_READER_PASSWORD_FILE"]),
           "KAFKA_TEST_ADMIN_PASSWORD_FILE": str(files["KAFKA_ADMIN_PASSWORD_FILE"]),
           "KAFKA_TEST_DEV_ALLOWED_BOOTSTRAP_SERVERS": f"127.0.0.1:{kafka_port},kafka:9092",
           }
    if profile == "external-end-to-end":
        indexer_port = free_port()
        runtime = run / "wazuh-runtime"
        env.update({"WAZUH_TEST_RUNTIME": str(runtime),
                    "WAZUH_TEST_PORT": str(indexer_port),
                    "WAZUH_TEST_BASE_URL": f"https://localhost:{indexer_port}",
                    "WAZUH_TEST_ADMIN_PASSWORD_FILE": str(secret_file(
                        run, "wazuh_initial_admin_password", "Aa1!" + secrets.token_urlsafe(28))),
                    "WAZUH_SMOKE_URL": f"https://localhost:{indexer_port}",
                    "WAZUH_SMOKE_CA": str(runtime / "certs" / "root-ca.pem")})
        command([sys.executable, str(ROOT / "backend" / "tests" / "opensearch-wazuh5" / "generate_certs.py")],
                env=env)
    if profile in {"smoke-development", "smoke-production"}:
        env.update({"OIDC_STUB_PORT": "19095", "OIDC_STUB_HOST": "oidc-idp",
                    "OIDC_STUB_BROWSER_HOST": "oidc-idp",
                    "OIDC_STUB_SCHEME": "https" if production else "http"})
    if profile == "smoke-development":
        env["EXTERNAL_SECRET_KEY"] = Fernet.generate_key().decode()
        env["OIDC_STUB_PUBLIC_PORT"] = str(free_port())
        env["OIDC_STUB_PORT"] = env["OIDC_STUB_PUBLIC_PORT"]
        env["OIDC_DEV_HTTP_ISSUER"] = f"http://localhost:{env['OIDC_STUB_PUBLIC_PORT']}/realms/master"
    else:
        env["OIDC_DEV_HTTP_ISSUER"] = ""
    if production:
        env.update({
            "KAFKA_PUBLIC_HOST": "127.0.0.1",
            "PUBLIC_HOST": "localhost",
            "HTTP_BIND": f"127.0.0.1:{proxy_port}:80",
            "HTTPS_BIND": f"127.0.0.1:{https_port}:443",
            "PUBLIC_ORIGIN": f"https://localhost:{https_port}",
            "KAFKA_TEST_BOOTSTRAP_SERVERS": f"127.0.0.1:{kafka_port}",
            "KAFKA_TEST_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_TEST_CA_FILE": str(files["KAFKA_CA_FILE"]),
        })
        env.pop("KAFKA_TEST_DEV_ALLOWED_BOOTSTRAP_SERVERS", None)
    limits = run / "compose-limits.yml"
    limits.write_text("services:\n" + "".join(
        f"  {service}:\n    mem_limit: {memory}\n    cpus: 2.0\n"
        + ("    build:\n      network: host\n" if service in {"kafka", "kafka-acls"} else "")
        for service, memory in {
            "postgres": "768m", "kafka": "2g", "kafka-acls": "512m",
            "migrate": "512m", "api": "1g", "provisioner": "768m",
            "proxy": "256m", "frontend": "2g",
        }.items() if not production or service != "frontend"
    ), encoding="utf-8")
    compose = [*COMPOSE, "-f", str(ROOT / ("docker-compose.prod.yml" if production else "docker-compose.dev.yml")),
               "-f", str(limits), "-p", project]
    if profile in {"smoke-development", "smoke-production"}:
        compose += ["-f", str(oidc_compose_overlay(run, env, project))]
    return env, compose, project


def check_config(compose: list[str], project: str, env: dict[str, str], run: Path):
    config = json.loads(command([*compose, "config", "--format", "json"], env=env, capture=True))
    if config["name"] != project:
        raise RuntimeError("Compose project name is not isolated")
    for name, volume in config.get("volumes", {}).items():
        if volume.get("external") or (volume.get("name") and not volume["name"].startswith(project + "_")):
            raise RuntimeError(f"Unsafe Compose volume {name}")
    for name, network in config.get("networks", {}).items():
        if network.get("external") or not network.get("name", "").startswith(project + "_"):
            raise RuntimeError(f"Unsafe Compose network {name}")
    if "oidc-idp" in config.get("services", {}):
        private = config.get("networks", {}).get("oidc-private", {})
        if not private.get("internal"):
            raise RuntimeError("Synthetic OIDC network is not marked internal")
        idp = config["services"]["oidc-idp"]
        expected_networks = ({"oidc-private": {"aliases": ["host.docker.internal"]},
                              "oidc-browser": {}}
                             if env.get("OIDC_STUB_SCHEME") == "http" else {"oidc-private": None})
        if idp.get("network_mode") or idp.get("networks") != expected_networks:
            raise RuntimeError(
                f"Synthetic OIDC provider networks differ from the profile: "
                f"actual={idp.get('networks')!r}, expected={expected_networks!r}"
            )
        expected_ports = ([{"mode": "ingress", "target": int(env["OIDC_STUB_PORT"]),
                            "published": env["OIDC_STUB_PUBLIC_PORT"],
                            "protocol": "tcp", "host_ip": "127.0.0.1"}]
                          if env.get("OIDC_STUB_SCHEME") == "http" else [])
        if idp.get("ports", []) != expected_ports:
            raise RuntimeError("Synthetic OIDC provider port binding differs from the profile")
        if "oidc-private" not in config["services"]["api"].get("networks", {}):
            raise RuntimeError("API cannot reach the isolated synthetic OIDC provider")
        if "oidc-private" not in config["services"]["proxy"].get("networks", {}):
            raise RuntimeError("Browser smoke network cannot reach the synthetic OIDC provider")
    for name, secret in config.get("secrets", {}).items():
        if not secret.get("file") or not Path(secret["file"]).resolve().is_relative_to(run):
            raise RuntimeError(f"Secret {name} is not isolated to this run")
    for name, service in config.get("services", {}).items():
        if service.get("privileged") or service.get("devices") or service.get("network_mode") or service.get("pid"):
            raise RuntimeError(f"Unsafe container privileges for {name}")
        if not service.get("mem_limit") or not service.get("cpus"):
            raise RuntimeError(f"Missing resource limit for {name}")
        if service.get("build"):
            context = Path(service["build"]["context"]).resolve()
            if (context not in {ROOT, ROOT / "backend", ROOT / "frontend", ROOT / "kafka"}
                    and not context.is_relative_to(run.resolve())):
                raise RuntimeError(f"Unexpected build context for {name}")
        for port in service.get("ports", []):
            if port.get("host_ip") not in {"127.0.0.1", "::1"}:
                raise RuntimeError(f"Port for {name} is not bound to loopback")
        for mount in service.get("volumes", []):
            if mount["type"] == "bind":
                source = Path(mount["source"]).resolve()
                if "docker.sock" in str(source).lower():
                    raise RuntimeError("Docker socket mount is forbidden")
                if (
                    source.is_relative_to(run)
                    or source == ROOT / "frontend" and name == "frontend"
                    or source in {ROOT / "nginx" / "dev.conf", ROOT / "nginx" / "prod.conf"}
                    and mount.get("read_only")
                ):
                    pass
                else:
                    raise RuntimeError(f"Unexpected bind mount for {name}: {source}")
            elif mount["type"] == "volume" and mount.get("source") not in config.get("volumes", {}):
                raise RuntimeError(f"Unknown Compose volume for {name}")


def verify_kafka_metadata(compose: list[str], env: dict[str, str]) -> None:
    """Check the advertised external and internal listeners before live tests."""
    host = env["KAFKA_TEST_BOOTSTRAP_SERVERS"]
    options = {
        "bootstrap.servers": host,
        "security.protocol": env["KAFKA_TEST_SECURITY_PROTOCOL"],
        "sasl.mechanisms": "SCRAM-SHA-512",
        "sasl.username": "diploma-kafka-admin",
        "sasl.password": Path(env["KAFKA_TEST_ADMIN_PASSWORD_FILE"]).read_text(encoding="utf-8").strip(),
    }
    if env.get("KAFKA_TEST_CA_FILE"):
        options["ssl.ca.location"] = env["KAFKA_TEST_CA_FILE"]
    external = AdminClient(options).list_topics(timeout=15)
    if not external.cluster_id or not external.brokers:
        raise RuntimeError("External Kafka metadata has no cluster ID or brokers")
    advertised = {(broker.host, broker.port) for broker in external.brokers.values()}
    expected_host, expected_port = host.rsplit(":", 1)
    if (expected_host, int(expected_port)) not in advertised:
        raise RuntimeError(f"Kafka advertises {advertised} instead of {host}")
    internal = command(
        [*compose, "exec", "-T", "kafka", "/opt/kafka/bin/kafka-cluster.sh",
         "cluster-id", "--bootstrap-server", "kafka:9092", "--command-config",
         "/tmp/diploma-kafka-admin.properties"], env=env, capture=True,
    )
    if external.cluster_id not in internal:
        raise RuntimeError("Internal and external Kafka listeners disagree on cluster ID")
    print(f"Kafka metadata verified: cluster={external.cluster_id}, external={host}, internal=kafka:9092", flush=True)


def verify_api_metadata(compose: list[str], env: dict[str, str]) -> None:
    script = (
        "import os,pathlib; from confluent_kafka.admin import AdminClient; "
        "c={'bootstrap.servers':'kafka:9092','security.protocol':os.environ['KAFKA_SECURITY_PROTOCOL'],"
        "'sasl.mechanisms':'SCRAM-SHA-512','sasl.username':os.environ['KAFKA_RUNTIME_USERNAME'],"
        "'sasl.password':pathlib.Path(os.environ['KAFKA_RUNTIME_PASSWORD_FILE']).read_text().strip()}; "
        "ca=os.environ.get('KAFKA_CA_FILE'); "
        "c.update({'ssl.ca.location':ca}) if ca else None; "
        "m=AdminClient(c).list_topics(timeout=15); print(m.cluster_id)"
    )
    internal = command([*compose, "exec", "-T", "api", "python", "-c", script], env=env, capture=True).strip()
    options = {
        "bootstrap.servers": env["KAFKA_TEST_BOOTSTRAP_SERVERS"],
        "security.protocol": env["KAFKA_TEST_SECURITY_PROTOCOL"],
        "sasl.mechanisms": "SCRAM-SHA-512",
        "sasl.username": "diploma-kafka-admin",
        "sasl.password": Path(env["KAFKA_TEST_ADMIN_PASSWORD_FILE"]).read_text(encoding="utf-8").strip(),
    }
    if env.get("KAFKA_TEST_CA_FILE"):
        options["ssl.ca.location"] = env["KAFKA_TEST_CA_FILE"]
    external = AdminClient(options).list_topics(timeout=15)
    if internal != external.cluster_id:
        raise RuntimeError("API and external Kafka clients disagree on cluster ID")
    print(f"API metadata verified: cluster={internal}, internal=kafka:9092", flush=True)


def verify_agent_metadata(compose: list[str], project: str, env: dict[str, str]) -> None:
    kafka_container = command([*compose, "ps", "-q", "kafka"], env=env, capture=True).strip()
    if not kafka_container:
        raise RuntimeError("Kafka container is unavailable for agent metadata")
    script = (
        "import os,pathlib; from confluent_kafka.admin import AdminClient; "
        "c={'bootstrap.servers':os.environ['KAFKA_TEST_BOOTSTRAP_SERVERS'],"
        "'security.protocol':os.environ['KAFKA_TEST_SECURITY_PROTOCOL'],"
        "'sasl.mechanisms':'SCRAM-SHA-512','sasl.username':'diploma-kafka-admin',"
        "'sasl.password':pathlib.Path('/test/admin').read_text().strip()}; "
        "c.update({'ssl.ca.location':'/test/ca'}) if os.path.exists('/test/ca') else None; "
        "m=AdminClient(c).list_topics(timeout=15); print(m.cluster_id)"
    )
    args = ["docker", "run", "--rm", "--memory", "512m", "--cpus", "1",
            "--network", f"container:{kafka_container}",
            "-v", f"{Path(env['KAFKA_TEST_ADMIN_PASSWORD_FILE']).resolve()}:/test/admin:ro",
            "-e", "KAFKA_TEST_BOOTSTRAP_SERVERS", "-e", "KAFKA_TEST_SECURITY_PROTOCOL"]
    if env.get("KAFKA_TEST_CA_FILE"):
        args += ["-v", f"{Path(env['KAFKA_TEST_CA_FILE']).resolve()}:/test/ca:ro"]
    agent_cluster = command([*args, f"{project}-api", "python", "-c", script], env=env, capture=True).strip()
    host_options = {
        "bootstrap.servers": env["KAFKA_TEST_BOOTSTRAP_SERVERS"],
        "security.protocol": env["KAFKA_TEST_SECURITY_PROTOCOL"],
        "sasl.mechanisms": "SCRAM-SHA-512", "sasl.username": "diploma-kafka-admin",
        "sasl.password": Path(env["KAFKA_TEST_ADMIN_PASSWORD_FILE"]).read_text(encoding="utf-8").strip(),
    }
    if env.get("KAFKA_TEST_CA_FILE"):
        host_options["ssl.ca.location"] = env["KAFKA_TEST_CA_FILE"]
    if agent_cluster != AdminClient(host_options).list_topics(timeout=15).cluster_id:
        raise RuntimeError("Test agent and host Kafka cluster IDs disagree")
    print(f"Test agent metadata verified: cluster={agent_cluster}, external={env['KAFKA_TEST_BOOTSTRAP_SERVERS']}", flush=True)


def smoke_http(env: dict[str, str], production: bool) -> None:
    bind = env["HTTPS_BIND"] if production else env["PROXY_BIND"]
    port = bind.split(":")[1]
    url = f"{'https' if production else 'http'}://localhost:{port}"
    context = ssl.create_default_context(cafile=env["KAFKA_CA_FILE"]) if production else None
    deadline = time.monotonic() + 35
    while True:
        try:
            with urllib.request.urlopen(url + "/api/v1/health/ready", context=context, timeout=5) as response:
                if response.status == 200 and json.load(response).get("status") == "ok":
                    break
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
            pass
        if time.monotonic() >= deadline:
            raise RuntimeError("API readiness through proxy failed")
        time.sleep(1)
    if production:
        import re
        def response(path: str):
            try:
                with urllib.request.urlopen(url + path, context=context, timeout=15) as result:
                    return result.status, result.headers, result.read()
            except urllib.error.HTTPError as error:
                return error.code, error.headers, error.read()

        status, headers, html = response("/login")
        if status != 200 or "text/html" not in headers.get("Content-Type", "") or b"/assets/" not in html:
            raise RuntimeError("Production login route did not serve React HTML")
        for path in ("/", "/agents", "/events", "/administration", "/normalizers/new"):
            route_status, route_headers, _ = response(path)
            if route_status != 200 or "text/html" not in route_headers.get("Content-Type", ""):
                raise RuntimeError(f"Production UI route failed: {path}")
        assets = re.findall(rb'(?:src|href)="(/assets/[^\"]+)"', html)
        if not assets or not any(asset.endswith(b".js") for asset in assets):
            raise RuntimeError("Production HTML has no JavaScript assets")
        for asset in assets:
            asset_status, asset_headers, asset_body = response(asset.decode("ascii"))
            if asset_status != 200 or not asset_body or "text/html" in asset_headers.get("Content-Type", ""):
                raise RuntimeError(f"Production asset failed: {asset.decode('ascii')}")
        for path in ("/api", "/api/v1/does-not-exist", "/assets/missing.js", "/docs", "/redoc", "/openapi.json"):
            error_status, _, error_body = response(path)
            if error_status < 400 or b'<div id="root"' in error_body:
                raise RuntimeError(f"Production route unexpectedly served React: {path}")
    else:
        with urllib.request.urlopen(url + "/agents", timeout=15) as response:
            if response.status != 200 or "text/html" not in response.headers.get("Content-Type", ""):
                raise RuntimeError("Frontend route through proxy failed")
    print(f"Proxy smoke passed: {url} (API and frontend)", flush=True)


def oidc_administration_smoke(compose: list[str], env: dict[str, str], production: bool) -> None:
    """Exercise managed OIDC through the same proxy and database as the smoke profile."""
    port = (env["HTTPS_BIND"] if production else env["PROXY_BIND"]).split(":")[1]
    base = f"{'https' if production else 'http'}://localhost:{port}"
    context = ssl.create_default_context()
    if production:
        context.load_verify_locations(cafile=env["OIDC_STUB_CA_FILE"])
        context.load_verify_locations(cafile=env["KAFKA_CA_FILE"])
    class FixtureHTTPSConnection(http.client.HTTPSConnection):
        def __init__(self, host, **kwargs):
            super().__init__(host, **kwargs)
            original_connect = self._create_connection

            def mapped_connect(address, *args, **kwargs):
                if address[0] == "oidc-idp":
                    address = ("127.0.0.1", address[1])
                return original_connect(address, *args, **kwargs)

            self._create_connection = mapped_connect

    class FixtureHTTPSHandler(urllib.request.HTTPSHandler):
        def https_open(self, request):
            return self.do_open(FixtureHTTPSConnection, request, context=self._context)

    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}),
                                         urllib.request.HTTPCookieProcessor(jar),
                                         FixtureHTTPSHandler(context=context))
    password = secrets.token_urlsafe(24)
    create = (
        "import os,app.cli as cli; "
        "cli.getpass.getpass=lambda prompt: os.environ['TASK27_ADMIN_PASSWORD']; "
        "raise SystemExit(cli.create_admin('task27admin','OIDC smoke admin'))"
    )
    command([*compose, "exec", "-T", "-e", f"TASK27_ADMIN_PASSWORD={password}",
             "api", "python", "-c", create], env=env)

    def call(path: str, method="GET", payload=None, csrf=None, content_type="application/json"):
        headers = {"Content-Type": content_type} if payload is not None else {}
        if csrf:
            headers["X-CSRF-Token"] = csrf
        data = (json.dumps(payload).encode() if content_type == "application/json"
                else payload) if payload is not None else None
        request = urllib.request.Request(base + path, data=data, headers=headers, method=method)
        try:
            with opener.open(request, timeout=20) as response:
                body = response.read()
                return response.status, json.loads(body) if response.headers.get_content_type() == "application/json" else None
        except urllib.error.HTTPError as error:
            body = error.read()
            try:
                return error.code, json.loads(body)
            except ValueError:
                return error.code, None

    status, local = call("/api/v1/auth/local/login", "POST", {
        "username": "task27admin", "password": password,
    })
    if status != 200 or not local.get("csrf_token"):
        raise RuntimeError("OIDC smoke local admin login failed")
    csrf = local["csrf_token"]
    status, current = call("/api/v1/oidc-provider")
    if status != 200 or current["mode"] != "env":
        raise RuntimeError("OIDC smoke initial configuration invalid")
    if production:
        status, refused = call("/api/v1/oidc-provider/draft", "PUT", {
            "version": current["version"],
            "issuer_url": "http://oidc-idp:19095/realms/master",
            "client_id": "smoke-client",
            "client_secret": "synthetic-smoke-secret",
            "redirect_uri": base + "/api/v1/auth/oidc/callback",
            "scopes": ["openid"],
        }, csrf)
        if status != 422 or refused["code"] != "oidc_https_required":
            raise RuntimeError("Production accepted a development HTTP issuer")
    dev_http = not production
    issuer = (env["OIDC_DEV_HTTP_ISSUER"] if dev_http
              else f"https://oidc-idp:{env['OIDC_STUB_PORT']}")
    status, saved = call("/api/v1/oidc-provider/draft", "PUT", {
        "version": current["version"], "issuer_url": issuer, "client_id": "smoke-client",
        "client_secret": "synthetic-smoke-secret",
        "redirect_uri": base + "/api/v1/auth/oidc/callback",
        "scopes": ["openid", "profile", "email"],
    }, csrf)
    if status != 200 or not saved["draft"]["has_secret"]:
        raise RuntimeError("OIDC smoke draft save failed")
    status, failure = call("/api/v1/oidc-provider/draft/check", "POST", {"version": saved["version"]}, csrf)
    if production and (status != 422 or failure["code"] not in {"oidc_tls_invalid", "oidc_hostname_mismatch"}):
        if isinstance(failure, dict) and failure.get("code") == "oidc_unreachable":
            diagnostic = (
                "import socket; h='oidc-idp'; p=" + env["OIDC_STUB_PORT"] + "; "
                "print('DNS', socket.getaddrinfo(h,p,type=socket.SOCK_STREAM)); "
                "s=socket.create_connection((h,p),timeout=5); print('TCP connected'); s.close()"
            )
            result = subprocess.run([*compose, "exec", "-T", "api", "python", "-c", diagnostic],
                                    env=env, text=True, capture_output=True, check=False)
            print(f"OIDC route diagnostic: exit={result.returncode}; stdout={result.stdout[-1200:]}; stderr={result.stderr[-1200:]}", flush=True)
        raise RuntimeError(f"OIDC smoke untrusted CA refusal: HTTP {status}, code {failure.get('code') if isinstance(failure, dict) else None}")
    if production:
        ca = Path(env["OIDC_STUB_CA_FILE"]).read_bytes()
        status, saved = call(f"/api/v1/oidc-provider/draft/ca?version={saved['version']}",
                             "PUT", ca, csrf, "application/x-pem-file")
        if status != 200 or not saved["draft"]["has_ca"]:
            raise RuntimeError("OIDC smoke CA upload failed")
    elif status != 200 or not failure.get("checked"):
        raise RuntimeError(f"OIDC smoke dev discovery failed: HTTP {status}, response={failure!r}")
    idp_ip = command([*compose, "exec", "-T", "api", "python", "-c",
                      "import socket; print(socket.gethostbyname('oidc-idp'))"],
                     env=env, capture=True).strip()
    status, wrong_host = call("/api/v1/oidc-provider/draft", "PUT", {
        "version": saved["version"],
        "issuer_url": f"{'http' if dev_http else 'https'}://{idp_ip}:{env['OIDC_STUB_PORT']}" + ("/realms/master" if dev_http else ""),
        "client_id": "smoke-client",
        "redirect_uri": base + "/api/v1/auth/oidc/callback",
        "scopes": ["openid", "profile", "email"],
    }, csrf)
    if dev_http:
        if status != 422 or wrong_host["code"] != "oidc_https_required":
            raise RuntimeError("OIDC smoke foreign HTTP issuer was accepted")
        restore_version = saved["version"]
    else:
        if status != 200:
            raise RuntimeError("OIDC smoke hostname setup failed")
        status, failure = call("/api/v1/oidc-provider/draft/check", "POST", {"version": wrong_host["version"]}, csrf)
        if status != 422 or failure["code"] != "oidc_hostname_mismatch":
            raise RuntimeError(f"OIDC smoke wrong hostname refusal: HTTP {status}, code {failure.get('code') if isinstance(failure, dict) else None}")
        restore_version = wrong_host["version"]
    status, saved = call("/api/v1/oidc-provider/draft", "PUT", {
        "version": restore_version, "issuer_url": issuer,
        "client_id": "smoke-client",
        "redirect_uri": base + "/api/v1/auth/oidc/callback",
        "scopes": ["openid", "profile", "email"],
    }, csrf)
    if status != 200:
        raise RuntimeError("OIDC smoke valid issuer restore failed")
    status, checked = call("/api/v1/oidc-provider/draft/check", "POST", {"version": saved["version"]}, csrf)
    if status != 200 or not checked["checked"]:
        raise RuntimeError("OIDC smoke discovery check failed")
    oidc_browser_trial(compose, env, production, password)
    status, tried = call("/api/v1/oidc-provider")
    if status != 200 or tried["mode"] != "managed":
        raise RuntimeError("OIDC smoke activation did not persist")
    status, session = call("/api/v1/auth/session")
    if status != 200 or session["user"]["username"] != "task27admin":
        raise RuntimeError("OIDC trial replaced local admin session")
    if call("/api/v1/auth/methods")[1]["oidc_enabled"] is not True:
        raise RuntimeError("OIDC smoke runtime method did not update")
    call("/api/v1/auth/logout", "POST", {}, csrf)
    status, local = call("/api/v1/auth/local/login", "POST", {
        "username": "task27admin", "password": password,
    })
    if status != 200:
        raise RuntimeError("OIDC smoke local fallback failed")
    status, disabled = call("/api/v1/oidc-provider/disable", "POST", {"version": checked["version"]}, local["csrf_token"])
    if status != 200 or disabled["mode"] != "disabled":
        raise RuntimeError("OIDC smoke disable failed")
    command([*compose, "restart", "api"], env=env)
    deadline = time.monotonic() + 60
    while True:
        try:
            status, methods = call("/api/v1/auth/methods")
        except urllib.error.URLError:
            status, methods = 0, None
        if status == 200 and isinstance(methods, dict):
            if methods["oidc_enabled"] is not False:
                raise RuntimeError("OIDC smoke disabled state did not survive restart")
            break
        if time.monotonic() >= deadline:
            raise RuntimeError(f"OIDC smoke API did not recover after restart: HTTP {status}")
        time.sleep(1)
    print(f"OIDC administration smoke passed: {base} with isolated IdP {issuer}", flush=True)


def oidc_browser_trial(compose: list[str], env: dict[str, str], production: bool,
                       password: str) -> None:
    """Verify trial and active login in Chromium; trust the test CA only in production."""
    run = Path(env["TEMP"])
    if not production:
        chrome = next((candidate for candidate in (
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        ) if Path(candidate).is_file()), None)
        if chrome is None:
            raise RuntimeError("Development OIDC smoke requires local Chrome or Edge")
        browser_env = {**env, "TASK27_ADMIN_PASSWORD": password,
                       "PLAYWRIGHT_BASE_URL": f"http://localhost:{env['PROXY_BIND'].split(':')[1]}",
                       "PLAYWRIGHT_CHROME_PATH": chrome}
        command([NPM, "run", "test:browser", "--", "oidc.trial.spec.ts",
                 "--output", str(run / "oidc-browser-results")],
                env=browser_env, cwd=ROOT / "frontend", timeout=300)
        return
    context = run / "oidc-browser-context"
    context.mkdir()
    source = ROOT / "frontend"
    locked = json.loads((source / "package-lock.json").read_text(encoding="utf-8"))
    packages = locked["packages"]
    browser_packages = (
        "node_modules/@playwright/test", "node_modules/playwright",
        "node_modules/playwright-core",
    )
    version = packages[browser_packages[0]]["version"]
    if version != "1.63.0":
        raise RuntimeError(
            f"Update the pinned Playwright browser image with package version {version}"
        )
    manifest = {
        "name": "diploma-oidc-browser-smoke", "version": "1.0.0",
        "private": True, "type": "module",
        "devDependencies": {"@playwright/test": version},
    }
    browser_lock = {
        "name": manifest["name"], "version": manifest["version"],
        "lockfileVersion": 3, "requires": True,
        "packages": {"": manifest, **{name: packages[name] for name in browser_packages}},
    }
    (context / "package.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    (context / "package-lock.json").write_text(
        json.dumps(browser_lock, indent=2) + "\n", encoding="utf-8"
    )
    (context / "playwright.config.ts").write_bytes((source / "playwright.config.ts").read_bytes())
    browser_dir = context / "browser"
    browser_dir.mkdir()
    (browser_dir / "oidc.trial.spec.ts").write_bytes(
        (source / "browser" / "oidc.trial.spec.ts").read_bytes()
    )
    project = env["COMPOSE_PROJECT_NAME"]
    image = "mcr.microsoft.com/playwright:v1.63.0-noble"
    proxy = command([*compose, "ps", "-q", "proxy"], env=env, capture=True).strip()
    if not proxy:
        raise RuntimeError("OIDC browser smoke proxy is unavailable")
    results = run / "oidc-browser-results"
    results.mkdir()
    script = [
        "set -e",
        "apt-get update >/dev/null",
        "apt-get install -y --no-install-recommends libnss3-tools socat >/dev/null",
        "npm ci --ignore-scripts --no-audit --no-fund",
    ]
    mounts = []
    if production:
        mounts += ["-v", f"{Path(env['OIDC_STUB_CA_FILE']).resolve()}:/tmp/oidc-ca.pem:ro",
                   "-v", f"{Path(env['KAFKA_CA_FILE']).resolve()}:/tmp/proxy-ca.pem:ro"]
        script += [
            "mkdir -p /usr/local/share/ca-certificates /root/.pki/nssdb",
            "cp /tmp/oidc-ca.pem /usr/local/share/ca-certificates/oidc-smoke.crt",
            "cp /tmp/proxy-ca.pem /usr/local/share/ca-certificates/proxy-smoke.crt",
            "update-ca-certificates >/dev/null",
            "certutil -d sql:/root/.pki/nssdb -N --empty-password",
            "certutil -d sql:/root/.pki/nssdb -A -t 'C,,' -n oidc-smoke -i /tmp/oidc-ca.pem",
            "certutil -d sql:/root/.pki/nssdb -A -t 'C,,' -n proxy-smoke -i /tmp/proxy-ca.pem",
        ]
    public_port = (env["HTTPS_BIND"] if production else env["PROXY_BIND"]).split(":")[1]
    proxy_port = "443" if production else "8080"
    script.append(
        f"socat TCP-LISTEN:{public_port},bind=127.0.0.1,reuseaddr,fork "
        f"TCP:127.0.0.1:{proxy_port} & npx playwright test browser/oidc.trial.spec.ts --output /results"
    )
    command(["docker", "run", "--rm", "--name", f"{project}-oidc-browser-smoke",
             "--user", "0:0", "--memory", "2g", "--cpus", "2", "--network", f"container:{proxy}",
             "--label", f"task27.project={project}", *mounts,
             "-w", "/test", "-v", f"{context.resolve()}:/test",
             "-v", f"{results.resolve()}:/results",
             "-e", f"TASK27_ADMIN_PASSWORD={password}",
             "-e", f"PLAYWRIGHT_BASE_URL={'https://localhost:443' if production else 'http://localhost:8080'}",
             image, "sh", "-c", "; ".join(script)], env=env, timeout=600)


def systemd_filter_smoke(env: dict[str, str], run: Path) -> None:
    """Run the exact inline Lua callback from the generated template on a synthetic journal record."""
    source = (ROOT / "frontend/src/features/agents/fluentBitConfig.ts").read_text(encoding="utf-8")
    code_line = next(
        (line.strip().rstrip(",") for line in source.splitlines()
         if 'Code function diploma_systemd_envelope(' in line),
        None,
    )
    if code_line is None:
        raise RuntimeError("Generated systemd inline Code callback was not found")
    code = ast.literal_eval(code_line)[len("    Code "):]
    config = "\n".join([
        "[SERVICE]", "    Flush 1", "    Log_Level info", "",
        "[INPUT]", "    Name dummy", "    Tag diploma.agent.systemd-smoke",
        '    Dummy {"MESSAGE":"synthetic journal message","_SYSTEMD_UNIT":"sample.service","PRIORITY":"6","_PID":"42","SYSLOG_IDENTIFIER":"sample"}',
        "    Start_time_sec 946684800",  # 2000-01-01: deliberately older than processing time
        "    Samples 1", "",
        "[FILTER]", "    Name lua", "    Match diploma.agent.systemd-smoke",
        "    Call diploma_systemd_envelope", f"    Code {code}", "",
        "[OUTPUT]", "    Name stdout", "    Match diploma.agent.systemd-smoke", "    Format json_lines", "",
        "    Json_Date_Key timestamp", "    Json_Date_Format iso8601", "",
    ])
    config_path = run / "systemd-filter-smoke.conf"
    config_path.write_text(config, encoding="utf-8")
    container = env["COMPOSE_PROJECT_NAME"] + "_systemd_smoke"
    command([
        "docker", "run", "-d", "--name", container, "--label",
        f"task21.project={env['COMPOSE_PROJECT_NAME']}", "--memory", "128m", "--cpus", "1",
        "-v", f"{config_path.resolve()}:/test/fluent-bit.conf:ro",
        "fluent/fluent-bit:4.2.3", "-c", "/test/fluent-bit.conf",
    ], env=env)
    result = ""
    try:
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            result = command(["docker", "logs", container], env=env, capture=True)
            if '"log"' in result:
                break
            time.sleep(0.5)
    finally:
        command(["docker", "rm", "-f", container], env=env)
    records = []
    for line in result.splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and "log" in record:
            records.append(record)
    if len(records) != 1 or not isinstance(records[0].get("log"), str):
        raise RuntimeError("Fluent Bit systemd callback did not emit one string log record")
    if set(records[0]) != {"timestamp", "log"} or not isinstance(records[0]["timestamp"], str):
        raise RuntimeError("Fluent Bit systemd callback did not emit the {timestamp, log} envelope")
    envelope_time = datetime.fromisoformat(records[0]["timestamp"].replace("Z", "+00:00"))
    now = datetime.now(UTC)
    if not now - timedelta(seconds=30) <= envelope_time <= now + timedelta(seconds=5):
        raise RuntimeError("Fluent Bit systemd envelope timestamp is not processing time")
    log = records[0]["log"]
    for value in ("synthetic journal message", "sample.service", "priority=6", "pid=42", "identifier=sample", "journal_time=2000-01-01T00:00:00Z"):
        if value not in log:
            raise RuntimeError(f"Fluent Bit systemd callback omitted expected field: {value}")
    print("Fluent Bit 4.2.3 systemd inline Lua smoke passed on synthetic journald input", flush=True)


def end_to_end(compose: list[str], project: str, env: dict[str, str], run: Path) -> None:
    """Use the real UI, Fluent Bit, Kafka and PostgreSQL on the same isolated Compose network."""
    password = secrets.token_urlsafe(24)
    topic = "task21-" + uuid4().hex[:12]
    profile_env = {**env, "TASK21_ADMIN_PASSWORD": password, "TASK21_TOPIC": topic,
                   "TASK21_RUN_DIR": str(run), "TASK21_PHASE": "setup",
                   "PLAYWRIGHT_BASE_URL": f"http://127.0.0.1:{env['PROXY_BIND'].split(':')[1]}",
                   "DATABASE_URL": env["TEST_DATABASE_URL"], "ENVIRONMENT": "development"}
    if not profile_env.get("PLAYWRIGHT_CHROME_PATH"):
        for candidate in (r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                          r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"):
            if Path(candidate).is_file():
                profile_env["PLAYWRIGHT_CHROME_PATH"] = candidate
                break
    create = (
        "import os,app.cli as cli; "
        "cli.getpass.getpass=lambda prompt: os.environ['TASK21_ADMIN_PASSWORD']; "
        "raise SystemExit(cli.create_admin('task21admin','Task 21 admin'))"
    )
    command([sys.executable, "-c", create], env=profile_env, cwd=ROOT / "backend")
    systemd_filter_smoke(env, run)

    from sqlalchemy import create_engine
    from sqlalchemy import text as sql_text

    home_engine = create_engine(env["TEST_DATABASE_URL"])
    try:
        with home_engine.connect() as connection:
            guest_permissions = connection.execute(sql_text(
                "SELECT permissions FROM app.roles "
                "WHERE id = '00000000-0000-4000-8000-000000000002'"
            )).scalar_one()
            if "users.read" in guest_permissions or not ("events.read" in guest_permissions or "settings.read" in guest_permissions):
                raise RuntimeError("The migrated Guest role does not preserve read-only permissions without users.read")
    finally:
        home_engine.dispose()
    command([NPM, "run", "test:browser", "--", "administration.live.spec.ts", "--output", str(run / "administration-playwright")],
            env=profile_env, cwd=ROOT / "frontend")
    command([NPM, "run", "test:browser", "--", "agents.live.spec.ts", "--output", str(run / "playwright")],
            env=profile_env, cwd=ROOT / "frontend")
    home_engine = create_engine(env["TEST_DATABASE_URL"])
    try:
        with home_engine.begin() as connection:
            guest_permissions = connection.execute(sql_text(
                "UPDATE app.roles SET permissions = '[]'::jsonb WHERE id = "
                "'00000000-0000-4000-8000-000000000002'"
            ))
            if guest_permissions.rowcount != 1:
                raise RuntimeError("The isolated Guest role was not prepared for the home smoke")
            external_id = connection.execute(sql_text(
                "INSERT INTO app.external_connections "
                "(name, base_url, username, encrypted_password) "
                "VALUES (:name, 'https://indexer.example.test:9200', 'reader', 'synthetic-unreadable') "
                "RETURNING id"
            ), {"name": f"Home synthetic indexer {project}"}).scalar_one()
            connection.execute(sql_text(
                "INSERT INTO app.sources (name, source_type, external_connection_id, "
                "target_type, index_name, is_enabled) "
                "VALUES (:name, 'external', :external_id, 'index', 'home-events', false)"
            ), {"name": f"Home synthetic ECS {project}", "external_id": external_id})
    finally:
        home_engine.dispose()
    command([NPM, "run", "test:browser", "--", "home.live.spec.ts", "--output", str(run / "home-playwright")],
            env=profile_env, cwd=ROOT / "frontend")
    agent = json.loads((run / "agent.json").read_text(encoding="utf-8"))
    if agent["topic"] != topic:
        raise RuntimeError("UI agent topic differs from the isolated test topic")
    rendered = (run / "fluent-bit.conf").read_text(encoding="utf-8")
    expected = f"Brokers {env['KAFKA_PUBLIC_HOST']}:{env['KAFKA_HOST_PORT']}"
    if expected not in rendered:
        raise RuntimeError("UI configuration does not use the advertised external listener")
    (run / "input.log").write_text("task21 smoke event\n", encoding="utf-8")
    (run / "input-2.log").write_text("", encoding="utf-8")
    state_dir = run / "fluent-bit-state"
    state_dir.mkdir()
    kafka_container = command([*compose, "ps", "-q", "kafka"], env=env, capture=True).strip()
    if not kafka_container:
        raise RuntimeError("Kafka container is unavailable for the external-listener agent check")
    image = "fluent/fluent-bit:4.2.3"
    common = ["docker", "run", "--rm", "--memory", "256m", "--cpus", "1",
              "--network", f"container:{kafka_container}",
              "-v", f"{(run / 'fluent-bit.conf').resolve()}:/test/fluent-bit.conf:ro",
              "-v", f"{(run / 'input.log').resolve()}:/REPLACE/WITH/SERVICE_1.log:ro",
              "-v", f"{(run / 'input-2.log').resolve()}:/REPLACE/WITH/SERVICE_2.log:ro",
              "-v", f"{state_dir.resolve()}:/var/lib/fluent-bit",
              image, "-c", "/test/fluent-bit.conf"]
    command([*common, "-D"], env=env)
    container = project + "_fluentbit"
    try:
        command(["docker", "run", "-d", "--name", container, "--label", f"task21.project={project}",
                 "--memory", "256m", "--cpus", "1", "--network", f"container:{kafka_container}",
                 "-v", f"{(run / 'fluent-bit.conf').resolve()}:/test/fluent-bit.conf:ro",
                 "-v", f"{(run / 'input.log').resolve()}:/REPLACE/WITH/SERVICE_1.log:ro",
                 "-v", f"{(run / 'input-2.log').resolve()}:/REPLACE/WITH/SERVICE_2.log:ro",
                 "-v", f"{state_dir.resolve()}:/var/lib/fluent-bit",
                 image, "-c", "/test/fluent-bit.conf"], env=env)
        options = {
            "bootstrap.servers": env["KAFKA_TEST_BOOTSTRAP_SERVERS"],
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanisms": "SCRAM-SHA-512",
            "sasl.username": "diploma-reader",
            "sasl.password": Path(env["KAFKA_TEST_READER_PASSWORD_FILE"]).read_text(encoding="utf-8").strip(),
            "group.id": "diploma-source-" + project + "-smoke", "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        consumer = Consumer(options)
        try:
            consumer.subscribe([topic])
            deadline = time.monotonic() + 40
            while time.monotonic() < deadline:
                message = consumer.poll(2)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(f"Kafka consumer failed: {message.error().code()}")
                payload = json.loads(message.value())
                if payload.get("log", "").strip() == "task21 smoke event" and payload.get("timestamp"):
                    break
            else:
                diagnostic = subprocess.run(
                    ["docker", "logs", container], cwd=ROOT, env=env,
                    check=False, text=True, capture_output=True,
                )
                agent_password = next(
                    (line.strip().partition(" ")[2] for line in rendered.splitlines()
                     if line.strip().startswith("rdkafka.sasl.password ")),
                    "",
                )
                agent_logs = (diagnostic.stdout + diagnostic.stderr).replace(
                    agent_password, "[REDACTED]",
                ) if agent_password else diagnostic.stdout + diagnostic.stderr
                print("Fluent Bit container log (secrets redacted):\n" + agent_logs[-12000:], file=sys.stderr)
                raise RuntimeError("Fluent Bit record did not reach the assigned Kafka topic")
        finally:
            consumer.close()
    finally:
        subprocess.run(["docker", "rm", "-f", container], cwd=ROOT, env=env,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    print("Fluent Bit produced {timestamp, log} to the assigned topic", flush=True)
    fields = {}
    for line in rendered.splitlines():
        if line.strip().startswith("rdkafka.sasl."):
            key, _, value = line.strip().partition(" ")
            fields[key] = value.strip()
    agent_options = {
        "bootstrap.servers": env["KAFKA_TEST_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanisms": "SCRAM-SHA-512",
        "sasl.username": fields["rdkafka.sasl.username"],
        "sasl.password": fields["rdkafka.sasl.password"],
    }
    foreign = "task21-foreign-" + uuid4().hex[:12]
    admin = AdminClient({**agent_options, "sasl.username": "diploma-kafka-admin",
                         "sasl.password": Path(env["KAFKA_TEST_ADMIN_PASSWORD_FILE"]).read_text(encoding="utf-8").strip()})
    admin.create_topics([NewTopic(foreign, num_partitions=1, replication_factor=1)])[foreign].result(15)
    delivery = []
    producer = Producer({**agent_options, "message.timeout.ms": 7000})
    producer.produce(foreign, b"must-be-denied", callback=lambda error, _: delivery.append(error))
    producer.flush(10)
    if len(delivery) != 1 or delivery[0] is None or delivery[0].code() != KafkaError.TOPIC_AUTHORIZATION_FAILED:
        raise RuntimeError("Agent was not denied write to an unassigned topic")
    unknown = AdminClient({**agent_options, "sasl.username": "task21-unknown",
                           "sasl.password": "not-a-real-password"})
    try:
        unknown.list_topics(timeout=7)
    except KafkaException:
        pass
    else:
        raise RuntimeError("Unknown agent authenticated to Kafka")
    del unknown, producer, admin
    print("Kafka denied an unknown agent and cross-topic write", flush=True)

    profile_env["TASK21_PHASE"] = "configure"
    command([NPM, "run", "test:browser", "--", "agents.live.spec.ts", "--output", str(run / "configure-playwright")],
            env=profile_env, cwd=ROOT / "frontend")
    agent_artifact = json.loads((run / "agent.json").read_text(encoding="utf-8"))
    source = agent["sourceId"]
    normalizer_id = agent_artifact["normalizerId"]
    from sqlalchemy import create_engine
    from sqlalchemy import text as sql_text
    engine = create_engine(env["TEST_DATABASE_URL"])
    try:
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            with engine.connect() as connection:
                record = connection.execute(sql_text(
                    "SELECT raw, ecs_data->>'message' AS message, kafka_topic, "
                    "kafka_partition, kafka_offset, source_id, connection_id, "
                    "normalizer_id, processed_record_id FROM logs.parsed_logs "
                    "WHERE source_id=:id ORDER BY backend_processed_at DESC LIMIT 1"
                ), {"id": source}).mappings().one_or_none()
            if record and record["message"] == "task21 smoke event":
                break
            time.sleep(1)
        else:
            raise RuntimeError("The source did not persist the normalized event in PostgreSQL")
        if (record["raw"] != "task21 smoke event" or record["kafka_topic"] != topic
                or record["kafka_partition"] < 0 or record["kafka_offset"] < 0
                or str(record["source_id"]) != source or not record["connection_id"]
                or str(record["normalizer_id"]) != normalizer_id
                or not record["processed_record_id"]):
            raise RuntimeError("The stored ECS event lost its raw log or Kafka provenance")
        with engine.connect() as connection:
            saved_count = connection.execute(sql_text(
                "SELECT count(*) FROM logs.parsed_logs WHERE source_id=:id AND raw=:raw"
            ), {"id": source, "raw": "task21 smoke event"}).scalar_one()
        if saved_count != 1:
            raise RuntimeError("The synthetic Kafka record was duplicated before restart")
        diagnostic_delivery = []
        diagnostic_producer = Producer({**agent_options, "message.timeout.ms": 10000})
        diagnostic_producer.produce(topic, b"task23-invalid-envelope", callback=lambda error, _: diagnostic_delivery.append(error))
        diagnostic_producer.flush(15)
        if diagnostic_delivery != [None]:
            raise RuntimeError("The synthetic malformed Kafka record was not delivered")
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            with engine.connect() as connection:
                error_count = connection.execute(sql_text(
                    "SELECT count(*) FROM logs.processing_errors WHERE source_identity=:id AND stage='decode'"
                ), {"id": source}).scalar_one()
            if error_count:
                break
            time.sleep(1)
        else:
            raise RuntimeError("The malformed Kafka record did not produce a saved decode error")
        with engine.begin() as connection:
            inserted = connection.execute(sql_text(
                "INSERT INTO logs.kafka_operational_events "
                "(kind, reason_code, source_id, source_identity, source_name, connection_id, "
                "connection_identity, connection_name, cluster_identity, topic_name, "
                "old_topic_identity, kafka_partition, offset_start, offset_end) "
                "SELECT 'retention_gap', 'retention_gap_detected', s.id, s.id, s.name, "
                "s.connection_id, s.connection_id, c.name, a.cluster_identity, s.topic_name, "
                "s.kafka_topic_identity, 0, 1000000, 1000002 "
                "FROM app.sources s JOIN app.kafka_connections c ON c.id=s.connection_id "
                "JOIN app.fluentbit_agents a ON a.id=:agent_id WHERE s.id=:source_id"
            ), {"agent_id": agent["agentId"], "source_id": source})
            if inserted.rowcount != 1:
                raise RuntimeError("The synthetic Kafka history event was not saved")
    finally:
        engine.dispose()
    command([*compose, "restart", "api"], env=env)
    command([*compose, "up", "-d", "--wait", "api"], env=env)
    time.sleep(3)
    engine = create_engine(env["TEST_DATABASE_URL"])
    try:
        with engine.connect() as connection:
            after_restart = connection.execute(sql_text(
                "SELECT count(*) FROM logs.parsed_logs WHERE source_id=:id AND raw=:raw"
            ), {"id": source, "raw": "task21 smoke event"}).scalar_one()
        if after_restart != saved_count:
            raise RuntimeError("Backend restart duplicated the saved Kafka record")
    finally:
        engine.dispose()
    print("Backend restart recovered the source without duplicating the stored event", flush=True)
    profile_env["TASK21_PHASE"] = "verify"
    profile_env["TASK23_DIAGNOSTIC_FIXTURES"] = "1"
    command([NPM, "run", "test:browser", "--", "agents.live.spec.ts", "--output", str(run / "playwright")],
            env=profile_env, cwd=ROOT / "frontend")
    print("UI Events page shows the normalized PostgreSQL record", flush=True)


def tls_agent_smoke(compose: list[str], project: str, env: dict[str, str], run: Path) -> None:
    """Exercise the frontend renderer, claimed credentials and CA against the TLS listener."""
    password = secrets.token_urlsafe(24)
    topic = "task21-tls-" + uuid4().hex[:12]
    profile_env = {**env, "TASK21_ADMIN_PASSWORD": password, "TASK21_TOPIC": topic,
                   "TASK21_RUN_DIR": str(run),
                   "PLAYWRIGHT_BASE_URL": f"https://localhost:{env['HTTPS_BIND'].split(':')[1]}",
                   "TASK21_SECURE_COOKIE": "true",
                   "DATABASE_URL": env["TEST_DATABASE_URL"], "ENVIRONMENT": "test"}
    if not profile_env.get("PLAYWRIGHT_CHROME_PATH"):
        for candidate in (r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                          r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"):
            if Path(candidate).is_file():
                profile_env["PLAYWRIGHT_CHROME_PATH"] = candidate
                break
    profile_env.pop("DATABASE_URL_FILE", None)
    create = (
        "import os,app.cli as cli; "
        "cli.getpass.getpass=lambda prompt: os.environ['TASK21_ADMIN_PASSWORD']; "
        "raise SystemExit(cli.create_admin('task21admin','Task 21 admin'))"
    )
    command([*compose, "exec", "-T", "-e", f"TASK21_ADMIN_PASSWORD={password}",
             "api", "python", "-c", create], env=profile_env)
    proxy_container = command([*compose, "ps", "-q", "proxy"], env=profile_env, capture=True).strip()
    if not proxy_container:
        raise RuntimeError("Production proxy is unavailable for the browser smoke")
    browser_result = run / "production-session-playwright"
    browser_result.mkdir()
    source = ROOT / "frontend"
    browser_context = run / "browser-context"
    browser_context.mkdir()
    for name in ("package.json", "package-lock.json", "playwright.config.ts"):
        (browser_context / name).write_bytes((source / name).read_bytes())
    browser_dir = browser_context / "browser"
    browser_dir.mkdir()
    (browser_dir / "proxy.session.spec.ts").write_bytes(
        (source / "browser" / "proxy.session.spec.ts").read_bytes()
    )
    browser_image = "mcr.microsoft.com/playwright:v1.63.0-noble"
    browser_container = f"{project}-browser-smoke"
    browser_command = [
        "docker", "run", "--rm", "--name", browser_container,
        "--user", "0:0", "--memory", "2g", "--cpus", "2",
        "--network", f"container:{proxy_container}", "--label", f"task26.project={project}",
        "-v", f"{Path(env['KAFKA_CA_FILE']).resolve()}:/tmp/test-ca.pem:ro",
        "-v", f"{browser_result.resolve()}:/results",
        "-v", f"{browser_context.resolve()}:/test",
        "-w", "/test",
        "-e", f"TASK21_ADMIN_PASSWORD={password}", "-e", "TASK21_SECURE_COOKIE=true",
        "-e", "PLAYWRIGHT_BASE_URL=https://localhost",
        browser_image, "sh", "-c",
        (
            "set -e; apt-get update >/dev/null; "
            "apt-get install -y --no-install-recommends libnss3-tools >/dev/null; "
            "npm ci --ignore-scripts --no-audit --no-fund; "
            "mkdir -p /usr/local/share/ca-certificates /root/.pki/nssdb; "
            "cp /tmp/test-ca.pem /usr/local/share/ca-certificates/task26-smoke.crt; "
            "update-ca-certificates >/dev/null; "
            "certutil -d sql:/root/.pki/nssdb -N --empty-password; "
            "certutil -d sql:/root/.pki/nssdb -A -t 'C,,' -n task26-smoke -i /tmp/test-ca.pem; "
            "npx playwright test browser/proxy.session.spec.ts --output /results"
        ),
    ]
    try:
        command(browser_command, env=profile_env, timeout=600)
    finally:
        subprocess.run(["docker", "rm", "-f", browser_container], env=profile_env,
                       cwd=ROOT, check=False, capture_output=True, timeout=30)
    command([NPM, "run", "test:browser", "--", "agents.tls.spec.ts", "--output", str(run / "playwright")],
            env=profile_env, cwd=ROOT / "frontend")
    rendered = (run / "fluent-bit.conf").read_text(encoding="utf-8")
    expected = f"Brokers {env['KAFKA_PUBLIC_HOST']}:{env['KAFKA_HOST_PORT']}"
    if expected not in rendered or "rdkafka.security.protocol SASL_SSL" not in rendered:
        raise RuntimeError("Rendered TLS config does not use the external TLS listener")
    (run / "input.log").write_text("task21 TLS smoke event\n", encoding="utf-8")
    state_dir = run / "fluent-bit-state"
    state_dir.mkdir()
    kafka_container = command([*compose, "ps", "-q", "kafka"], env=env, capture=True).strip()
    if not kafka_container:
        raise RuntimeError("Kafka container is unavailable for the TLS agent check")
    image = "fluent/fluent-bit:4.2.3"
    volumes = ["-v", f"{(run / 'fluent-bit.conf').resolve()}:/test/fluent-bit.conf:ro",
               "-v", f"{(run / 'input.log').resolve()}:/REPLACE/WITH/SERVICE_1.log:ro",
               "-v", f"{state_dir.resolve()}:/var/lib/fluent-bit",
               "-v", f"{(run / 'diploma-kafka-ca.pem').resolve()}:/etc/fluent-bit/diploma-kafka-ca.pem:ro"]
    base = ["docker", "run", "--rm", "--memory", "256m", "--cpus", "1",
            "--network", f"container:{kafka_container}", *volumes,
            image, "-c", "/test/fluent-bit.conf"]
    command([*base, "-D"], env=env)
    container = project + "_tls_fluentbit"
    try:
        command(["docker", "run", "-d", "--name", container, "--label", f"task21.project={project}",
                 "--memory", "256m", "--cpus", "1", "--network", f"container:{kafka_container}",
                 *volumes, image, "-c", "/test/fluent-bit.conf"], env=env)
        options = {
            "bootstrap.servers": env["KAFKA_TEST_BOOTSTRAP_SERVERS"],
            "security.protocol": "SASL_SSL", "ssl.ca.location": env["KAFKA_TEST_CA_FILE"],
            "sasl.mechanisms": "SCRAM-SHA-512", "sasl.username": "diploma-reader",
            "sasl.password": Path(env["KAFKA_TEST_READER_PASSWORD_FILE"]).read_text(encoding="utf-8").strip(),
            "group.id": "diploma-source-" + project + "-tls", "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        consumer = Consumer(options)
        try:
            consumer.subscribe([topic])
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                message = consumer.poll(2)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(f"TLS Kafka consumer failed: {message.error().code()}")
                if json.loads(message.value()).get("log") == "task21 TLS smoke event":
                    print("Fluent Bit TLS config, CA and hostname produced to external Kafka listener", flush=True)
                    break
            else:
                raise RuntimeError("Fluent Bit did not deliver the TLS test event")
        finally:
            consumer.close()
    finally:
        command(["docker", "rm", "-f", container], env=env)


def external_end_to_end(compose: list[str], project: str, env: dict[str, str], run: Path) -> None:
    """Check Wazuh-shaped ECS data through the real HTTPS API and indexer."""
    scripts = ROOT / "backend" / "tests" / "opensearch-wazuh5"
    for name in ("bootstrap_readonly.py", "seed_synthetic.py", "smoke_indexer.py"):
        command([sys.executable, str(scripts / name)], env=env, timeout=240)
    reader_password = (Path(env["WAZUH_TEST_RUNTIME"]) / "reader-password.txt").read_text(encoding="utf-8").strip()
    test_env = {**env, "WAZUH_SMOKE_USERNAME": "diploma_task12_reader",
                "WAZUH_SMOKE_PASSWORD": reader_password}
    for name in ("DATABASE_URL_FILE", "OIDC_STATE_SECRET_FILE", "OIDC_CLIENT_SECRET_FILE",
                 "EXTERNAL_SECRET_KEY_FILE"):
        test_env.pop(name, None)
    test_env["ENVIRONMENT"] = "test"
    command([sys.executable, "scripts/run_tests.py", "--temporary", "--pytest",
             "tests/test_auth_foundation.py::test_external_live_wazuh_smoke"],
            env=test_env, cwd=ROOT / "backend", timeout=600)

    kafka_options = {
        "bootstrap.servers": env["KAFKA_TEST_BOOTSTRAP_SERVERS"],
        "security.protocol": env["KAFKA_TEST_SECURITY_PROTOCOL"],
        "sasl.mechanisms": "SCRAM-SHA-512", "sasl.username": "diploma-kafka-admin",
        "sasl.password": Path(env["KAFKA_TEST_ADMIN_PASSWORD_FILE"]).read_text(encoding="utf-8").strip(),
        "ssl.ca.location": env["KAFKA_TEST_CA_FILE"],
    }
    topics_before = set(AdminClient(kafka_options).list_topics(timeout=15).topics)

    password = secrets.token_urlsafe(24)
    create = (
        "import os,app.cli as cli; "
        "cli.getpass.getpass=lambda prompt: os.environ['TASK29_ADMIN_PASSWORD']; "
        "raise SystemExit(cli.create_admin('task29admin','Task 29 admin'))"
    )
    command([*compose, "exec", "-T", "-e", f"TASK29_ADMIN_PASSWORD={password}",
             "api", "python", "-c", create], env=env)
    base = f"https://localhost:{env['HTTPS_BIND'].split(':')[1]}/api/v1"
    context = ssl.create_default_context(cafile=env["KAFKA_CA_FILE"])
    opener = urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=context),
        urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),
    )

    def request(path: str, method: str = "GET", body: dict | bytes | None = None,
                csrf: str | None = None, content_type: str = "application/json"):
        payload = json.dumps(body).encode() if isinstance(body, dict) else body
        headers = {"Content-Type": content_type}
        if csrf:
            headers["X-CSRF-Token"] = csrf
        with opener.open(urllib.request.Request(base + path, data=payload,
                                                headers=headers, method=method),
                         timeout=30) as response:
            content = response.read(5_242_881)
            if len(content) > 5_242_880:
                raise RuntimeError("External acceptance response exceeded the size limit")
            return json.loads(content) if content else None

    login = request("/auth/local/login", "POST", {"username": "task29admin", "password": password})
    csrf = login["csrf_token"]
    from sqlalchemy import create_engine
    from sqlalchemy import text as sql_text
    engine = create_engine(env["TEST_DATABASE_URL"])
    try:
        with engine.connect() as connection:
            before = connection.execute(sql_text("SELECT count(*) FROM logs.parsed_logs")).scalar_one()
        created = request("/external-connections", "POST", {
            "name": "Task 29 Wazuh indexer", "base_url": "https://indexer:9200",
            "username": "diploma_task12_reader", "password": reader_password,
        }, csrf)
        connection_id = created["id"]
        request(f"/external-connections/{connection_id}/ca", "PUT",
                Path(env["WAZUH_SMOKE_CA"]).read_bytes(), csrf,
                "application/x-pem-file")
        checked = request(f"/external-connections/{connection_id}/test", "POST", csrf=csrf)
        if checked["status"] != "ok":
            raise RuntimeError("API did not confirm the external indexer")
        streams = request(f"/external-connections/{connection_id}/data-streams?limit=100")
        if "wazuh-findings-v5-security" not in streams["items"]:
            raise RuntimeError("API did not discover the Wazuh-shaped data stream")
        source = request("/external-sources", "POST", {
            "name": "Task 29 Wazuh findings", "external_connection_id": connection_id,
            "target_type": "data_stream", "data_stream_name": "wazuh-findings-v5-security",
        }, csrf)
        enabled = request(f"/external-sources/{source['id']}/enable", "POST", csrf=csrf)
        if not enabled["is_enabled"]:
            raise RuntimeError("External source was not enabled")
        search = request(f"/events/sources/{source['id']}/search", "POST", {
            "timestamp_from": "2026-06-15T12:00:00Z",
            "timestamp_to": "2026-06-15T12:04:00Z", "sort": "asc", "limit": 10,
            "filters": [{"field": "wazuh.agent.name", "operator": "eq", "value": "agent-0"}],
        }, csrf)
        if not search["items"]:
            raise RuntimeError("Production API did not return filtered Wazuh-shaped documents")
        with engine.connect() as connection:
            after = connection.execute(sql_text("SELECT count(*) FROM logs.parsed_logs")).scalar_one()
        if after != before:
            raise RuntimeError("External documents were copied into parsed_logs")
    finally:
        engine.dispose()
    # A small local fixture in the same production UI verifies source switching.
    # The full Fluent Bit -> Kafka -> PostgreSQL route is exercised by end-to-end.
    engine = create_engine(env["TEST_DATABASE_URL"])
    try:
        with engine.begin() as connection:
            local_connection = connection.execute(sql_text(
                "INSERT INTO app.kafka_connections (name, bootstrap_servers) "
                "VALUES ('Task 29 local fixture', ARRAY['kafka:9092']) RETURNING id"
            )).scalar_one()
            local_normalizer = connection.execute(sql_text(
                "INSERT INTO app.normalizers (name, rule) "
                "VALUES ('Task 29 fixture normalizer', '{}'::jsonb) RETURNING id"
            )).scalar_one()
            local_source = connection.execute(sql_text(
                "INSERT INTO app.sources (name, connection_id, normalizer_id, topic_name) "
                "VALUES ('Task 29 local event', :connection_id, :normalizer_id, 'task29-local-fixture') "
                "RETURNING id"
            ), {"connection_id": local_connection, "normalizer_id": local_normalizer}).scalar_one()
            timestamp = datetime(2026, 6, 15, 12, 0, tzinfo=UTC)
            connection.execute(sql_text(
                "INSERT INTO logs.parsed_logs "
                "(source_id, connection_id, normalizer_id, normalizer_version, normalizer_name, "
                "source_name, connection_name, kafka_topic, kafka_partition, kafka_offset, "
                "deduplication_key, fluent_bit_collected_at, backend_received_at, "
                "backend_processed_at, raw, ecs_data) VALUES "
                "(:source_id, :connection_id, :normalizer_id, 1, "
                "'Task 29 fixture normalizer', 'Task 29 local event', 'Task 29 local fixture', "
                "'task29-local-fixture', 0, 0, :dedup, :timestamp, :timestamp, :timestamp, "
                "'task29 local smoke event', CAST(:ecs AS jsonb))"
            ), {"source_id": local_source, "connection_id": local_connection,
                "normalizer_id": local_normalizer, "dedup": str(uuid4()),
                "timestamp": timestamp,
                "ecs": json.dumps({"@timestamp": timestamp.isoformat(),
                                   "message": "task29 local smoke event",
                                   "event": {"kind": "event"}})})
    finally:
        engine.dispose()
    local_search = request(f"/events/sources/{local_source}/search", "POST", {
        "timestamp_from": "2026-06-15T11:59:00Z",
        "timestamp_to": "2026-06-15T12:05:00Z",
        "sort": "desc", "limit": 10,
        "filters": [{"field": "message", "operator": "contains", "value": "local smoke"}],
    }, csrf)
    if not local_search["items"]:
        raise RuntimeError("Local fixture was not searchable through production API")
    proxy_container = command([*compose, "ps", "-q", "proxy"], env=env, capture=True).strip()
    if not proxy_container:
        raise RuntimeError("Production proxy is unavailable for the external UI smoke")
    browser_context = run / "external-browser"
    browser_context.mkdir()
    for name in ("package.json", "package-lock.json", "playwright.config.ts"):
        (browser_context / name).write_bytes((ROOT / "frontend" / name).read_bytes())
    browser_dir = browser_context / "browser"
    browser_dir.mkdir()
    (browser_dir / "external.live.spec.ts").write_bytes(
        (ROOT / "frontend" / "browser" / "external.live.spec.ts").read_bytes())
    results = run / "external-browser-results"
    results.mkdir()
    browser_container = f"{project}-external-browser"
    browser_command = [
        "docker", "run", "--rm", "--name", browser_container,
        "--user", "0:0", "--memory", "2g", "--cpus", "2",
        "--network", f"container:{proxy_container}",
        "--label", f"task29.project={project}",
        "-v", f"{Path(env['KAFKA_CA_FILE']).resolve()}:/tmp/test-ca.pem:ro",
        "-v", f"{Path(env['WAZUH_SMOKE_CA']).resolve()}:/tmp/indexer-ca.pem:ro",
        "-v", f"{results.resolve()}:/results",
        "-v", f"{browser_context.resolve()}:/test",
        "-w", "/test", "-e", f"TASK29_ADMIN_PASSWORD={password}",
        "-e", f"TASK29_READER_PASSWORD={reader_password}",
        "-e", "PLAYWRIGHT_BASE_URL=https://localhost",
        "mcr.microsoft.com/playwright:v1.63.0-noble", "sh", "-c",
        (
            "set -e; apt-get update >/dev/null; "
            "apt-get install -y --no-install-recommends libnss3-tools >/dev/null; "
            "npm ci --ignore-scripts --no-audit --no-fund; "
            "mkdir -p /usr/local/share/ca-certificates /root/.pki/nssdb; "
            "cp /tmp/test-ca.pem /usr/local/share/ca-certificates/task29-smoke.crt; "
            "update-ca-certificates >/dev/null; "
            "certutil -d sql:/root/.pki/nssdb -N --empty-password; "
            "certutil -d sql:/root/.pki/nssdb -A -t 'C,,' -n task29-smoke -i /tmp/test-ca.pem; "
            "npx playwright test browser/external.live.spec.ts --output /results"
        ),
    ]
    try:
        command(browser_command, env=env, timeout=600)
    finally:
        subprocess.run(["docker", "rm", "-f", browser_container], env=env,
                       cwd=ROOT, check=False, capture_output=True, timeout=30)
    topics_after = set(AdminClient(kafka_options).list_topics(timeout=15).topics)
    if topics_after != topics_before:
        raise RuntimeError("External path changed Kafka topics")
    print("Production HTTPS API read filtered Wazuh-shaped documents without PostgreSQL copies", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=["frontend", "backend-db", "kafka-integration", "end-to-end", "smoke-development", "smoke-production", "external-end-to-end"])
    parser.add_argument("--pytest", nargs="*", help="Specific backend test nodes for a focused retry")
    parser.add_argument("--check-only", action="store_true", help="Resolve and validate Compose without starting containers")
    args = parser.parse_args()
    run = ROOT / ".test_runs" / "runs" / f"task21-{uuid4().hex}"
    run.mkdir(parents=True, exist_ok=False)
    env = {**os.environ, "TEMP": str(run), "TMP": str(run), "TMPDIR": str(run),
           "npm_config_cache": str(run / "npm-cache"), "PYTHONDONTWRITEBYTECODE": "1",
           "VITE_CACHE_DIR": str(run / "vite-cache"),
           "PYTEST_ADDOPTS": pytest_addopts(run)}
    compose = None
    project = None
    builder = None
    before = None
    successful = False
    try:
        if args.profile == "frontend":
            artifacts_before = frontend_artifact_snapshot()
            build_info = run / "frontend.tsbuildinfo"
            build_output = run / "frontend-dist"
            for check in ("typecheck", "lint", "format:check", "test", "build"):
                if check == "typecheck":
                    command(
                        [NPM, "exec", "--", "tsc", "--project", "tsconfig.json",
                         "--noEmit", "--incremental", "--tsBuildInfoFile", str(build_info)],
                        env=env,
                        cwd=ROOT / "frontend",
                    )
                elif check == "build":
                    command(
                        [NPM, "exec", "--", "tsc", "--project", "tsconfig.json",
                         "--noEmit", "--incremental", "--tsBuildInfoFile", str(build_info)],
                        env=env,
                        cwd=ROOT / "frontend",
                    )
                    command(
                        [NPM, "exec", "--", "vite", "build", "--outDir", str(build_output),
                         "--emptyOutDir"],
                        env=env,
                        cwd=ROOT / "frontend",
                    )
                elif check == "test":
                    command([NPM, "run", "test", "--", "--maxWorkers=1"],
                            env=env, cwd=ROOT / "frontend")
                else:
                    command([NPM, "run", check], env=env, cwd=ROOT / "frontend")
            artifacts_after = frontend_artifact_snapshot()
            if artifacts_after != artifacts_before:
                paths = sorted(set(artifacts_before) | set(artifacts_after))
                changed = [
                    path for path in paths
                    if artifacts_before.get(path) != artifacts_after.get(path)
                ]
                raise RuntimeError(
                    f"Frontend profile changed artifacts outside its run directory: {changed[:10]}"
                )
            successful = True
            return
        before = inventory(env)
        (run / "docker-before.json").write_text(json.dumps(before, indent=2), encoding="utf-8")
        env, compose, project = prepare(run, args.profile)
        if args.profile == "external-end-to-end":
            compose += ["-f", str(wazuh_compose_overlay(run, env))]
        print(f"Profile={args.profile}; project={project}; run={run}", flush=True)
        print(f"Addresses: PostgreSQL={env['POSTGRES_BIND']}, Kafka={env['KAFKA_TEST_BOOTSTRAP_SERVERS']}, proxy={env.get('HTTPS_BIND', env['PROXY_BIND'])}", flush=True)
        print("Secret variables: POSTGRES_PASSWORD_FILE, KAFKA_ADMIN_PASSWORD_FILE, KAFKA_READER_PASSWORD_FILE, AGENT_CREDENTIALS_KEY_FILE", flush=True)
        check_config(compose, project, env, run)
        if args.check_only:
            print("Resolved Compose config passed isolated-resource checks", flush=True)
            successful = True
            return
        if args.profile in {"kafka-integration", "smoke-development", "smoke-production", "end-to-end", "external-end-to-end"}:
            builder = project + "_builder"
            command(["docker", "buildx", "create", "--name", builder, "--driver", "docker-container",
                     "--driver-opt", "network=host", "--bootstrap"], env=env)
            env["BUILDX_BUILDER"] = builder
            env["COMPOSE_DOCKER_CLI_BUILD"] = "1"
        if args.profile == "backend-db":
            command([*compose, "up", "-d", "--wait", "postgres"], env=env)
            command([sys.executable, "-m", "pytest", "-q", *(args.pytest or ["tests/test_auth_foundation.py", "tests/test_agent_contracts.py"])], env=env, cwd=ROOT / "backend")
            migration_env = {**env, "DATABASE_URL": env["TEST_DATABASE_URL"]}
            migration_env.pop("DATABASE_URL_FILE", None)
            command([sys.executable, "-m", "alembic", "upgrade", "head"], env=migration_env, cwd=ROOT / "backend")
            command([sys.executable, "-m", "alembic", "check"], env=migration_env, cwd=ROOT / "backend")
        elif args.profile == "kafka-integration":
            command([*compose, "up", "-d", "--build", "--wait", "postgres", "kafka", "kafka-acls"], env=env)
            verify_kafka_metadata(compose, env)
            if args.pytest:
                command([sys.executable, "-m", "pytest", "-q", *args.pytest], env=env, cwd=ROOT / "backend")
            else:
                command([sys.executable, "scripts/run_tests.py", "--temporary"], env=env, cwd=ROOT / "backend")
        elif args.profile in {"smoke-development", "smoke-production", "end-to-end", "external-end-to-end"}:
            production = args.profile in {"smoke-production", "external-end-to-end"}
            services = ["postgres", "kafka", "kafka-acls", "migrate", "api", "provisioner", "proxy"]
            if args.profile == "external-end-to-end":
                services.append("indexer")
            if args.profile in {"smoke-development", "smoke-production"}:
                services.append("oidc-idp")
            if not production:
                services.append("frontend")
            command([*compose, "up", "-d", "--build", "--wait", *services], env=env)
            if args.profile in {"smoke-development", "smoke-production"}:
                idp_container = command([*compose, "ps", "-q", "oidc-idp"], env=env, capture=True).strip()
                inspection = json.loads(command(["docker", "inspect", idp_container], env=env, capture=True))[0]
                bindings = inspection["HostConfig"]["PortBindings"]
                expected = ({f"{env['OIDC_STUB_PORT']}/tcp": [{"HostIp": "127.0.0.1",
                    "HostPort": env["OIDC_STUB_PUBLIC_PORT"]}]} if not production else {})
                if bindings != expected:
                    raise RuntimeError(f"Synthetic OIDC port binding differs from the profile: {bindings!r}")
                attached = inspection["NetworkSettings"]["Networks"]
                network_name = f"{project}_oidc-private"
                expected_network_names = ({network_name, f"{project}_oidc-browser"}
                                          if not production else {network_name})
                if set(attached) != expected_network_names:
                    raise RuntimeError(f"Synthetic OIDC provider escaped its private network: {sorted(attached)}")
                print(f"Synthetic IdP isolation passed: {network_name}, service oidc-idp:{env['OIDC_STUB_PORT']}", flush=True)
            verify_kafka_metadata(compose, env)
            verify_api_metadata(compose, env)
            if args.profile in {"end-to-end", "smoke-production"}:
                verify_agent_metadata(compose, project, env)
            smoke_http(env, production)
            if args.profile in {"smoke-development", "smoke-production"}:
                oidc_administration_smoke(compose, env, production)
            if args.profile == "smoke-development":
                password = secrets.token_urlsafe(24)
                browser_env = {**env, "TASK21_ADMIN_PASSWORD": password,
                               "TASK21_SECURE_COOKIE": "false",
                               "PLAYWRIGHT_BASE_URL": f"http://127.0.0.1:{env['PROXY_BIND'].split(':')[1]}",
                               "PLAYWRIGHT_USERNAME": "task21admin",
                               "PLAYWRIGHT_PASSWORD": password,
                               "PLAYWRIGHT_OIDC": "1"}
                if not browser_env.get("PLAYWRIGHT_CHROME_PATH"):
                    for candidate in (r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                                      r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"):
                        if Path(candidate).is_file():
                            browser_env["PLAYWRIGHT_CHROME_PATH"] = candidate
                            break
                create = (
                    "import os,app.cli as cli; "
                    "cli.getpass.getpass=lambda prompt: os.environ['TASK21_ADMIN_PASSWORD']; "
                    "raise SystemExit(cli.create_admin('task21admin','Task 21 admin'))"
                )
                command([*compose, "exec", "-T", "-e", f"TASK21_ADMIN_PASSWORD={password}",
                         "api", "python", "-c", create], env=browser_env)
                command([NPM, "run", "test:browser", "--", "proxy.session.spec.ts",
                         "--output", str(run / "development-session-playwright")],
                        env=browser_env, cwd=ROOT / "frontend")
                command([NPM, "run", "test:browser", "--", "auth.spec.ts",
                         "--grep", "mobile navigation, account layout and viewport bounds",
                         "--output", str(run / "development-navigation-playwright")],
                        env=browser_env, cwd=ROOT / "frontend")
                command([NPM, "run", "test:browser", "--", "administration.live.spec.ts",
                         "--output", str(run / "development-administration-playwright")],
                        env={**browser_env, "TASK21_ADMIN_PASSWORD": password},
                        cwd=ROOT / "frontend")
            if production:
                command([*compose, "exec", "-T", "proxy", "sh", "-c",
                         (
                             "test ! -e /usr/bin/node && test ! -e /app && "
                             "test -z \"$(find /usr/share/nginx/html -name '*.map' -print -quit)\""
                         )], env=env)
                command([*compose, "up", "-d", "--no-deps", "--force-recreate", "--wait", "proxy"], env=env)
                smoke_http(env, True)
            if args.profile == "end-to-end":
                end_to_end(compose, project, env, run)
            if args.profile == "external-end-to-end":
                external_end_to_end(compose, project, env, run)
            if args.profile == "smoke-production":
                tls_agent_smoke(compose, project, env, run)
        successful = True
    finally:
        cleanup_error = None
        if before is not None:
            try:
                if compose and project and not args.check_only:
                    if not successful and args.profile in {"backend-db", "kafka-integration", "smoke-development", "smoke-production", "end-to-end", "external-end-to-end"}:
                        logs = subprocess.run([*compose, "logs", "--no-color", "--tail=100", "postgres", "kafka", "api", "proxy"], cwd=ROOT, env=env, check=False, text=True, capture_output=True)
                        selected = [line for line in logs.stdout.splitlines() if any(
                            token in line for token in ("ERROR", "Exception", "Caused by", "SSL", "TLS", "certificate", "unhealthy")
                        )]
                        print("\n".join(selected[-25:]).replace(env["POSTGRES_PASSWORD"], "[REDACTED]"), file=sys.stderr)
                    command([*compose, "down", "--volumes", "--remove-orphans"], env=env)
                    for service in ("kafka", "kafka-acls", "migrate", "api", "provisioner", "frontend", "proxy", "browser", "oidc-browser", "oidc-idp"):
                        image = f"{project}-{service}"
                        matches = command(["docker", "image", "ls", "--filter", f"reference={image}", "--format", "{{.Repository}}"], env=env, capture=True).splitlines()
                        if image in matches:
                            command(["docker", "image", "rm", image], env=env)
                if builder:
                    command(["docker", "buildx", "rm", "--force", builder], env=env)
                    env.pop("BUILDX_BUILDER", None)
                remove_new_images(before, env, project or "", args.profile)
                after = inventory(env)
                (run / "docker-after.json").write_text(json.dumps(after, indent=2), encoding="utf-8")
                if project:
                    residual = command(["docker", "ps", "-a", "--filter", f"label=com.docker.compose.project={project}", "--format", "{{.ID}}"], env=env, capture=True).strip()
                    volumes = command(["docker", "volume", "ls", "--filter", f"label=com.docker.compose.project={project}", "--format", "{{.Name}}"], env=env, capture=True).strip()
                    networks = command(["docker", "network", "ls", "--filter", f"label=com.docker.compose.project={project}", "--format", "{{.Name}}"], env=env, capture=True).strip()
                    if residual or volumes or networks:
                        raise RuntimeError(f"Temporary Docker resources remain for {project}: containers={residual!r}, volumes={volumes!r}, networks={networks!r}")
                compare_inventory(before, after, project or "unassigned project")
            except Exception as exc:
                cleanup_error = exc
        run_root = (ROOT / ".test_runs" / "runs").resolve()
        target = run.resolve()
        if target == run_root or not target.is_relative_to(run_root):
            raise RuntimeError(f"Unexpected run cleanup path: {target}")
        if cleanup_error is not None:
            try:
                current = inventory(env)
                remaining = {kind: sorted(set(current[kind]) - set(before[kind]))
                             for kind in before if set(current[kind]) - set(before[kind])}
            except Exception as exc:
                remaining = {"inventory_error": str(exc)}
            raise RuntimeError(
                f"Docker cleanup failed for {project or 'unassigned project'}: "
                f"{cleanup_error}; remaining={remaining}; run directory={run}"
            ) from cleanup_error
        import shutil
        shutil.rmtree(target)


if __name__ == "__main__":
    main()
