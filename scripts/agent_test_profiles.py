"""Isolated TASK-21 verification using the project's Compose services.

Run from the repository root: python scripts/agent_test_profiles.py PROFILE.
Profiles: frontend, backend-db, kafka-integration, end-to-end, smoke-development,
smoke-production. Docker profiles use a unique Compose project and remove it in finally.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
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


def command(args: list[str], *, env: dict[str, str], cwd: Path = ROOT, capture=False):
    result = subprocess.run(args, cwd=cwd, env=env, check=False,
                            text=True, capture_output=capture)
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


def remove_new_images(before: dict, env: dict[str, str], project: str) -> None:
    prior = set(before["images"])
    prior_ids = {entry.split(" ", 1)[0] for entry in prior}
    allowed = (f"{project}-", "fluent/fluent-bit:4.2.3", "moby/buildkit:buildx-stable-1",
               "python:3.12-slim", "node:20.19.5-alpine", "nginx:1.30.5-alpine",
               "postgres:18", "apache/kafka:4.2.0")
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
    production = profile == "smoke-production"
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
    if production:
        env.update({
            "KAFKA_PUBLIC_HOST": "127.0.0.1",
            "PUBLIC_HOST": "localhost",
            "HTTP_BIND": f"127.0.0.1:{proxy_port}:80",
            "HTTPS_BIND": f"127.0.0.1:{https_port}:443",
            "KAFKA_TEST_BOOTSTRAP_SERVERS": f"127.0.0.1:{kafka_port}",
            "KAFKA_TEST_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_TEST_CA_FILE": str(files["KAFKA_CA_FILE"]),
        })
        env.pop("KAFKA_TEST_DEV_ALLOWED_BOOTSTRAP_SERVERS", None)
    limits = run / "compose-limits.yml"
    limits.write_text("services:\n" + "".join(
        f"  {service}:\n    mem_limit: {memory}\n    cpus: 2.0\n"
        for service, memory in {
            "postgres": "768m", "kafka": "2g", "kafka-acls": "512m",
            "migrate": "512m", "api": "1g", "provisioner": "768m",
            "proxy": "256m", "frontend": "2g",
        }.items() if not production or service != "frontend"
    ), encoding="utf-8")
    compose = [*COMPOSE, "-f", str(ROOT / ("docker-compose.prod.yml" if production else "docker-compose.dev.yml")),
               "-f", str(limits), "-p", project]
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
            if context not in {ROOT / "backend", ROOT / "frontend", ROOT / "kafka"}:
                raise RuntimeError(f"Unexpected build context for {name}")
        for port in service.get("ports", []):
            if port.get("host_ip") not in {"127.0.0.1", "::1"}:
                raise RuntimeError(f"Port for {name} is not bound to loopback")
        for mount in service.get("volumes", []):
            if mount["type"] == "bind":
                source = Path(mount["source"]).resolve()
                if "docker.sock" in str(source).lower():
                    raise RuntimeError("Docker socket mount is forbidden")
                if source.is_relative_to(run):
                    pass
                elif source == ROOT / "frontend" and name == "frontend":
                    pass
                elif source in {ROOT / "nginx" / "dev.conf", ROOT / "nginx" / "prod.conf"} and mount.get("read_only"):
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
    if not production:
        with urllib.request.urlopen(url + "/agents", timeout=15) as response:
            if response.status != 200 or "text/html" not in response.headers.get("Content-Type", ""):
                raise RuntimeError("Frontend route through proxy failed")
    print(f"Proxy smoke passed: {url} (API{' and frontend' if not production else ''})", flush=True)


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
    command([NPM, "run", "test:browser", "--", "agents.live.spec.ts", "--output", str(run / "playwright")],
            env=profile_env, cwd=ROOT / "frontend")
    from sqlalchemy import create_engine, text as sql_text
    home_engine = create_engine(env["TEST_DATABASE_URL"])
    try:
        with home_engine.begin() as connection:
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
            guest_permissions = connection.execute(sql_text(
                "UPDATE app.roles SET permissions = '[]'::jsonb WHERE name = 'Guest'"
            ))
            if guest_permissions.rowcount != 1:
                raise RuntimeError("The isolated Guest role was not prepared")
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

    base = profile_env["PLAYWRIGHT_BASE_URL"] + "/api/v1"
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    def request(path: str, method: str, body: dict | None = None, csrf: str | None = None):
        headers = {"Content-Type": "application/json"}
        if csrf:
            headers["X-CSRF-Token"] = csrf
        data = json.dumps(body).encode() if body is not None else None
        with opener.open(urllib.request.Request(base + path, data=data, headers=headers,
                                                method=method), timeout=15) as response:
            return json.load(response)
    session = request("/auth/local/login", "POST", {"username": "task21admin", "password": password})
    csrf = session["csrf_token"]
    rule = {"format_version": 1, "variants": [{"key": "plain", "priority": 1,
            "when": {"kind": "contains", "value": " "}, "blocks": [{"key": "message",
            "kind": "map_ecs", "target": "message", "source": {"ref": "log"},
            "required": True}]}]}
    normalizer = request("/normalizers", "POST", {"name": "Task 21 normalizer", "rule": rule}, csrf)
    agent_artifact = json.loads((run / "agent.json").read_text(encoding="utf-8"))
    agent_artifact["normalizerId"] = normalizer["id"]
    (run / "agent.json").write_text(json.dumps(agent_artifact), encoding="utf-8")
    source = agent["sourceId"]
    request(f"/sources/{source}/normalizer", "PUT", {"normalizer_id": normalizer["id"]}, csrf)
    request(f"/sources/{source}/enable", "POST", csrf=csrf)
    from sqlalchemy import create_engine
    from sqlalchemy import text as sql_text
    engine = create_engine(env["TEST_DATABASE_URL"])
    try:
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            with engine.connect() as connection:
                value = connection.execute(sql_text(
                    "SELECT ecs_data->>'message' FROM logs.parsed_logs WHERE source_id=:id "
                    "ORDER BY backend_processed_at DESC LIMIT 1"), {"id": source}).scalar_one_or_none()
            if value == "task21 smoke event":
                break
            time.sleep(1)
        else:
            raise RuntimeError("The source did not persist the normalized event in PostgreSQL")
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
                   "DATABASE_URL": env["TEST_DATABASE_URL"], "ENVIRONMENT": "test"}
    profile_env.pop("DATABASE_URL_FILE", None)
    create = (
        "import os,app.cli as cli; "
        "cli.getpass.getpass=lambda prompt: os.environ['TASK21_ADMIN_PASSWORD']; "
        "raise SystemExit(cli.create_admin('task21admin','Task 21 admin'))"
    )
    command([*compose, "exec", "-T", "-e", f"TASK21_ADMIN_PASSWORD={password}",
             "api", "python", "-c", create], env=profile_env)
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=["frontend", "backend-db", "kafka-integration", "end-to-end", "smoke-development", "smoke-production"])
    parser.add_argument("--pytest", nargs="*", help="Specific backend test nodes for a focused retry")
    parser.add_argument("--check-only", action="store_true", help="Resolve and validate Compose without starting containers")
    args = parser.parse_args()
    run = ROOT / ".harness" / "runs" / f"task21-{uuid4().hex}"
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
        print(f"Profile={args.profile}; project={project}; run={run}", flush=True)
        print(f"Addresses: PostgreSQL={env['POSTGRES_BIND']}, Kafka={env['KAFKA_TEST_BOOTSTRAP_SERVERS']}, proxy={env.get('HTTPS_BIND', env['PROXY_BIND'])}", flush=True)
        print("Secret variables: POSTGRES_PASSWORD_FILE, KAFKA_ADMIN_PASSWORD_FILE, KAFKA_READER_PASSWORD_FILE, AGENT_CREDENTIALS_KEY_FILE", flush=True)
        check_config(compose, project, env, run)
        if args.check_only:
            print("Resolved Compose config passed isolated-resource checks", flush=True)
            successful = True
            return
        if args.profile in {"kafka-integration", "smoke-development", "smoke-production", "end-to-end"}:
            builder = project + "_builder"
            command(["docker", "buildx", "create", "--name", builder, "--driver", "docker-container", "--bootstrap"], env=env)
            env["BUILDX_BUILDER"] = builder
            env["COMPOSE_DOCKER_CLI_BUILD"] = "1"
        if args.profile == "backend-db":
            command([*compose, "up", "-d", "--wait", "postgres"], env=env)
            command([sys.executable, "-m", "pytest", "-q", *(args.pytest or ["tests/test_auth_foundation.py", "tests/test_agent_contracts.py"])], env=env, cwd=ROOT / "backend")
        elif args.profile == "kafka-integration":
            command([*compose, "up", "-d", "--build", "--wait", "postgres", "kafka", "kafka-acls"], env=env)
            verify_kafka_metadata(compose, env)
            if args.pytest:
                command([sys.executable, "-m", "pytest", "-q", *args.pytest], env=env, cwd=ROOT / "backend")
            else:
                command([sys.executable, "scripts/run_tests.py", "--temporary"], env=env, cwd=ROOT / "backend")
        elif args.profile in {"smoke-development", "smoke-production", "end-to-end"}:
            production = args.profile == "smoke-production"
            services = ["postgres", "kafka", "kafka-acls", "migrate", "api", "provisioner", "proxy"]
            if not production:
                services.append("frontend")
            command([*compose, "up", "-d", "--build", "--wait", *services], env=env)
            verify_kafka_metadata(compose, env)
            verify_api_metadata(compose, env)
            if args.profile in {"end-to-end", "smoke-production"}:
                verify_agent_metadata(compose, project, env)
            smoke_http(env, production)
            if args.profile == "end-to-end":
                end_to_end(compose, project, env, run)
            if args.profile == "smoke-production":
                tls_agent_smoke(compose, project, env, run)
        successful = True
    finally:
        if compose and project and not args.check_only:
            if not successful and args.profile in {"backend-db", "kafka-integration", "smoke-development", "smoke-production", "end-to-end"}:
                logs = subprocess.run([*compose, "logs", "--no-color", "--tail=100", "postgres", "kafka", "api", "proxy"], cwd=ROOT, env=env, check=False, text=True, capture_output=True)
                selected = [line for line in logs.stdout.splitlines() if any(
                    token in line for token in ("ERROR", "Exception", "Caused by", "SSL", "TLS", "certificate", "unhealthy")
                )]
                print("\n".join(selected[-25:]).replace(env["POSTGRES_PASSWORD"], "[REDACTED]"), file=sys.stderr)
            command([*compose, "down", "--volumes", "--remove-orphans"], env=env)
            for service in ("kafka", "kafka-acls", "migrate", "api", "provisioner", "frontend"):
                image = f"{project}-{service}"
                matches = command(["docker", "image", "ls", "--filter", f"reference={image}", "--format", "{{.Repository}}"], env=env, capture=True).splitlines()
                if image in matches:
                    command(["docker", "image", "rm", image], env=env)
            if builder:
                command(["docker", "buildx", "rm", "--force", builder], env=env)
                env.pop("BUILDX_BUILDER", None)
            remove_new_images(before, env, project)
            after = inventory(env)
            (run / "docker-after.json").write_text(json.dumps(after, indent=2), encoding="utf-8")
            residual = command(["docker", "ps", "-a", "--filter", f"label=com.docker.compose.project={project}", "--format", "{{.ID}}"], env=env, capture=True).strip()
            volumes = command(["docker", "volume", "ls", "--filter", f"label=com.docker.compose.project={project}", "--format", "{{.Name}}"], env=env, capture=True).strip()
            networks = command(["docker", "network", "ls", "--filter", f"label=com.docker.compose.project={project}", "--format", "{{.Name}}"], env=env, capture=True).strip()
            if residual or volumes or networks:
                raise RuntimeError(f"Temporary Docker resources remain for {project}; run directory: {run}")
            compare_inventory(before, after, project)
        run_root = (ROOT / ".harness" / "runs").resolve()
        target = run.resolve()
        if target != run_root and target.is_relative_to(run_root):
            import shutil
            shutil.rmtree(target)
        else:
            raise RuntimeError(f"Unexpected run cleanup path: {target}")


if __name__ == "__main__":
    main()
