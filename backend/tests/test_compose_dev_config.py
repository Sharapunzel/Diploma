"""Check the rendered Compose boundary, not only in-process Settings values."""

import json
import os
import shutil
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_dev_compose_passes_external_key_and_oidc_settings():
    if shutil.which("docker") is None:
        pytest.skip("Docker Compose CLI is unavailable")

    project = f"diploma-config-check-{uuid4().hex[:12]}"
    expected = {
        "EXTERNAL_SECRET_KEY": "FjzCZG1bo4T5WBYB_oBUrhyoSEkbxsOgCwMsNXCEsVw=",
        "OIDC_ENABLED": "true",
        "OIDC_ISSUER_URL": "https://identity.example.test",
        "OIDC_CLIENT_ID": "dev-client",
        "OIDC_CLIENT_SECRET": "synthetic-dev-client-secret",
        "OIDC_REDIRECT_URI": "http://localhost:18080/api/v1/auth/oidc/callback",
        "OIDC_SCOPES": "openid,profile,email",
        "OIDC_SUCCESS_REDIRECT_URL": "/",
        "OIDC_ERROR_REDIRECT_URL": "/login?error=oidc",
        "LOCAL_AUTH_ENABLED": "true",
    }
    environment = os.environ.copy()
    environment.update(expected)
    environment.update({
        "POSTGRES_DB": "diploma_compose_config_check",
        "POSTGRES_BIND": "127.0.0.1:15433:5432",
        "PROXY_BIND": "127.0.0.1:18080:8080",
        "KAFKA_HOST_PORT": "19093",
        "COMPOSE_SUBNET": "172.31.83.0/24",
        "PROXY_IP": "172.31.83.10",
    })
    command = [
        "docker", "compose", "-p", project,
        "-f", "docker-compose.yml", "-f", "docker-compose.dev.yml",
        "config", "--format", "json",
    ]
    result = subprocess.run(
        command, cwd=ROOT, env=environment, capture_output=True, text=True,
        check=True, timeout=30,
    )
    config = json.loads(result.stdout)
    api = config["services"]["api"]["environment"]
    assert config["name"] == project
    assert {item["name"] for item in config["volumes"].values()} == {
        f"{project}_postgres_data", f"{project}_kafka_data",
    }
    for key, value in expected.items():
        assert api[key] == value
    assert api["ENVIRONMENT"] == "development"
    assert api["SESSION_COOKIE_SECURE"] == "false"
