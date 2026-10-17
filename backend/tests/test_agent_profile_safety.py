"""Safety gates for the permanent isolated TASK-21 Compose profile."""

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("agent_test_profiles", ROOT / "scripts" / "agent_test_profiles.py")
assert SPEC and SPEC.loader
profiles = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(profiles)


def safe_config():
    return {
        "name": "diploma_t21_test",
        "volumes": {"postgres_data": {"name": "diploma_t21_test_postgres_data"}},
        "networks": {"internal": {"name": "diploma_t21_test_internal"}},
        "services": {"postgres": {
            "mem_limit": "768m", "cpus": 2,
            "ports": [{"host_ip": "127.0.0.1", "published": "15432", "target": 5432}],
            "volumes": [{"type": "volume", "source": "postgres_data", "target": "/var/lib/postgresql"}],
        }},
    }


@pytest.mark.parametrize("change", ["public_port", "privileged", "unlimited", "external_volume", "foreign_network", "foreign_secret", "docker_socket"])
def test_profile_rejects_unsafe_resolved_compose(monkeypatch, change):
    config = safe_config()
    if change == "public_port":
        config["services"]["postgres"]["ports"][0]["host_ip"] = "0.0.0.0"
    elif change == "privileged":
        config["services"]["postgres"]["privileged"] = True
    elif change == "unlimited":
        config["services"]["postgres"].pop("mem_limit")
    elif change == "external_volume":
        config["volumes"]["postgres_data"]["name"] = "diploma_postgres_data"
    elif change == "foreign_secret":
        config["secrets"] = {"postgres_password": {"file": str(ROOT / ".dev" / "postgres_password")}}
    elif change == "docker_socket":
        config["services"]["postgres"]["volumes"].append({
            "type": "bind", "source": "/var/run/docker.sock", "target": "/var/run/docker.sock", "read_only": True,
        })
    else:
        config["networks"]["internal"]["name"] = "diploma_internal"
    monkeypatch.setattr(profiles, "command", lambda *args, **kwargs: json.dumps(config))
    with pytest.raises(RuntimeError):
        profiles.check_config([], "diploma_t21_test", {}, ROOT / ".harness" / "runs" / "test")
