"""Regression checks for Docker resources created before Compose starts."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from scripts import agent_test_profiles as profiles


@pytest.mark.parametrize("check_only", [False, True])
def test_external_preparation_removes_only_new_image(tmp_path: Path, monkeypatch, check_only: bool):
    project = "diploma_t21_cleanup_test"
    image = "sha256:new-wazuh wazuh/wazuh-indexer:<none>"
    foreign = "sha256:old-foreign user/foreign:latest"
    present = {foreign}
    removed: list[str] = []
    before = {kind: [] for kind in (
        "containers", "networks", "volumes", "images", "builders", "default_cache"
    )}
    before["images"] = [foreign]

    def inventory(_env):
        return {**before, "images": sorted(present)}

    def command(args, **_kwargs):
        if args[:3] == ["docker", "image", "rm"]:
            assert args[3] == "sha256:new-wazuh"
            present.remove(image)
            removed.append(args[3])
        return ""

    def prepare(run, _profile):
        return ({"POSTGRES_PASSWORD": "synthetic", "POSTGRES_BIND": "127.0.0.1:1:5432",
                 "KAFKA_TEST_BOOTSTRAP_SERVERS": "127.0.0.1:2", "PROXY_BIND": "127.0.0.1:3:8080"},
                ["docker", "compose", "-p", project], project)

    def overlay(run, _env):
        present.add(image)
        if not check_only:
            raise RuntimeError("synthetic hash failure")
        path = run / "wazuh-compose.json"
        path.write_text("{}", encoding="utf-8")
        return path

    monkeypatch.setattr(profiles, "ROOT", tmp_path)
    monkeypatch.setattr(profiles, "inventory", inventory)
    monkeypatch.setattr(profiles, "command", command)
    monkeypatch.setattr(profiles, "prepare", prepare)
    monkeypatch.setattr(profiles, "wazuh_compose_overlay", overlay)
    monkeypatch.setattr(profiles, "check_config", lambda *_args: None)
    monkeypatch.setattr(profiles.subprocess, "run", lambda *_args, **_kwargs: SimpleNamespace(stdout=""))
    argv = ["agent_test_profiles.py", "external-end-to-end"]
    if check_only:
        argv.append("--check-only")
    with patch.object(sys, "argv", argv):
        if check_only:
            profiles.main()
        else:
            with pytest.raises(RuntimeError, match="synthetic hash failure"):
                profiles.main()

    assert removed == ["sha256:new-wazuh"]
    assert present == {foreign}
    assert list((tmp_path / ".harness" / "runs").iterdir()) == []
