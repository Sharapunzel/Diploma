"""Regression checks for Docker resources created before Compose starts."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from scripts import agent_test_profiles as profiles


def test_volume_inventory_survives_down_and_cleans_only_proven_mounts(tmp_path, monkeypatch):
    project = "diploma_t21_volume_test"
    current = "first"
    present = {"anonymous-first", "anonymous-second", "foreign"}
    removed = []
    def command(args, **_kwargs):
        if args[:3] == ["docker", "ps", "-aq"]:
            return current if "label=" in args[-1] else ""
        if args[:4] == ["docker", "inspect", "--type", "container"]:
            return json.dumps([{"Id": current, "Config": {"Labels": {
                "com.docker.compose.project": project, "com.docker.compose.service": "kafka"}},
                "Mounts": [{"Type": "volume", "Name": "anonymous-" + current,
                            "Destination": "/etc/kafka/secrets"}]}])
        if args[:3] == ["docker", "volume", "ls"]:
            return "\n".join(present)
        if args[:3] == ["docker", "volume", "inspect"]:
            return json.dumps([{"Name": args[3], "Labels": {"com.docker.volume.anonymous": ""}}])
        if args[:3] == ["docker", "volume", "rm"]:
            present.remove(args[3])
            removed.append(args[3])
            return ""
        raise AssertionError(args)
    monkeypatch.setattr(profiles, "command", command)
    profiles.record_compose_volumes(project, {}, tmp_path)
    # The first container disappeared in ordinary down; its association must
    # still be present when the second cycle records a different anonymous mount.
    current = "second"
    profiles.record_compose_volumes(project, {}, tmp_path)
    profiles.remove_recorded_volumes(project, {}, tmp_path, {"volumes": ["foreign"]})
    assert removed == ["anonymous-first", "anonymous-second"]
    assert present == {"foreign"}


@pytest.mark.parametrize("reason", ["preexisting", "attached", "labels", "project"])
def test_recorded_volume_cleanup_refuses_unproven_or_attached_volume(tmp_path, monkeypatch, reason):
    project = "diploma_t21_volume_test"
    record = {"project": "foreign" if reason == "project" else project,
              "container": "owned", "volume": "candidate", "service": "kafka", "destination": "/data"}
    (tmp_path / "compose-volume-mounts.json").write_text(json.dumps([record]), encoding="utf-8")
    def command(args, **_kwargs):
        assert args[:3] != ["docker", "volume", "rm"]
        if args[:3] == ["docker", "volume", "ls"]:
            return "candidate"
        if args[:3] == ["docker", "ps", "-aq"]:
            return "foreign-container" if reason == "attached" else ""
        return json.dumps([{"Name": "candidate", "Labels": {}}])
    monkeypatch.setattr(profiles, "command", command)
    with pytest.raises(RuntimeError):
        profiles.remove_recorded_volumes(project, {}, tmp_path,
                                         {"volumes": ["candidate"] if reason == "preexisting" else []})


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
    assert list((tmp_path / ".test_runs" / "runs").iterdir()) == []


def test_failed_builder_removes_only_its_new_untagged_image(monkeypatch):
    before = {kind: [] for kind in (
        "containers", "networks", "volumes", "images", "builders", "default_cache"
    )}
    before["images"] = ["sha256:old <none>:<none>"]
    present = set(before["images"]) | {"sha256:buildkit <none>:<none>"}
    removed = []

    def inventory(_env):
        return {**before, "images": sorted(present)}

    def command(args, **_kwargs):
        if args[:3] == ["docker", "ps", "-a"] and "--filter" in args:
            return "abc buildx_buildkit_diploma_t21_test_builder0"
        if args[:2] == ["docker", "inspect"]:
            return "sha256:buildkit"
        if args[:3] == ["docker", "image", "rm"]:
            removed.append(args[3])
            present.remove("sha256:buildkit <none>:<none>")
        return ""

    monkeypatch.setattr(profiles, "inventory", inventory)
    monkeypatch.setattr(profiles, "command", command)
    images = profiles.builder_image_ids("diploma_t21_test_builder", {})
    # The builder container is gone before image deletion.
    monkeypatch.setattr(profiles, "command", lambda args, **kwargs: (
        "" if args[:3] == ["docker", "ps", "-a"] else command(args, **kwargs)
    ))
    profiles.remove_new_images(before, {}, "diploma_t21_test", "kafka-integration", images)
    profiles.compare_inventory(before, inventory({}), "diploma_t21_test")
    assert removed == ["sha256:buildkit"]
    assert present == set(before["images"])
