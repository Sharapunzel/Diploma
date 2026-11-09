"""Credential preparation never repairs or rotates existing user paths."""

import stat

import pytest

from scripts import prepare_dev_kafka as credentials

NAMES = ("kafka_admin_password", "kafka_reader_password", "wal_reader_password")


def test_missing_files_are_created_and_repeated_preparation_preserves_them(tmp_path):
    root = tmp_path / ".dev"
    credentials.prepare(root)
    before = {name: ((root / name).read_bytes(), (root / name).stat().st_mode) for name in NAMES}
    assert all(value[0].strip() and stat.S_ISREG(value[1]) for value in before.values())
    credentials.prepare(root)
    assert {name: ((root / name).read_bytes(), (root / name).stat().st_mode) for name in NAMES} == before


@pytest.mark.parametrize("name", NAMES)
@pytest.mark.parametrize("kind", ["directory", "nonempty_directory", "empty", "whitespace"])
def test_invalid_paths_fail_without_ready_or_modification(tmp_path, monkeypatch, capsys, name, kind):
    (tmp_path / "scripts").mkdir()
    root = tmp_path / ".dev"
    root.mkdir()
    path = root / name
    if "directory" in kind:
        path.mkdir()
        if kind == "nonempty_directory":
            (path / "keep").write_text("user data", encoding="utf-8")
    else:
        path.write_text("" if kind == "empty" else " \n", encoding="utf-8")
    monkeypatch.setattr(credentials, "__file__", str(tmp_path / "scripts" / "prepare_dev_kafka.py"))
    with pytest.raises(SystemExit) as result:
        credentials.main()
    assert result.value.code == 1
    output = capsys.readouterr()
    assert "ready" not in output.out and name in output.err
    assert list(root.iterdir()) == [path]
    if kind == "nonempty_directory":
        assert (path / "keep").read_text(encoding="utf-8") == "user data"


def test_symlink_is_not_a_credential_file(tmp_path, monkeypatch):
    from pathlib import Path
    from types import SimpleNamespace

    link = tmp_path / "link"
    monkeypatch.setattr(Path, "lstat", lambda _path: SimpleNamespace(st_mode=stat.S_IFLNK))
    with pytest.raises(ValueError, match="link"):
        credentials.validate_existing(link)


def test_invalid_peer_does_not_change_valid_credentials(tmp_path):
    root = tmp_path / ".dev"
    root.mkdir()
    existing = root / "kafka_admin_password"
    existing.write_bytes(b"synthetic-existing-credential\n")
    before = (existing.read_bytes(), existing.stat().st_mode)
    (root / "wal_reader_password").mkdir()
    with pytest.raises(ValueError, match="wal_reader_password"):
        credentials.prepare(root)
    assert (existing.read_bytes(), existing.stat().st_mode) == before
    assert not (root / "kafka_reader_password").exists()
