"""The permanent browser writer's gate must coexist with event-time RR reads.

Run with the existing backend-db profile and --pytest tests/test_browser_fixture.py.
No fixture quality, timestamps, capture boundary or payloads are manufactured.
"""

import importlib.util
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
from app.db import Database
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError, OperationalError


def load_browser_fixture():
    path = Path(__file__).resolve().parents[2] / "scripts/browser_detector_fixture.py"
    spec = importlib.util.spec_from_file_location("browser_detector_fixture", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validated_test_url():
    raw = os.environ.get("TEST_DATABASE_URL")
    message = "TEST_DATABASE_URL must point to isolated PostgreSQL diploma_test or diploma_test_review_<12 hex> without query parameters"
    if not raw:
        raise ValueError(message)
    try:
        parsed = make_url(raw)
    except ArgumentError:
        raise ValueError(message) from None
    if (parsed.get_backend_name() != "postgresql" or parsed.query
            or not re.fullmatch(r"diploma_test(?:_review_[0-9a-f]{12})?", parsed.database or "")):
        raise ValueError(message)
    return raw


def migrate_test_database():
    url = validated_test_url()
    env = {**os.environ, "DATABASE_URL": url}
    env.pop("DATABASE_URL_FILE", None)
    subprocess.run([sys.executable, "-B", "-m", "alembic", "upgrade", "head"],
                   env=env, check=True)
    return url


@pytest.fixture(scope="module", autouse=True)
def migrated():
    return migrate_test_database()


@pytest.mark.parametrize("abort", [False, True])
def test_working_gate_allows_rr_reader_and_releases_every_lock(abort, migrated):
    database = Database(migrated)
    fixture = load_browser_fixture()
    try:
        with database.session_factory.begin() as snapshot:
            snapshot.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            snapshot.execute(text("SELECT id FROM logs.source_interruptions LIMIT 1"))
            # This is the former gate: a long-running read keeps ACCESS SHARE
            # until its RR finishes. It deterministically reproduces the real
            # lock_timeout with no SQL or point-quality modification.
            with pytest.raises(OperationalError) as caught, database.session_factory.begin() as old:
                old.execute(text("SET LOCAL lock_timeout='1000ms'"))
                old.execute(text("LOCK TABLE logs.source_interruptions IN ACCESS EXCLUSIVE MODE"))
            assert caught.value.orig.sqlstate == "55P03"
            if abort:
                with pytest.raises(RuntimeError, match="synthetic abort"), fixture.working_closure_gate(database):
                    raise RuntimeError("synthetic abort")
            else:
                with fixture.working_closure_gate(database) as publication:
                    assert publication.scalar(text("SELECT 1")) == 1
                    assert snapshot.scalar(text("SELECT count(*) FROM logs.source_interruptions")) >= 0
        # Both successful and exceptional publication released their locks.
        with database.session_factory.begin() as check:
            check.execute(text("SET LOCAL lock_timeout='1000ms'"))
            check.execute(text("LOCK TABLE logs.source_interruptions IN ACCESS EXCLUSIVE MODE"))
    finally:
        database.dispose()


@pytest.mark.parametrize("name", ["diploma_test", "diploma_test_review_012345abcdef"])
def test_browser_fixture_migrates_only_validated_test_target(monkeypatch, name):
    url = f"postgresql+psycopg://fixture:synthetic-password@127.0.0.1:5432/{name}"
    monkeypatch.setenv("TEST_DATABASE_URL", url)
    monkeypatch.setenv("DATABASE_URL_FILE", "/synthetic-not-read")
    calls = []
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: calls.append((args, kwargs)))
    assert migrate_test_database() == url
    assert len(calls) == 1
    args, kwargs = calls[0]
    assert args == ([sys.executable, "-B", "-m", "alembic", "upgrade", "head"],)
    assert kwargs["check"] is True
    assert kwargs["env"]["DATABASE_URL"] == url
    assert "DATABASE_URL_FILE" not in kwargs["env"]


@pytest.mark.parametrize("raw", [
    None,
    "",
    "not-a-database-url",
    "sqlite:///diploma_test",
    "postgresql+psycopg://fixture:synthetic-password@127.0.0.1/diploma_db",
    "postgresql+psycopg://fixture:synthetic-password@127.0.0.1/postgres",
    "postgresql+psycopg://fixture:synthetic-password@127.0.0.1/another_test",
    "postgresql+psycopg://fixture:synthetic-password@127.0.0.1/diploma_test?sslmode=disable",
    "postgresql+psycopg://fixture:synthetic-password@127.0.0.1/diploma_test_review_012345abcde",
    "postgresql+psycopg://fixture:synthetic-password@127.0.0.1/diploma_test_review_012345abcdef0",
    "postgresql+psycopg://fixture:synthetic-password@127.0.0.1/diploma_test_review_012345ABCDEF",
    "postgresql+psycopg://fixture:synthetic-password@127.0.0.1/diploma_test_review_012345abcdeg",
])
def test_browser_fixture_rejects_target_before_migration(monkeypatch, raw):
    if raw is None:
        monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    else:
        monkeypatch.setenv("TEST_DATABASE_URL", raw)
    calls = []
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: calls.append((args, kwargs)))
    with pytest.raises(ValueError, match="TEST_DATABASE_URL") as caught:
        migrate_test_database()
    assert calls == []
    if raw:
        assert raw not in str(caught.value)
    assert "synthetic-password" not in str(caught.value)
