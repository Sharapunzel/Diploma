import pytest

from scripts.run_tests import database_urls


def test_runner_accepts_only_dedicated_base_database(monkeypatch):
    monkeypatch.setenv(
        "TEST_DATABASE_URL",
        "postgresql+psycopg://user:password@127.0.0.1:5432/diploma_test",
    )
    test_url, admin_url = database_urls()
    assert test_url.endswith("/diploma_test")
    assert admin_url.endswith("/postgres")


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+psycopg://user:password@127.0.0.1:5432/diploma_db",
        "postgresql+psycopg://user:password@127.0.0.1:5432/another_test",
        "postgresql+psycopg://user:password@127.0.0.1:5432/diploma_test?sslmode=disable",
        "sqlite:///diploma_test",
    ],
)
def test_runner_rejects_other_or_ambiguous_targets(monkeypatch, url):
    monkeypatch.setenv("TEST_DATABASE_URL", url)
    with pytest.raises(ValueError):
        database_urls()
