"""PostgreSQL integration-test environment boundary."""

from __future__ import annotations

import pytest

from tests.integration._postgres import get_test_database_url


def test_postgres_integration_url_never_falls_back_to_application_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://application-db/newcoin")
    monkeypatch.delenv("NEWCOIN_TEST_DATABASE_URL", raising=False)

    assert get_test_database_url() == ""


def test_postgres_integration_url_uses_only_explicit_test_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://application-db/newcoin")
    monkeypatch.setenv("NEWCOIN_TEST_DATABASE_URL", "postgresql+asyncpg://test-db/newcoin_test")

    assert get_test_database_url() == "postgresql+asyncpg://test-db/newcoin_test"
