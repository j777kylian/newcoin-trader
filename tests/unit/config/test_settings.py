"""Settings load paper-only configuration from the environment."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from newcoin_trader.config import Settings


def test_default_execution_mode_is_paper() -> None:
    settings = Settings(_env_file=None)
    assert settings.execution_mode == "paper"


def test_live_execution_mode_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXECUTION_MODE", "live")
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_database_url_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+asyncpg://user:pass@localhost:5432/research",
    )
    settings = Settings(_env_file=None)
    assert settings.database_url.endswith("/research")
