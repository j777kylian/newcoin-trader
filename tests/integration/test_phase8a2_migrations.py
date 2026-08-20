"""Phase 8A.2 alembic upgrade/downgrade against the dedicated test database only."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine

from tests.integration._postgres import get_test_database_url

pytestmark = pytest.mark.integration

TEST_DATABASE_URL = get_test_database_url()
ROOT = Path(__file__).resolve().parents[2]
EXPECTED_NEW_TABLES = (
    "markets",
    "early_market_events",
    "early_market_event_evidence",
    "early_market_observations",
)
LEGACY_TABLES = (
    "tokens",
    "price_snapshots",
    "trades",
    "strategy_results",
    "paper_trades",
    "live_paper_sessions",
    "live_paper_signals",
    "live_paper_positions",
)


def _postgres_available() -> bool:
    return TEST_DATABASE_URL.startswith("postgresql")


def _alembic_config(url: str) -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


async def _table_names(url: str) -> set[str]:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            names = await conn.run_sync(lambda sync_conn: set(inspect(sync_conn).get_table_names()))
        return names
    finally:
        await engine.dispose()


async def _reset_schema(url: str) -> None:
    engine = create_async_engine(url)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("DROP SCHEMA public CASCADE"))
            await conn.execute(text("CREATE SCHEMA public"))
    finally:
        await engine.dispose()


def test_migration_tests_never_fall_back_to_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://application-db/newcoin")
    monkeypatch.delenv("NEWCOIN_TEST_DATABASE_URL", raising=False)
    assert get_test_database_url() == ""


def test_phase8a2_migration_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    if not _postgres_available():
        pytest.skip("PostgreSQL not configured")

    # Alembic env reads DATABASE_URL; bind it only to the dedicated test URL.
    monkeypatch.setenv("DATABASE_URL", TEST_DATABASE_URL)
    cfg = _alembic_config(TEST_DATABASE_URL)

    try:
        asyncio.run(_reset_schema(TEST_DATABASE_URL))
        command.upgrade(cfg, "0002_live_paper_session_state")
        before = asyncio.run(_table_names(TEST_DATABASE_URL))
        assert set(LEGACY_TABLES) <= before
        assert not (set(EXPECTED_NEW_TABLES) & before)

        command.upgrade(cfg, "0003_early_event_store")
        after_upgrade = asyncio.run(_table_names(TEST_DATABASE_URL))
        assert set(LEGACY_TABLES) <= after_upgrade
        assert set(EXPECTED_NEW_TABLES) <= after_upgrade

        command.downgrade(cfg, "0002_live_paper_session_state")
        after_downgrade = asyncio.run(_table_names(TEST_DATABASE_URL))
        assert set(LEGACY_TABLES) <= after_downgrade
        assert not (set(EXPECTED_NEW_TABLES) & after_downgrade)

        command.upgrade(cfg, "0003_early_event_store")
        after_reupgrade = asyncio.run(_table_names(TEST_DATABASE_URL))
        assert set(EXPECTED_NEW_TABLES) <= after_reupgrade
    except (OSError, OperationalError, ConnectionRefusedError):
        pytest.skip("PostgreSQL is not reachable")
