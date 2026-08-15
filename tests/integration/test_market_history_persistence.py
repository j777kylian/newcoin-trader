"""Concrete collector -> MarketHistoryService -> PostgreSQL persistence.

Runs only when NEWCOIN_TEST_DATABASE_URL points at PostgreSQL.
Skipped clearly otherwise — this is not an SQLite stand-in.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from newcoin_trader.collectors.binance.client import BinanceClient
from newcoin_trader.collectors.gecko.client import GeckoTerminalClient
from newcoin_trader.collectors.http import AsyncHttpClient
from newcoin_trader.collectors.raydium.client import RaydiumClient
from newcoin_trader.database.base import Base
from newcoin_trader.database.models import PriceSnapshot, Token, Trade
from newcoin_trader.database.repositories.market import MarketRepository
from newcoin_trader.database.repositories.tokens import TokenRepository
from newcoin_trader.services.ingestion import MarketHistoryService
from tests.integration._postgres import get_test_database_url

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "fixtures"
DATABASE_URL = get_test_database_url()


def _postgres_available() -> bool:
    return DATABASE_URL.startswith("postgresql")


def _load(rel: str) -> object:
    return json.loads((FIXTURES / rel).read_text(encoding="utf-8"))


async def _no_sleep(_seconds: float) -> None:
    return None


def _fixture_handler(
    request: httpx.Request,
    *,
    hits: dict[str, int] | None = None,
    fail_first_path: str | None = None,
) -> httpx.Response:
    path = request.url.path
    if hits is not None:
        hits[path] = hits.get(path, 0) + 1
        if fail_first_path == path and hits[path] == 1:
            return httpx.Response(500, json={"error": "transient"})
    if path.endswith("/api/v3/klines"):
        return httpx.Response(200, json=_load("binance/klines.json"))
    if path.endswith("/api/v3/aggTrades"):
        return httpx.Response(200, json=_load("binance/agg_trades.json"))
    if path.endswith("/api/v3/trades"):
        return httpx.Response(200, json=_load("binance/trades.json"))
    if path.endswith("/pools/info/list"):
        return httpx.Response(200, json=_load("raydium/pools.json"))
    if path.endswith("/ohlcv/minute"):
        return httpx.Response(200, json=_load("gecko/ohlcv.json"))
    if "/networks/" in path and "/pools/" in path:
        return httpx.Response(200, json=_load("gecko/pool.json"))
    return httpx.Response(404, json={"error": path})


@pytest.fixture
async def session() -> AsyncIterator[AsyncSession]:
    """Per-case PostgreSQL schema isolation.

    Rollback after the service commits is not enough: each parametrized venue case
    gets a unique temporary schema, ``search_path`` is set on every connection, and
    the schema is dropped with CASCADE even if the test fails or data was committed.
    """
    if not _postgres_available():
        pytest.skip("PostgreSQL not configured (set NEWCOIN_TEST_DATABASE_URL to a postgresql:// URL)")

    schema = f"test_mh_{uuid.uuid4().hex}"
    admin_engine = create_async_engine(DATABASE_URL, poolclass=NullPool)
    try:
        async with admin_engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    except (OSError, OperationalError, ConnectionRefusedError):
        await admin_engine.dispose()
        pytest.skip("PostgreSQL is not reachable")

    engine = create_async_engine(
        DATABASE_URL,
        poolclass=NullPool,
        connect_args={"server_settings": {"search_path": schema}},
    )
    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'SET search_path TO "{schema}"'))
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as sess:
            await sess.execute(text(f'SET search_path TO "{schema}"'))
            yield sess
    except (OSError, OperationalError, ConnectionRefusedError):
        pytest.skip("PostgreSQL is not reachable")
    finally:
        await engine.dispose()
        try:
            async with admin_engine.begin() as conn:
                await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        except (OSError, OperationalError, ConnectionRefusedError):
            pass
        await admin_engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "venue",
    ["binance", "raydium", "gecko"],
)
async def test_concrete_market_history_persists_per_venue(
    session: AsyncSession,
    venue: str,
) -> None:
    hits: dict[str, int] = {}
    fail_paths = {
        "binance": "/api/v3/klines",
        "raydium": "/pools/info/list",
        "gecko": "/api/v2/networks/solana/pools/PoolAddress111111111111111111111111111",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        # Guard against accidental network escape: only fixture host paths are served.
        assert request.url.host in {
            "api.binance.com",
            "api-v3.raydium.io",
            "api.geckoterminal.com",
        }
        return _fixture_handler(
            request,
            hits=hits,
            fail_first_path=fail_paths[venue],
        )

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=3,
        rate_limit_per_second=1000.0,
        sleep=_no_sleep,
    )
    tokens = TokenRepository(session)
    market = MarketRepository(session)
    service = MarketHistoryService(
        binance=BinanceClient(http=http, base_url="https://api.binance.com"),
        raydium=RaydiumClient(
            http=http,
            pool_base_url="https://api-v3.raydium.io",
            quote_base_url="https://transaction-v1.raydium.io",
        ),
        gecko=GeckoTerminalClient(http=http, base_url="https://api.geckoterminal.com/api/v2"),
        tokens=tokens,
        market=market,
    )

    ingest_kwargs: dict[str, Any]
    if venue == "binance":
        ingest_kwargs = {
            "binance_symbol": "NEWUSDT",
            "binance_interval": "1h",
            "binance_limit": 2,
            "include_binance_recent_trades": True,
        }
    elif venue == "raydium":
        ingest_kwargs = {"raydium_page": 1, "raydium_page_size": 1}
    else:
        ingest_kwargs = {
            "gecko_network": "solana",
            "gecko_pool": "PoolAddress111111111111111111111111111",
            "gecko_ohlcv_limit": 3,
        }

    first = await service.ingest_market_history(**ingest_kwargs)
    await session.commit()
    second = await service.ingest_market_history(**ingest_kwargs)
    await session.commit()
    await http.aclose()

    assert hits[fail_paths[venue]] >= 2
    assert first.snapshots == second.snapshots
    assert first.trades == second.trades

    token_count = await session.scalar(select(func.count()).select_from(Token))
    snap_count = await session.scalar(select(func.count()).select_from(PriceSnapshot))
    trade_count = await session.scalar(select(func.count()).select_from(Trade))
    assert token_count is not None and token_count >= 1
    assert snap_count is not None and snap_count == first.snapshots
    assert trade_count is not None and trade_count == first.trades

    if venue == "binance":
        token = (
            await session.execute(select(Token).where(Token.chain == "binance", Token.token_address == "NEWUSDT"))
        ).scalar_one()
        snaps = (
            (
                await session.execute(
                    select(PriceSnapshot).where(
                        PriceSnapshot.token_id == token.id,
                        PriceSnapshot.source == "binance",
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(snaps) == 1
        assert snaps[0].timestamp == datetime.fromtimestamp(1704070799999 / 1000, tz=UTC)
        assert snaps[0].price == Decimal("1.10000000")
        agg = (await session.execute(select(Trade).where(Trade.source == "binance:aggTrades"))).scalars().all()
        assert len(agg) == 1
        assert agg[0].external_trade_id == "26129"
    elif venue == "raydium":
        pool_id = "Pool1111111111111111111111111111111111111"
        token = (
            await session.execute(select(Token).where(Token.chain == "solana", Token.token_address == pool_id))
        ).scalar_one()
        snaps = (
            (
                await session.execute(
                    select(PriceSnapshot).where(
                        PriceSnapshot.token_id == token.id,
                        PriceSnapshot.source == "raydium",
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(snaps) == 1
        assert snaps[0].price == Decimal("0.00125")
        assert snaps[0].timestamp.tzinfo is not None
    else:
        pool = "PoolAddress111111111111111111111111111"
        token = (
            await session.execute(select(Token).where(Token.chain == "solana", Token.token_address == pool))
        ).scalar_one()
        snaps = (await session.execute(select(PriceSnapshot).where(PriceSnapshot.token_id == token.id))).scalars().all()
        assert {row.source for row in snaps} == {"geckoterminal:pool", "geckoterminal:ohlcv:1m"}
        assert len(snaps) == 2
