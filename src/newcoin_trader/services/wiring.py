"""Wire settings → read-only HTTP clients → async DB → ingestion service."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from newcoin_trader.collectors.binance.client import BinanceClient
from newcoin_trader.collectors.birdeye.client import BirdeyeClient
from newcoin_trader.collectors.gecko.client import GeckoTerminalClient
from newcoin_trader.collectors.http import AsyncHttpClient
from newcoin_trader.collectors.raydium.client import RaydiumClient
from newcoin_trader.config import Settings
from newcoin_trader.database.engine import create_engine, create_session_factory
from newcoin_trader.database.repositories.market import MarketRepository
from newcoin_trader.database.repositories.tokens import TokenRepository
from newcoin_trader.errors import ConfigError
from newcoin_trader.services.ingestion import IngestionService, MarketHistoryService


@dataclass
class LiveIngestionStack:
    settings: Settings
    http: AsyncHttpClient
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]


@dataclass
class ResearchDbStack:
    """DATABASE_URL research reads only — no HTTP collectors."""

    settings: Settings
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]


@asynccontextmanager
async def open_research_db_stack(settings: Settings) -> AsyncIterator[ResearchDbStack]:
    """Open async Postgres for bounded research queries (no network collectors)."""
    engine = create_engine(settings.database_url)
    factory = create_session_factory(engine)
    stack = ResearchDbStack(settings=settings, engine=engine, session_factory=factory)
    try:
        yield stack
    finally:
        await engine.dispose()


def require_birdeye_api_key(settings: Settings) -> str:
    key = settings.birdeye_api_key.strip()
    if not key:
        raise ConfigError(
            "BIRDEYE_API_KEY is required for live collect-once/poll Solana discovery. "
            "Export it in your shell (see .env.example). Unit tests never need a real key."
        )
    return key


@asynccontextmanager
async def open_live_stack(settings: Settings) -> AsyncIterator[LiveIngestionStack]:
    """Open GET-only HTTP clients and an async Postgres engine. No order endpoints."""
    http = AsyncHttpClient(
        timeout_seconds=settings.http_timeout_seconds,
        max_attempts=settings.http_max_attempts,
        backoff_seconds=settings.http_backoff_seconds,
        rate_limit_per_second=settings.http_rate_limit_per_second,
    )
    engine = create_engine(settings.database_url)
    factory = create_session_factory(engine)
    stack = LiveIngestionStack(
        settings=settings,
        http=http,
        engine=engine,
        session_factory=factory,
    )
    try:
        yield stack
    finally:
        await http.aclose()
        await engine.dispose()


def build_ingestion_service(
    *,
    settings: Settings,
    http: AsyncHttpClient,
    session: AsyncSession,
) -> IngestionService:
    api_key = require_birdeye_api_key(settings)
    binance = BinanceClient(http=http, base_url=settings.binance_base_url)
    birdeye = BirdeyeClient(
        http=http,
        api_key=api_key,
        base_url=settings.birdeye_base_url,
        chain=settings.birdeye_chain,
    )
    return IngestionService(
        binance=binance,
        birdeye=birdeye,
        tokens=TokenRepository(session),
        market=MarketRepository(session),
    )


def build_market_history_service(
    *,
    settings: Settings,
    http: AsyncHttpClient,
    session: AsyncSession,
) -> MarketHistoryService:
    """Wire GET-only Binance/Raydium/Gecko collectors for bounded market-history ingest."""
    return MarketHistoryService(
        binance=BinanceClient(http=http, base_url=settings.binance_base_url),
        raydium=RaydiumClient(
            http=http,
            pool_base_url=settings.raydium_pool_base_url,
            quote_base_url=settings.raydium_quote_base_url,
        ),
        gecko=GeckoTerminalClient(http=http, base_url=settings.gecko_base_url),
        tokens=TokenRepository(session),
        market=MarketRepository(session),
    )
