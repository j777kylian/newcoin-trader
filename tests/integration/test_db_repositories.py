"""PostgreSQL repository integration tests (skipped without test DB URL)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from newcoin_trader.database.base import Base
from newcoin_trader.database.repositories.market import MarketRepository
from newcoin_trader.database.repositories.paper import PaperTradeRepository
from newcoin_trader.database.repositories.strategy import StrategyResultRepository
from newcoin_trader.database.repositories.tokens import TokenRepository
from newcoin_trader.domain.enums import PaperStatus, Side
from tests.integration._postgres import get_test_database_url

pytestmark = pytest.mark.integration

DATABASE_URL = get_test_database_url()


def _postgres_available() -> bool:
    return DATABASE_URL.startswith("postgresql")


@pytest.fixture
async def session() -> AsyncIterator[AsyncSession]:
    if not _postgres_available():
        pytest.skip("PostgreSQL not configured")
    engine = create_async_engine(DATABASE_URL)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as sess:
            yield sess
            await sess.rollback()
    except (OSError, OperationalError, ConnectionRefusedError):
        pytest.skip("PostgreSQL is not reachable")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_token_upsert_preserves_earliest_first_seen(session: AsyncSession) -> None:
    repo = TokenRepository(session)
    early = datetime(2024, 1, 1, tzinfo=UTC)
    late = datetime(2024, 2, 1, tzinfo=UTC)
    first = await repo.upsert(
        chain="solana",
        token_address="Mint111",
        symbol="MEME",
        created_time=early,
        first_seen_time=early,
        source="birdeye",
    )
    second = await repo.upsert(
        chain="solana",
        token_address="Mint111",
        symbol="MEME2",
        created_time=late,
        first_seen_time=late,
        source="birdeye",
    )
    await session.commit()
    assert first.id == second.id
    assert second.first_seen_time == early
    assert second.symbol == "MEME2"


@pytest.mark.asyncio
async def test_snapshot_insert_is_idempotent(session: AsyncSession) -> None:
    tokens = TokenRepository(session)
    token = await tokens.upsert(
        chain="binance",
        token_address="NEWUSDT",
        symbol="NEWUSDT",
        created_time=datetime(2024, 1, 1, tzinfo=UTC),
        first_seen_time=datetime(2024, 1, 1, tzinfo=UTC),
        source="binance",
    )
    market = MarketRepository(session)
    ts = datetime(2024, 1, 1, 1, tzinfo=UTC)
    kwargs = {
        "token_id": token.id,
        "timestamp": ts,
        "price": Decimal("1.1"),
        "volume": Decimal("10"),
        "liquidity": Decimal("1000"),
        "market_cap": Decimal("5000"),
        "buy_count": 3,
        "sell_count": 2,
        "source": "binance",
    }
    a = await market.upsert_snapshot(**kwargs)
    b = await market.upsert_snapshot(**kwargs)
    await session.commit()
    assert a.id == b.id


@pytest.mark.asyncio
async def test_paper_and_strategy_insert_are_idempotent(session: AsyncSession) -> None:
    tokens = TokenRepository(session)
    token = await tokens.upsert(
        chain="solana",
        token_address="Mint222",
        symbol="PEPE",
        created_time=None,
        first_seen_time=datetime(2024, 1, 1, tzinfo=UTC),
        source="demo",
    )
    paper = PaperTradeRepository(session)
    kwargs = {
        "run_id": "00000000-0000-0000-0000-000000000001",
        "token_id": token.id,
        "signal_ts": datetime(2024, 1, 1, tzinfo=UTC),
        "side": Side.BUY,
        "requested_qty": Decimal("1"),
        "requested_price": Decimal("1.01"),
        "fill_price": Decimal("1.01"),
        "fill_qty": Decimal("1"),
        "fee": Decimal("0.01"),
        "slippage_bps": Decimal("25"),
        "status": PaperStatus.FILLED,
        "reject_reason": None,
    }
    row_a = await paper.insert(**kwargs)
    row_b = await paper.insert(**kwargs)
    # Different limit price must not collide with the prior natural key.
    row_c = await paper.insert(
        **{
            **kwargs,
            "requested_price": Decimal("1.50"),
            "fill_price": Decimal("1.50"),
        }
    )
    results = StrategyResultRepository(session)
    result_kwargs = {
        "run_id": "00000000-0000-0000-0000-000000000001",
        "strategy_name": "listing_momentum",
        "strategy_version": "1.0.0",
        "token_id": token.id,
        "params": {"lookback_minutes": 15},
        "metrics": {"return": "0.1"},
        "signals": [{"kind": "buy"}],
        "window_start": datetime(2024, 1, 1, tzinfo=UTC),
        "window_end": datetime(2024, 1, 2, tzinfo=UTC),
    }
    saved_a = await results.insert(**result_kwargs)
    saved_b = await results.insert(**result_kwargs)
    # Nullable token_id duplicates must collide under NULLS NOT DISTINCT.
    null_kwargs = {
        **result_kwargs,
        "token_id": None,
        "window_start": None,
        "window_end": None,
    }
    null_a = await results.insert(**null_kwargs)
    null_b = await results.insert(**null_kwargs)
    await session.commit()
    assert row_a.id == row_b.id
    assert row_c.id != row_a.id
    assert row_a.mode == "paper"
    assert saved_a.id == saved_b.id
    assert null_a.id == null_b.id
    assert saved_a.strategy_name == "listing_momentum"
