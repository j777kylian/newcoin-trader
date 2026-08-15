"""Ingestion orchestration with injected fake collectors/repositories."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from newcoin_trader.domain.enums import Chain, Side, Venue
from newcoin_trader.domain.market import PriceSnapshot, TradeTick
from newcoin_trader.domain.tokens import NewListingEvent
from newcoin_trader.services.ingestion import CollectOnceResult, IngestionService, PollController


class FakeTokenRepo:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.calls = 0

    async def upsert(
        self,
        *,
        chain: str,
        token_address: str,
        symbol: str,
        created_time: datetime | None,
        first_seen_time: datetime,
        source: str,
        venue: str | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.calls += 1
        key = (chain, token_address)
        existing = self.rows.get(key)
        if existing is None:
            row = {
                "id": len(self.rows) + 1,
                "chain": chain,
                "token_address": token_address,
                "symbol": symbol,
                "created_time": created_time,
                "first_seen_time": first_seen_time,
                "source": source,
                "venue": venue,
                "metadata_json": metadata_json,
            }
            self.rows[key] = row
            return row
        if first_seen_time < existing["first_seen_time"]:
            existing["first_seen_time"] = first_seen_time
        existing["symbol"] = symbol
        return existing


class FakeMarketRepo:
    def __init__(self) -> None:
        self.snapshots: list[dict[str, Any]] = []
        self.trades: list[dict[str, Any]] = []

    async def upsert_snapshot(
        self,
        *,
        token_id: int,
        timestamp: datetime,
        price: Decimal,
        volume: Decimal | None,
        liquidity: Decimal | None,
        market_cap: Decimal | None,
        buy_count: int | None,
        sell_count: int | None,
        source: str,
        provenance: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        key = (token_id, timestamp, source)
        for row in self.snapshots:
            if (row["token_id"], row["timestamp"], row["source"]) == key:
                return row
        row = {
            "id": len(self.snapshots) + 1,
            "token_id": token_id,
            "timestamp": timestamp,
            "price": price,
            "volume": volume,
            "liquidity": liquidity,
            "market_cap": market_cap,
            "buy_count": buy_count,
            "sell_count": sell_count,
            "source": source,
            "provenance": provenance,
        }
        self.snapshots.append(row)
        return row

    async def upsert_trade(
        self,
        *,
        token_id: int,
        timestamp: datetime,
        side: str,
        amount: Decimal,
        price: Decimal,
        source: str,
        external_trade_id: str | None = None,
        provenance: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if external_trade_id:
            for row in self.trades:
                if row.get("external_trade_id") == external_trade_id and row["source"] == source:
                    return row
        row = {
            "id": len(self.trades) + 1,
            "token_id": token_id,
            "timestamp": timestamp,
            "side": side,
            "amount": amount,
            "price": price,
            "source": source,
            "external_trade_id": external_trade_id,
            "provenance": provenance,
        }
        self.trades.append(row)
        return row


class FakeBinance:
    def __init__(self, events: list[NewListingEvent]) -> None:
        self.events = events
        self.calls = 0

    async def exchange_info(self) -> list[NewListingEvent]:
        self.calls += 1
        return list(self.events)


class FakeBirdeye:
    def __init__(
        self,
        tokens: list[NewListingEvent],
        pairs: list[NewListingEvent],
    ) -> None:
        self.tokens = tokens
        self.pairs = pairs
        self.token_calls = 0
        self.pair_calls = 0

    async def discover_new_tokens(
        self,
        *,
        limit: int = 10,
        time_to: int | None = None,
        meme_platform_enabled: bool = False,
    ) -> list[NewListingEvent]:
        self.token_calls += 1
        return list(self.tokens)

    async def discover_new_pairs(
        self,
        *,
        limit: int = 10,
        offset: int = 0,
    ) -> list[NewListingEvent]:
        self.pair_calls += 1
        return list(self.pairs)


def _listing(
    *,
    address: str,
    chain: Chain,
    symbol: str,
    source: str,
) -> NewListingEvent:
    return NewListingEvent(
        token_address=address,
        chain=chain,
        symbol=symbol,
        created_time=datetime(2024, 1, 1, tzinfo=UTC),
        first_seen_time=datetime(2024, 1, 1, tzinfo=UTC),
        source=source,
        venue=Venue.BINANCE if chain is Chain.BINANCE else Venue.BIRDEYE,
    )


@pytest.mark.asyncio
async def test_collect_once_discovers_and_upserts_idempotently() -> None:
    tokens = FakeTokenRepo()
    market = FakeMarketRepo()
    binance = FakeBinance([_listing(address="NEWUSDT", chain=Chain.BINANCE, symbol="NEWUSDT", source="binance")])
    birdeye = FakeBirdeye(
        tokens=[_listing(address="MintA", chain=Chain.SOLANA, symbol="AAA", source="birdeye")],
        pairs=[_listing(address="MintB", chain=Chain.SOLANA, symbol="BBB", source="birdeye")],
    )
    service = IngestionService(
        binance=binance,
        birdeye=birdeye,
        tokens=tokens,
        market=market,
    )
    first = await service.collect_once()
    second = await service.collect_once()
    assert isinstance(first, CollectOnceResult)
    assert first.discovered == 3
    assert first.upserted == 3
    assert second.discovered == 3
    assert len(tokens.rows) == 3
    assert tokens.calls == 6
    assert binance.calls == 2
    assert birdeye.token_calls == 2
    assert birdeye.pair_calls == 2


@pytest.mark.asyncio
async def test_ingest_snapshots_and_trades_are_idempotent() -> None:
    tokens = FakeTokenRepo()
    market = FakeMarketRepo()
    service = IngestionService(
        binance=FakeBinance([]),
        birdeye=FakeBirdeye([], []),
        tokens=tokens,
        market=market,
    )
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    snaps = [
        PriceSnapshot(
            token_address="MintA",
            chain="solana",
            timestamp=ts,
            price=Decimal("1.1"),
            volume=Decimal("10"),
            liquidity=Decimal("1000"),
            market_cap=Decimal("5000"),
            buy_count=1,
            sell_count=2,
            source="fixture",
        )
    ]
    trades = [
        TradeTick(
            token_address="MintA",
            chain="solana",
            timestamp=ts,
            side=Side.BUY,
            amount=Decimal("2"),
            price=Decimal("1.1"),
            external_trade_id="t1",
            source="fixture",
        )
    ]
    n1 = await service.ingest_snapshots(token_id=7, snapshots=snaps)
    n2 = await service.ingest_snapshots(token_id=7, snapshots=snaps)
    t1 = await service.ingest_trades(token_id=7, trades=trades)
    t2 = await service.ingest_trades(token_id=7, trades=trades)
    assert n1 == 1 and n2 == 1
    assert t1 == 1 and t2 == 1
    assert len(market.snapshots) == 1
    assert len(market.trades) == 1


@pytest.mark.asyncio
async def test_poll_controller_respects_stop_and_max_iterations() -> None:
    tokens = FakeTokenRepo()
    service = IngestionService(
        binance=FakeBinance([_listing(address="NEWUSDT", chain=Chain.BINANCE, symbol="NEWUSDT", source="binance")]),
        birdeye=FakeBirdeye([], []),
        tokens=tokens,
        market=FakeMarketRepo(),
    )
    controller = PollController(interval_seconds=0.01, max_iterations=2)
    results = await controller.run(service)
    assert len(results) == 2
    assert controller.stopped is True


@pytest.mark.asyncio
async def test_poll_controller_can_be_stopped_externally() -> None:
    tokens = FakeTokenRepo()
    service = IngestionService(
        binance=FakeBinance([]),
        birdeye=FakeBirdeye([], []),
        tokens=tokens,
        market=FakeMarketRepo(),
    )
    stop = asyncio.Event()
    controller = PollController(interval_seconds=60.0, stop_event=stop, max_iterations=100)

    async def stopper() -> None:
        await asyncio.sleep(0.01)
        stop.set()

    task = asyncio.create_task(controller.run(service))
    await stopper()
    results = await asyncio.wait_for(task, timeout=1.0)
    assert controller.stopped is True
    assert len(results) >= 1
