"""Offline mocked-HTTP market-history ingestion through concrete collectors."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest

from newcoin_trader.collectors.binance.client import BinanceClient
from newcoin_trader.collectors.gecko.client import GeckoTerminalClient
from newcoin_trader.collectors.http import AsyncHttpClient
from newcoin_trader.collectors.raydium.client import RaydiumClient
from newcoin_trader.errors import ConfigError
from newcoin_trader.services.ingestion import (
    INGEST_BINANCE_LIMIT_MAX,
    INGEST_CONTROL_MIN,
    INGEST_GECKO_OHLCV_LIMIT_MAX,
    INGEST_RAYDIUM_PAGE_MAX,
    INGEST_RAYDIUM_PAGE_SIZE_MAX,
    MarketHistoryService,
    validate_ingest_market_history_controls,
)

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "fixtures"


def _load(rel: str) -> object:
    return json.loads((FIXTURES / rel).read_text(encoding="utf-8"))


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


def _stack(
    handler: Any,
    *,
    max_attempts: int = 1,
) -> tuple[AsyncHttpClient, MarketHistoryService, FakeTokenRepo, FakeMarketRepo]:
    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=max_attempts,
        rate_limit_per_second=1000.0,
        sleep=_no_sleep,
    )
    tokens = FakeTokenRepo()
    market = FakeMarketRepo()
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
    return http, service, tokens, market


async def _no_sleep(_seconds: float) -> None:
    return None


@pytest.mark.asyncio
async def test_binance_mocked_http_persists_identities_and_timestamps() -> None:
    http, service, tokens, market = _stack(_fixture_handler)
    result = await service.ingest_market_history(
        binance_symbol="NEWUSDT",
        binance_interval="1h",
        binance_limit=2,
        include_binance_recent_trades=True,
    )
    await http.aclose()
    assert result.by_source["binance"] >= 2
    assert ("binance", "NEWUSDT") in tokens.rows
    token_id = tokens.rows[("binance", "NEWUSDT")]["id"]
    kline_close = datetime.fromtimestamp(1704070799999 / 1000, tz=UTC)
    trade_ts = datetime.fromtimestamp(1704067260000 / 1000, tz=UTC)
    snaps = [row for row in market.snapshots if row["source"] == "binance"]
    assert snaps[0]["token_id"] == token_id
    assert snaps[0]["timestamp"] == kline_close
    assert snaps[0]["price"] == Decimal("1.10000000")
    agg = next(row for row in market.trades if row["source"] == "binance:aggTrades")
    recent = next(row for row in market.trades if row["source"] == "binance:trades")
    assert agg["external_trade_id"] == "26129"
    assert agg["timestamp"] == trade_ts
    assert recent["external_trade_id"] == "28457"
    assert recent["timestamp"] == trade_ts
    assert agg["token_id"] == token_id == recent["token_id"]


@pytest.mark.asyncio
async def test_raydium_mocked_http_persists_pool_identity() -> None:
    http, service, tokens, market = _stack(_fixture_handler)
    result = await service.ingest_market_history(raydium_page=1, raydium_page_size=1)
    await http.aclose()
    pool_id = "Pool1111111111111111111111111111111111111"
    assert result.pools == 1
    assert result.by_source["raydium"] == 1
    assert ("solana", pool_id) in tokens.rows
    assert tokens.rows[("solana", pool_id)]["source"] == "raydium"
    snap = next(row for row in market.snapshots if row["source"] == "raydium")
    assert snap["price"] == Decimal("0.00125")
    assert snap["liquidity"] == Decimal("25000.5")
    assert snap["volume"] == Decimal("4200.1")


@pytest.mark.asyncio
async def test_gecko_mocked_http_persists_pool_and_ohlcv() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/ohlcv/minute"):
            payload = _load("gecko/ohlcv.json")
            assert isinstance(payload, dict)
            payload["data"]["attributes"]["ohlcv_list"][0][0] = 1704067260
            return httpx.Response(200, json=payload)
        return _fixture_handler(request)

    http, service, tokens, market = _stack(handler)
    pool = "PoolAddress111111111111111111111111111"
    result = await service.ingest_market_history(
        gecko_network="solana",
        gecko_pool=pool,
        gecko_ohlcv_limit=3,
    )
    await http.aclose()
    assert result.pools == 1
    assert "geckoterminal" in result.by_source
    assert ("solana", pool) in tokens.rows
    assert tokens.rows[("solana", pool)]["source"] == "geckoterminal"
    sources = {row["source"] for row in market.snapshots}
    assert sources == {"geckoterminal:pool", "geckoterminal:ohlcv:1m"}
    token_id = tokens.rows[("solana", pool)]["id"]
    pool_snap = next(row for row in market.snapshots if row["liquidity"] is not None)
    ohlcv_snap = next(row for row in market.snapshots if row["liquidity"] is None)
    assert pool_snap["token_id"] == token_id == ohlcv_snap["token_id"]
    assert pool_snap["price"] == Decimal("0.00125")
    assert pool_snap["timestamp"] == datetime(2024, 1, 1, tzinfo=UTC)
    assert ohlcv_snap["price"] == Decimal("0.0011")
    assert ohlcv_snap["timestamp"] == datetime.fromtimestamp(1704067260, tz=UTC)
    assert ohlcv_snap["volume"] == Decimal("1000.5")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fail_path", "ingest_kwargs", "source_key"),
    [
        (
            "/api/v3/klines",
            {
                "binance_symbol": "NEWUSDT",
                "binance_interval": "1h",
                "binance_limit": 2,
                "include_binance_recent_trades": True,
            },
            "binance",
        ),
        (
            "/pools/info/list",
            {"raydium_page": 1, "raydium_page_size": 1},
            "raydium",
        ),
        (
            "/api/v2/networks/solana/pools/PoolAddress111111111111111111111111111",
            {
                "gecko_network": "solana",
                "gecko_pool": "PoolAddress111111111111111111111111111",
                "gecko_ohlcv_limit": 3,
            },
            "geckoterminal",
        ),
    ],
)
async def test_transient_failure_then_success_per_venue(
    fail_path: str,
    ingest_kwargs: dict[str, Any],
    source_key: str,
) -> None:
    hits: dict[str, int] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        return _fixture_handler(request, hits=hits, fail_first_path=fail_path)

    http, service, tokens, market = _stack(handler, max_attempts=3)
    result = await service.ingest_market_history(**ingest_kwargs)
    await http.aclose()
    assert hits.get(fail_path, 0) >= 2
    assert source_key in result.by_source
    assert result.by_source[source_key] >= 1
    assert tokens.rows
    assert market.snapshots or market.trades


@pytest.mark.asyncio
async def test_raydium_reingest_identical_payload_is_idempotent() -> None:
    http, service, tokens, market = _stack(_fixture_handler)
    kwargs = {"raydium_page": 1, "raydium_page_size": 1}
    first = await service.ingest_market_history(**kwargs)
    first_ts = [row["timestamp"] for row in market.snapshots if row["source"] == "raydium"]
    assert len(first_ts) == 1
    second = await service.ingest_market_history(**kwargs)
    await http.aclose()
    raydium_rows = [row for row in market.snapshots if row["source"] == "raydium"]
    assert first.snapshots == second.snapshots == 1
    assert len(raydium_rows) == 1
    assert raydium_rows[0]["timestamp"] == first_ts[0]
    assert tokens.calls == 2
    assert len(tokens.rows) == 1


@pytest.mark.asyncio
async def test_mocked_http_retry_then_idempotent_reingest() -> None:
    hits: dict[str, int] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        return _fixture_handler(request, hits=hits, fail_first_path="/api/v3/klines")

    http, service, tokens, market = _stack(handler, max_attempts=3)
    kwargs = {
        "binance_symbol": "NEWUSDT",
        "binance_interval": "1h",
        "binance_limit": 2,
        "include_binance_recent_trades": True,
        "raydium_page": 1,
        "raydium_page_size": 1,
        "gecko_network": "solana",
        "gecko_pool": "PoolAddress111111111111111111111111111",
        "gecko_ohlcv_limit": 3,
    }
    first = await service.ingest_market_history(**kwargs)
    snap_n = len(market.snapshots)
    trade_n = len(market.trades)
    token_n = len(tokens.rows)
    second = await service.ingest_market_history(**kwargs)
    await http.aclose()
    assert hits["/api/v3/klines"] >= 2
    assert first.snapshots == second.snapshots
    assert first.trades == second.trades
    assert len(market.snapshots) == snap_n
    assert len(market.trades) == trade_n
    assert len(tokens.rows) == token_n


@pytest.mark.parametrize(
    "kwargs",
    [
        {"binance_symbol": "NEWUSDT", "binance_limit": 0},
        {"binance_symbol": "NEWUSDT", "binance_limit": -1},
        {"binance_symbol": "NEWUSDT", "binance_limit": INGEST_BINANCE_LIMIT_MAX + 1},
        {"raydium_page_size": 1, "raydium_page": 0},
        {"raydium_page_size": 1, "raydium_page": INGEST_RAYDIUM_PAGE_MAX + 1},
        {"raydium_page_size": 0},
        {"raydium_page_size": -5},
        {"raydium_page_size": INGEST_RAYDIUM_PAGE_SIZE_MAX + 1},
        {"gecko_network": "solana", "gecko_pool": "x", "gecko_ohlcv_limit": 0},
        {"gecko_ohlcv_limit": INGEST_GECKO_OHLCV_LIMIT_MAX + 1},
        {"binance_limit": True},
    ],
)
@pytest.mark.asyncio
async def test_ingest_controls_rejected_before_any_http(kwargs: dict[str, Any]) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        raise AssertionError(f"HTTP started: {request.url}")

    http, service, tokens, market = _stack(handler)
    with pytest.raises(ConfigError):
        await service.ingest_market_history(**kwargs)
    await http.aclose()
    assert calls == []
    assert tokens.calls == 0
    assert market.snapshots == []
    assert market.trades == []


def test_validate_controls_documents_inclusive_bounds() -> None:
    validate_ingest_market_history_controls(
        binance_limit=INGEST_CONTROL_MIN,
        raydium_page=INGEST_CONTROL_MIN,
        raydium_page_size=None,
        gecko_ohlcv_limit=INGEST_CONTROL_MIN,
    )
    validate_ingest_market_history_controls(
        binance_limit=INGEST_BINANCE_LIMIT_MAX,
        raydium_page=INGEST_RAYDIUM_PAGE_MAX,
        raydium_page_size=INGEST_RAYDIUM_PAGE_SIZE_MAX,
        gecko_ohlcv_limit=INGEST_GECKO_OHLCV_LIMIT_MAX,
    )
    with pytest.raises(ConfigError):
        validate_ingest_market_history_controls(
            binance_limit=0,
            raydium_page=1,
            raydium_page_size=None,
            gecko_ohlcv_limit=1,
        )
