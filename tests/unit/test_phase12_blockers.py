"""Failing-first coverage for the six authorized Phase 1/2 blockers."""

from __future__ import annotations

import inspect
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, get_type_hints

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from newcoin_trader.collectors.binance.client import BinanceClient
from newcoin_trader.collectors.binance.normalize import normalize_agg_trade, normalize_recent_trade
from newcoin_trader.collectors.http import AsyncHttpClient, GetJsonClient
from newcoin_trader.config import Settings
from newcoin_trader.database.models import Trade
from newcoin_trader.database.repositories.market import trade_upsert_statement
from newcoin_trader.domain.enums import Side
from newcoin_trader.domain.market import Kline, PoolSnapshot, TradeTick
from newcoin_trader.errors import ConfigError, ParseError
from newcoin_trader.execution.paper_broker import PaperBroker
from newcoin_trader.services.ingestion import (
    INGEST_BINANCE_LIMIT_MAX,
    INGEST_CONTROL_MIN,
    INGEST_GECKO_OHLCV_LIMIT_MAX,
    INGEST_RAYDIUM_PAGE_MAX,
    INGEST_RAYDIUM_PAGE_SIZE_MAX,
    MarketHistoryResult,
    MarketHistoryService,
    validate_ingest_market_history_controls,
)

# --- 1) Overlapping trade fallback constraint ---


def test_trade_fallback_unique_is_partial_null_external_id() -> None:
    idx = next(i for i in Trade.__table__.indexes if i.name == "uq_trades_composite_fallback")
    assert idx.unique is True
    where = idx.dialect_options.get("postgresql", {}).get("where")
    assert where is not None
    assert "external_trade_id IS NULL" in str(where).replace('"', "")
    # Must not be a table-level UniqueConstraint (those apply even when external id is set).
    constraint_names = {c.name for c in Trade.__table__.constraints if c.name}
    assert "uq_trades_composite_fallback" not in constraint_names
    ddl = str(CreateIndex(idx).compile(dialect=postgresql.dialect())).lower()
    assert "where" in ddl
    assert "external_trade_id is null" in ddl.replace('"', "")
    stmt = trade_upsert_statement(
        token_id=1,
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        side="buy",
        amount=Decimal("1"),
        price=Decimal("1"),
        source="fixture",
        external_trade_id=None,
    )
    sql = str(stmt.compile(dialect=postgresql.dialect())).lower()
    assert "on conflict" in sql


# --- 2) Binance endpoint ID namespaces ---


def test_binance_agg_and_recent_trade_ids_do_not_collide() -> None:
    same_id = 4242
    agg = normalize_agg_trade(
        {"a": same_id, "p": "1", "q": "1", "T": 1704067260000, "m": False},
        symbol="NEWUSDT",
    )
    recent = normalize_recent_trade(
        {
            "id": same_id,
            "price": "1",
            "qty": "1",
            "time": 1704067260000,
            "isBuyerMaker": False,
        },
        symbol="NEWUSDT",
    )
    assert agg.external_trade_id is not None
    assert recent.external_trade_id is not None
    # Same numeric venue ID must not share (source, external_trade_id) identity.
    assert (agg.source, agg.external_trade_id) != (recent.source, recent.external_trade_id)
    assert "agg" in agg.source.lower() or "agg" in (agg.external_trade_id or "").lower()
    assert "trade" in recent.source.lower() or recent.source.endswith(":trades")


# --- 3) Collector transport structurally GET-only ---


def test_collector_http_exposes_get_only_interface() -> None:
    assert hasattr(AsyncHttpClient, "get_json")
    assert not hasattr(AsyncHttpClient, "request")
    assert not hasattr(AsyncHttpClient, "post")
    assert not hasattr(AsyncHttpClient, "put")
    assert not hasattr(AsyncHttpClient, "delete")
    assert not hasattr(AsyncHttpClient, "patch")
    hints = get_type_hints(BinanceClient.__init__)
    http_annotation = hints.get("http")
    assert http_annotation is GetJsonClient or getattr(http_annotation, "__name__", "") == "GetJsonClient"


@pytest.mark.asyncio
async def test_collector_http_cannot_invoke_post_via_public_api() -> None:
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        return httpx.Response(200, json={"ok": True})

    client = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    await client.get_json("https://example.test/v1")
    assert methods == ["GET"]
    assert not hasattr(client, "request")
    source = inspect.getsource(AsyncHttpClient)
    assert "def request(" not in source
    assert 'method="GET"' in source or 'method = "GET"' in source or '"GET"' in source
    await client.aclose()


# --- 4) Non-finite broker/settings validation ---


@pytest.mark.parametrize("bad", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")])
def test_paper_broker_rejects_non_finite_config(bad: Decimal) -> None:
    with pytest.raises(ConfigError):
        PaperBroker(fee_bps=bad)
    with pytest.raises(ConfigError):
        PaperBroker(slippage_bps=bad)
    with pytest.raises(ConfigError):
        PaperBroker(max_fill_liquidity_fraction=bad)


@pytest.mark.parametrize(
    ("env_key", "raw"),
    [
        ("PAPER_FEE_BPS", "nan"),
        ("PAPER_SLIPPAGE_BPS", "inf"),
        ("PAPER_MAX_FILL_LIQUIDITY_FRACTION", "-inf"),
        ("RISK_MAX_NOTIONAL", "NaN"),
        ("HTTP_TIMEOUT_SECONDS", "Infinity"),
    ],
)
def test_settings_reject_non_finite_numbers(
    monkeypatch: pytest.MonkeyPatch,
    env_key: str,
    raw: str,
) -> None:
    monkeypatch.setenv(env_key, raw)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


# --- 5) Malformed Binance 200 payloads ---


@pytest.mark.asyncio
async def test_binance_malformed_success_payloads_raise_parse_error() -> None:
    responses: dict[str, Any] = {
        "/api/v3/klines": {"not": "a list"},
        "/api/v3/aggTrades": {"oops": True},
        "/api/v3/trades": "nope",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=responses[request.url.path])

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    client = BinanceClient(http=http, base_url="https://api.binance.com")
    with pytest.raises(ParseError):
        await client.klines("NEWUSDT", interval="1h")
    with pytest.raises(ParseError):
        await client.agg_trades("NEWUSDT")
    with pytest.raises(ParseError):
        await client.recent_trades("NEWUSDT")
    await http.aclose()


@pytest.mark.asyncio
async def test_binance_wrong_element_types_and_missing_fields_raise_parse_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/klines"):
            return httpx.Response(200, json=[{"bad": "row"}])
        if request.url.path.endswith("/aggTrades"):
            return httpx.Response(200, json=[{"a": 1, "p": "1"}])  # missing q/T
        return httpx.Response(200, json=[{"id": 1, "price": "1"}])  # missing qty/time

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    client = BinanceClient(http=http, base_url="https://api.binance.com")
    with pytest.raises(ParseError):
        await client.klines("NEWUSDT", interval="1h")
    with pytest.raises(ParseError):
        await client.agg_trades("NEWUSDT")
    with pytest.raises(ParseError):
        await client.recent_trades("NEWUSDT")
    await http.aclose()


@pytest.mark.asyncio
async def test_binance_exchange_info_depth_ticker_malformed_raise_parse_error() -> None:
    payloads: dict[str, Any] = {
        "/api/v3/exchangeInfo": {"symbols": [{"status": "TRADING"}]},
        "/api/v3/depth": {"bids": ["not-a-level"], "asks": []},
        "/api/v3/ticker/24hr": {"symbol": "NEWUSDT", "lastPrice": "NaN", "volume": "1"},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payloads[request.url.path])

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    client = BinanceClient(http=http, base_url="https://api.binance.com")
    with pytest.raises(ParseError):
        await client.exchange_info()
    with pytest.raises(ParseError):
        await client.order_book("NEWUSDT")
    with pytest.raises(ParseError):
        await client.ticker_24h("NEWUSDT")
    await http.aclose()


@pytest.mark.asyncio
async def test_binance_malformed_numerics_and_missing_required_fields_raise_parse_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/klines"):
            return httpx.Response(
                200,
                json=[[1704067200000, "bad", "2", "0.5", "1.5", "10", 1704070799999, "11", 1]],
            )
        if request.url.path.endswith("/depth"):
            return httpx.Response(200, json={"bids": [["1.0"]], "asks": []})
        return httpx.Response(200, json={"lastPrice": "1", "volume": "1"})

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    client = BinanceClient(http=http, base_url="https://api.binance.com")
    with pytest.raises(ParseError):
        await client.klines("NEWUSDT", interval="1h")
    with pytest.raises(ParseError):
        await client.order_book("NEWUSDT")
    with pytest.raises(ParseError):
        await client.ticker_24h("NEWUSDT")
    await http.aclose()


@pytest.mark.asyncio
async def test_binance_valid_empty_lists_are_not_parse_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/exchangeInfo"):
            return httpx.Response(200, json={"serverTime": 1704067200000, "symbols": []})
        if request.url.path.endswith("/depth"):
            return httpx.Response(200, json={"bids": [], "asks": [], "lastUpdateId": 0})
        return httpx.Response(200, json=[])

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    client = BinanceClient(http=http, base_url="https://api.binance.com")
    assert await client.exchange_info() == []
    assert await client.klines("NEWUSDT", interval="1h") == []
    assert await client.agg_trades("NEWUSDT") == []
    assert await client.recent_trades("NEWUSDT") == []
    book = await client.order_book("NEWUSDT")
    assert book.bids == () and book.asks == ()
    assert book.last_update_id == 0
    await http.aclose()


# --- 6) End-to-end market-history ingestion ---


@pytest.mark.asyncio
async def test_market_history_ingestion_persists_bounded_sources() -> None:
    class FakeTokens:
        def __init__(self) -> None:
            self.rows: dict[tuple[str, str], dict[str, Any]] = {}

        async def upsert(self, **kwargs: Any) -> dict[str, Any]:
            key = (kwargs["chain"], kwargs["token_address"])
            if key not in self.rows:
                self.rows[key] = {"id": len(self.rows) + 1, **kwargs}
            return self.rows[key]

    class FakeMarket:
        def __init__(self) -> None:
            self.snapshots: list[dict[str, Any]] = []
            self.trades: list[dict[str, Any]] = []

        async def upsert_snapshot(self, **kwargs: Any) -> dict[str, Any]:
            self.snapshots.append(kwargs)
            return kwargs

        async def upsert_trade(self, **kwargs: Any) -> dict[str, Any]:
            self.trades.append(kwargs)
            return kwargs

    class FakeBinance:
        async def klines(self, symbol: str, **kwargs: Any) -> list[Kline]:
            assert symbol == "NEWUSDT"
            assert kwargs.get("limit") == 2
            return [
                Kline(
                    token_address=symbol,
                    chain="binance",
                    open_time=datetime(2024, 1, 1, tzinfo=UTC),
                    close_time=datetime(2024, 1, 1, 1, tzinfo=UTC),
                    open=Decimal("1"),
                    high=Decimal("2"),
                    low=Decimal("0.5"),
                    close=Decimal("1.5"),
                    volume=Decimal("10"),
                    interval="1h",
                    source="binance",
                )
            ]

        async def agg_trades(self, symbol: str, **kwargs: Any) -> list[TradeTick]:
            return [
                TradeTick(
                    token_address=symbol,
                    chain="binance",
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                    side=Side.BUY,
                    amount=Decimal("1"),
                    price=Decimal("1.1"),
                    external_trade_id="agg:1",
                    source="binance:aggTrades",
                )
            ]

        async def recent_trades(self, symbol: str, **kwargs: Any) -> list[TradeTick]:
            return []

    class FakeRaydium:
        async def list_pools(self, **kwargs: Any) -> list[PoolSnapshot]:
            assert kwargs.get("page_size") == 1
            return [
                PoolSnapshot(
                    pool_address="Pool111",
                    chain="solana",
                    base_mint="Base",
                    quote_mint="Quote",
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                    price=Decimal("0.01"),
                    liquidity=Decimal("1000"),
                    volume_24h=Decimal("50"),
                    source="raydium",
                )
            ]

    class FakeGecko:
        async def get_pool(self, network: str, pool_address: str) -> PoolSnapshot:
            assert network == "solana"
            return PoolSnapshot(
                pool_address=pool_address,
                chain=network,
                base_mint="B",
                quote_mint="Q",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                price=Decimal("0.02"),
                liquidity=Decimal("2000"),
                source="geckoterminal",
            )

        async def pool_ohlcv(self, network: str, pool_address: str, **kwargs: Any) -> list[Kline]:
            assert kwargs.get("limit") == 3
            return [
                Kline(
                    token_address=pool_address,
                    chain=network,
                    open_time=datetime(2024, 1, 1, tzinfo=UTC),
                    close_time=datetime(2024, 1, 1, tzinfo=UTC),
                    open=Decimal("0.02"),
                    high=Decimal("0.03"),
                    low=Decimal("0.01"),
                    close=Decimal("0.025"),
                    volume=Decimal("9"),
                    interval="1m",
                    source="geckoterminal",
                )
            ]

    tokens = FakeTokens()
    market = FakeMarket()
    service = MarketHistoryService(
        binance=FakeBinance(),
        raydium=FakeRaydium(),
        gecko=FakeGecko(),
        tokens=tokens,
        market=market,
    )
    result = await service.ingest_market_history(
        binance_symbol="NEWUSDT",
        binance_interval="1h",
        binance_limit=2,
        raydium_page_size=1,
        gecko_network="solana",
        gecko_pool="GeckoPool",
        gecko_ohlcv_limit=3,
    )
    assert isinstance(result, MarketHistoryResult)
    assert result.snapshots >= 3
    assert result.trades >= 1
    assert result.pools >= 1
    assert "binance" in result.by_source
    assert "raydium" in result.by_source
    assert "geckoterminal" in result.by_source
    assert market.snapshots
    assert market.trades


def test_ingest_market_history_control_bounds_are_exported() -> None:
    assert INGEST_CONTROL_MIN == 1
    assert INGEST_BINANCE_LIMIT_MAX == 1000
    assert INGEST_RAYDIUM_PAGE_MAX == 100
    assert INGEST_RAYDIUM_PAGE_SIZE_MAX == 100
    assert INGEST_GECKO_OHLCV_LIMIT_MAX == 1000
    with pytest.raises(ConfigError):
        validate_ingest_market_history_controls(
            binance_limit=0,
            raydium_page=1,
            raydium_page_size=None,
            gecko_ohlcv_limit=1,
        )
