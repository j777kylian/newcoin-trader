"""Binance collector unit tests using fixture/mock transport only."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from newcoin_trader.collectors.binance.client import BinanceClient
from newcoin_trader.collectors.binance.normalize import (
    normalize_agg_trade,
    normalize_exchange_info,
    normalize_kline,
    normalize_order_book,
    normalize_recent_trade,
    normalize_ticker_24h,
)
from newcoin_trader.collectors.http import AsyncHttpClient
from newcoin_trader.collectors.normalization import parse_int, parse_required_venue_time
from newcoin_trader.domain.enums import Chain, Side
from newcoin_trader.errors import ParseError

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures" / "binance"

VALID_KLINE = [
    1704067200000,
    "1.00000000",
    "1.25000000",
    "0.90000000",
    "1.10000000",
    "100.00000000",
    1704070799999,
    "110.00000000",
    42,
    "50.00000000",
    "55.00000000",
    "0",
]


def _load(name: str) -> object:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_normalize_exchange_info_trading_pairs() -> None:
    events = normalize_exchange_info(_load("exchange_info.json"))
    assert len(events) == 1
    event = events[0]
    assert event.symbol == "NEWUSDT"
    assert event.token_address == "NEWUSDT"
    assert event.chain is Chain.BINANCE
    assert event.created_time == datetime(2024, 1, 1, tzinfo=UTC)
    assert event.first_seen_time == datetime(2024, 1, 1, tzinfo=UTC)


def test_normalize_kline_uses_decimal_and_utc() -> None:
    kline = normalize_kline(_load("klines.json")[0], symbol="NEWUSDT", interval="1h")
    assert kline.open == Decimal("1.00000000")
    assert kline.close == Decimal("1.10000000")
    assert kline.open_time.tzinfo is not None
    assert kline.volume == Decimal("100.00000000")


def test_normalize_agg_trade_buyer_maker_is_sell() -> None:
    trade = normalize_agg_trade(
        {"a": 1, "p": "2.5", "q": "3", "T": 1704067260000, "m": True},
        symbol="NEWUSDT",
    )
    assert trade.side is Side.SELL
    assert trade.external_trade_id == "1"
    assert trade.source == "binance:aggTrades"
    assert trade.price == Decimal("2.5")


# --- Blocker 5: shared strict helpers ---


@pytest.mark.parametrize(
    "bad",
    [1.5, 2.7, Decimal("1.5"), True, False, None, "1.5", "nan", float("nan"), float("inf")],
)
def test_parse_int_rejects_fractional_bool_null_nonfinite(bad: object) -> None:
    with pytest.raises(ParseError):
        parse_int(bad, context="test")


@pytest.mark.parametrize("good", [0, 1, 42, "7", "100"])
def test_parse_int_accepts_integral_scalars(good: object) -> None:
    assert isinstance(parse_int(good, context="test"), int)


@pytest.mark.parametrize(
    "bad",
    [None, True, False, 0, -1, -100, float("nan"), float("inf"), float("-inf"), "0", "nope"],
)
def test_parse_required_venue_time_rejects_invalid(bad: object) -> None:
    with pytest.raises(ParseError):
        parse_required_venue_time(bad, context="test")


def test_parse_required_venue_time_accepts_positive_integral_millis() -> None:
    assert parse_required_venue_time(1704067200000, context="test") == datetime(2024, 1, 1, tzinfo=UTC)


# --- Blocker 5: exchangeInfo ---


@pytest.mark.parametrize(
    "payload",
    [
        {"symbols": []},  # missing serverTime
        {"serverTime": None, "symbols": []},
        {"serverTime": 0, "symbols": []},
        {"serverTime": -1, "symbols": []},
        {"serverTime": 1.5, "symbols": []},
        {"serverTime": True, "symbols": []},
        {"serverTime": "now", "symbols": []},
        {"serverTime": 1704067200000, "symbols": [{"symbol": "X"}]},  # missing status
        {"serverTime": 1704067200000, "symbols": [{"status": "TRADING"}]},  # missing symbol
        {"serverTime": 1704067200000, "symbols": [{"status": "TRADING", "symbol": None}]},
        {"serverTime": 1704067200000, "symbols": [{"status": "TRADING", "symbol": ""}]},
        {"serverTime": 1704067200000, "symbols": [{"status": "TRADING", "symbol": 123}]},
        {"serverTime": 1704067200000, "symbols": [None]},
    ],
)
def test_exchange_info_strict_rules_raise_parse_error(payload: object) -> None:
    with pytest.raises(ParseError):
        normalize_exchange_info(payload)


def test_exchange_info_malformed_symbol_does_not_become_empty() -> None:
    with pytest.raises(ParseError):
        normalize_exchange_info(
            {
                "serverTime": 1704067200000,
                "symbols": [{"status": "TRADING", "symbol": "OK"}, {"symbol": "BAD"}],
            }
        )


def test_exchange_info_empty_symbols_with_server_time_ok() -> None:
    assert normalize_exchange_info({"serverTime": 1704067200000, "symbols": []}) == []


def test_exchange_info_break_with_null_symbol_raises_parse_error() -> None:
    """Identity/shape is validated before status filtering; BREAK + null symbol is malformed."""
    with pytest.raises(ParseError):
        normalize_exchange_info(
            {
                "serverTime": 1704067200000,
                "symbols": [{"status": "BREAK", "symbol": None}],
            }
        )


def test_exchange_info_break_with_valid_symbol_is_filtered() -> None:
    assert (
        normalize_exchange_info(
            {
                "serverTime": 1704067200000,
                "symbols": [{"status": "BREAK", "symbol": "OLDUSDT"}],
            }
        )
        == []
    )


# --- Blocker 5: klines ---


@pytest.mark.parametrize(
    "row",
    [
        VALID_KLINE[:9],  # too short
        VALID_KLINE + ["extra"],  # too long
        [1704067200000.5, *VALID_KLINE[1:]],  # fractional open time
        [*VALID_KLINE[:8], 1.5, *VALID_KLINE[9:]],  # fractional count
        [*VALID_KLINE[:8], True, *VALID_KLINE[9:]],
        [*VALID_KLINE[:8], None, *VALID_KLINE[9:]],
        [None, *VALID_KLINE[1:]],
        [*VALID_KLINE[:1], "0", *VALID_KLINE[2:]],  # non-positive open
        [*VALID_KLINE[:1], "-1", *VALID_KLINE[2:]],
        [*VALID_KLINE[:5], "-1", *VALID_KLINE[6:]],  # negative volume
        [*VALID_KLINE[:8], -1, *VALID_KLINE[9:]],  # negative count
        [
            1704070800000,
            *VALID_KLINE[1:6],
            1704067200000,
            *VALID_KLINE[7:],
        ],  # close < open
        [*VALID_KLINE[:1], "NaN", *VALID_KLINE[2:]],
        [*VALID_KLINE[:1], "Infinity", *VALID_KLINE[2:]],
        {"bad": "row"},
    ],
)
def test_kline_strict_rules_raise_parse_error(row: object) -> None:
    with pytest.raises(ParseError):
        normalize_kline(row, symbol="NEWUSDT", interval="1h")


def test_kline_exact_supported_shape_ok() -> None:
    kline = normalize_kline(VALID_KLINE, symbol="NEWUSDT", interval="1h")
    assert kline.trade_count == 42
    assert kline.close_time >= kline.open_time


# --- Blocker 5: aggTrades / trades ---


@pytest.mark.parametrize(
    "payload",
    [
        {"p": "1", "q": "1", "T": 1704067260000, "m": False},  # missing id
        {"a": None, "p": "1", "q": "1", "T": 1704067260000, "m": False},
        {"a": True, "p": "1", "q": "1", "T": 1704067260000, "m": False},
        {"a": 1.5, "p": "1", "q": "1", "T": 1704067260000, "m": False},
        {"a": 1, "p": "1", "q": "1", "T": 1704067260000},  # missing m
        {"a": 1, "p": "1", "q": "1", "T": 1704067260000, "m": "true"},
        {"a": 1, "p": "1", "q": "1", "T": 1704067260000, "m": 1},
        {"a": 1, "p": "1", "q": "1", "T": 1704067260000, "m": None},
        {"a": 1, "p": "0", "q": "1", "T": 1704067260000, "m": False},
        {"a": 1, "p": "-1", "q": "1", "T": 1704067260000, "m": False},
        {"a": 1, "p": "1", "q": "0", "T": 1704067260000, "m": False},
        {"a": 1, "p": "1", "q": "-1", "T": 1704067260000, "m": False},
        {"a": 1, "p": "1", "q": "1", "T": 0, "m": False},
        {"a": 1, "p": "1", "q": "1", "T": -1, "m": False},
        {"a": 1, "p": "1", "q": "1", "T": 1.5, "m": False},
        {"a": 1, "p": "bad", "q": "1", "T": 1704067260000, "m": False},
        {"a": 1, "p": "NaN", "q": "1", "T": 1704067260000, "m": False},
    ],
)
def test_agg_trade_strict_rules_raise_parse_error(payload: object) -> None:
    with pytest.raises(ParseError):
        normalize_agg_trade(payload, symbol="NEWUSDT")


def test_agg_trade_null_id_does_not_become_none_string() -> None:
    with pytest.raises(ParseError):
        normalize_agg_trade(
            {"a": None, "p": "1", "q": "1", "T": 1704067260000, "m": False},
            symbol="NEWUSDT",
        )


@pytest.mark.parametrize(
    "bad_id",
    [-1, "-1", 1.5, "1.5", True, False, None, "", "abc"],
)
def test_agg_trade_id_must_be_nonnegative_integral(bad_id: object) -> None:
    with pytest.raises(ParseError):
        normalize_agg_trade(
            {"a": bad_id, "p": "1", "q": "1", "T": 1704067260000, "m": False},
            symbol="NEWUSDT",
        )


@pytest.mark.parametrize(
    ("good_id", "expected"),
    [(0, "0"), (1, "1"), (26129, "26129"), ("0", "0"), ("7", "7")],
)
def test_agg_trade_id_preserves_valid_nonnegative(good_id: object, expected: str) -> None:
    trade = normalize_agg_trade(
        {"a": good_id, "p": "1", "q": "1", "T": 1704067260000, "m": False},
        symbol="NEWUSDT",
    )
    assert trade.external_trade_id == expected


@pytest.mark.parametrize(
    "payload",
    [
        {"price": "1", "qty": "1", "time": 1704067260000, "isBuyerMaker": False},
        {"id": None, "price": "1", "qty": "1", "time": 1704067260000, "isBuyerMaker": False},
        {"id": 1, "price": "1", "qty": "1", "time": 1704067260000},
        {"id": 1, "price": "1", "qty": "1", "time": 1704067260000, "isBuyerMaker": "false"},
        {"id": 1, "price": "1", "qty": "1", "time": 1704067260000, "isBuyerMaker": 0},
        {"id": 1, "price": "0", "qty": "1", "time": 1704067260000, "isBuyerMaker": False},
        {"id": 1, "price": "1", "qty": "0", "time": 1704067260000, "isBuyerMaker": False},
        {"id": 1, "price": "1", "qty": "1", "time": 0, "isBuyerMaker": False},
        {"id": 1, "price": "x", "qty": "1", "time": 1704067260000, "isBuyerMaker": False},
    ],
)
def test_recent_trade_strict_rules_raise_parse_error(payload: object) -> None:
    with pytest.raises(ParseError):
        normalize_recent_trade(payload, symbol="NEWUSDT")


@pytest.mark.parametrize(
    "bad_id",
    [-1, "-1", 1.5, "1.5", True, False, None, "", "abc"],
)
def test_recent_trade_id_must_be_nonnegative_integral(bad_id: object) -> None:
    with pytest.raises(ParseError):
        normalize_recent_trade(
            {
                "id": bad_id,
                "price": "1",
                "qty": "1",
                "time": 1704067260000,
                "isBuyerMaker": False,
            },
            symbol="NEWUSDT",
        )


@pytest.mark.parametrize(
    ("good_id", "expected"),
    [(0, "0"), (1, "1"), (99, "99"), ("0", "0"), ("7", "7")],
)
def test_recent_trade_id_preserves_valid_nonnegative(good_id: object, expected: str) -> None:
    trade = normalize_recent_trade(
        {
            "id": good_id,
            "price": "1",
            "qty": "1",
            "time": 1704067260000,
            "isBuyerMaker": False,
        },
        symbol="NEWUSDT",
    )
    assert trade.external_trade_id == expected


# --- Blocker 5: depth ---


@pytest.mark.parametrize(
    "payload",
    [
        {"asks": [], "lastUpdateId": 1},  # missing bids
        {"bids": [], "lastUpdateId": 1},  # missing asks
        {"bids": [], "asks": []},  # missing lastUpdateId
        {"bids": [], "asks": [], "lastUpdateId": None},
        {"bids": [], "asks": [], "lastUpdateId": -1},
        {"bids": [], "asks": [], "lastUpdateId": 1.5},
        {"bids": [], "asks": [], "lastUpdateId": True},
        {"bids": [], "asks": [], "lastUpdateId": "1"},
        {"bids": ["not-a-level"], "asks": [], "lastUpdateId": 1},
        {"bids": [["1.0"]], "asks": [], "lastUpdateId": 1},
        {"bids": [["1.0", "1.0", "extra"]], "asks": [], "lastUpdateId": 1},
        {"bids": [[None, "1"]], "asks": [], "lastUpdateId": 1},
        {"bids": [["0", "1"]], "asks": [], "lastUpdateId": 1},
        {"bids": [["-1", "1"]], "asks": [], "lastUpdateId": 1},
        {"bids": [["1", "-1"]], "asks": [], "lastUpdateId": 1},
        {"bids": [["NaN", "1"]], "asks": [], "lastUpdateId": 1},
        {"bids": [None], "asks": [], "lastUpdateId": 1},
        {"bids": [], "asks": [["1.0", "nope"]], "lastUpdateId": 1},
    ],
)
def test_depth_strict_rules_raise_parse_error(payload: object) -> None:
    with pytest.raises(ParseError):
        normalize_order_book(
            payload,
            symbol="NEWUSDT",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )


def test_depth_requires_last_update_id_even_when_empty() -> None:
    book = normalize_order_book(
        {"bids": [], "asks": [], "lastUpdateId": 0},
        symbol="NEWUSDT",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
    )
    assert book.last_update_id == 0
    assert book.bids == () and book.asks == ()


# --- Blocker 5: ticker/24hr ---


@pytest.mark.parametrize(
    "payload",
    [
        {"lastPrice": "1", "volume": "1", "closeTime": 1704067200000},  # missing symbol
        {"symbol": "", "lastPrice": "1", "volume": "1", "closeTime": 1704067200000},
        {"symbol": None, "lastPrice": "1", "volume": "1", "closeTime": 1704067200000},
        {"symbol": 1, "lastPrice": "1", "volume": "1", "closeTime": 1704067200000},
        {"symbol": "NEWUSDT", "lastPrice": "1", "volume": "1"},  # missing closeTime
        {"symbol": "NEWUSDT", "lastPrice": "1", "volume": "1", "closeTime": None},
        {"symbol": "NEWUSDT", "lastPrice": "1", "volume": "1", "closeTime": 0},
        {"symbol": "NEWUSDT", "lastPrice": "1", "volume": "1", "closeTime": -1},
        {"symbol": "NEWUSDT", "lastPrice": "1", "volume": "1", "closeTime": 1.5},
        {"symbol": "NEWUSDT", "lastPrice": "0", "volume": "1", "closeTime": 1704067200000},
        {"symbol": "NEWUSDT", "lastPrice": "-1", "volume": "1", "closeTime": 1704067200000},
        {"symbol": "NEWUSDT", "lastPrice": "NaN", "volume": "1", "closeTime": 1704067200000},
        {"symbol": "NEWUSDT", "lastPrice": "1", "volume": "-1", "closeTime": 1704067200000},
        {
            "symbol": "NEWUSDT",
            "lastPrice": "1",
            "volume": "1",
            "closeTime": 1704067200000,
            "count": 1.5,
        },
        {
            "symbol": "NEWUSDT",
            "lastPrice": "1",
            "volume": "1",
            "closeTime": 1704067200000,
            "count": -1,
        },
        {
            "symbol": "NEWUSDT",
            "lastPrice": "1",
            "volume": "1",
            "closeTime": 1704067200000,
            "quoteVolume": "NaN",
        },
        {
            "symbol": "NEWUSDT",
            "lastPrice": "1",
            "volume": "1",
            "closeTime": 1704067200000,
            "priceChange": "Infinity",
        },
    ],
)
def test_ticker_24h_strict_rules_raise_parse_error(payload: object) -> None:
    with pytest.raises(ParseError):
        normalize_ticker_24h(payload)


def test_ticker_24h_does_not_fallback_close_time_to_now() -> None:
    with pytest.raises(ParseError):
        normalize_ticker_24h({"symbol": "NEWUSDT", "lastPrice": "1", "volume": "1"})


@pytest.mark.parametrize("quote_volume", [-1, "-1"])
def test_ticker_24h_present_negative_quote_volume_raises_parse_error(quote_volume: object) -> None:
    with pytest.raises(ParseError):
        normalize_ticker_24h(
            {
                "symbol": "NEWUSDT",
                "lastPrice": "1",
                "volume": "1",
                "closeTime": 1704067200000,
                "quoteVolume": quote_volume,
            }
        )


@pytest.mark.parametrize("quote_volume", ["", None])
def test_ticker_24h_present_empty_or_null_quote_volume_raises_parse_error(
    quote_volume: object,
) -> None:
    with pytest.raises(ParseError):
        normalize_ticker_24h(
            {
                "symbol": "NEWUSDT",
                "lastPrice": "1",
                "volume": "1",
                "closeTime": 1704067200000,
                "quoteVolume": quote_volume,
            }
        )


def test_ticker_24h_absent_quote_volume_maps_to_none() -> None:
    ticker = normalize_ticker_24h(
        {
            "symbol": "NEWUSDT",
            "lastPrice": "1",
            "volume": "1",
            "closeTime": 1704067200000,
        }
    )
    assert ticker.quote_volume is None


def test_ticker_24h_present_nonnegative_quote_volume_is_kept() -> None:
    ticker = normalize_ticker_24h(
        {
            "symbol": "NEWUSDT",
            "lastPrice": "1",
            "volume": "1",
            "closeTime": 1704067200000,
            "quoteVolume": "0",
        }
    )
    assert ticker.quote_volume == Decimal("0")


@pytest.mark.asyncio
async def test_client_methods_use_public_spot_paths() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        mapping = {
            "/api/v3/exchangeInfo": "exchange_info.json",
            "/api/v3/klines": "klines.json",
            "/api/v3/aggTrades": "agg_trades.json",
            "/api/v3/trades": "trades.json",
            "/api/v3/depth": "depth.json",
            "/api/v3/ticker/24hr": "ticker_24h.json",
        }
        name = mapping[request.url.path]
        return httpx.Response(200, json=_load(name))

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    client = BinanceClient(http=http, base_url="https://api.binance.com")
    listings = await client.exchange_info()
    klines = await client.klines("NEWUSDT", interval="1h", start_time=1, end_time=2, limit=500)
    agg = await client.agg_trades("NEWUSDT", from_id=1, limit=500)
    recent = await client.recent_trades("NEWUSDT", limit=500)
    book = await client.order_book("NEWUSDT", limit=5)
    ticker = await client.ticker_24h("NEWUSDT")
    await http.aclose()

    assert [e.symbol for e in listings] == ["NEWUSDT"]
    assert len(klines) == 1
    assert agg[0].external_trade_id == "26129"
    assert agg[0].source == "binance:aggTrades"
    assert recent[0].side is Side.SELL
    assert recent[0].source == "binance:trades"
    assert book.bids[0].price == Decimal("1.09000000")
    assert ticker.volume == Decimal("1000.00000000")
    assert seen == [
        "/api/v3/exchangeInfo",
        "/api/v3/klines",
        "/api/v3/aggTrades",
        "/api/v3/trades",
        "/api/v3/depth",
        "/api/v3/ticker/24hr",
    ]


async def _call_binance(method: str, payload: object, **kwargs: object) -> object:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    client = BinanceClient(http=http, base_url="https://api.binance.com")
    try:
        if method == "exchange_info":
            return await client.exchange_info()
        if method == "klines":
            return await client.klines(str(kwargs.get("symbol", "NEWUSDT")), interval="1h")
        if method == "agg_trades":
            return await client.agg_trades(str(kwargs.get("symbol", "NEWUSDT")))
        if method == "recent_trades":
            return await client.recent_trades(str(kwargs.get("symbol", "NEWUSDT")))
        if method == "order_book":
            return await client.order_book(str(kwargs.get("symbol", "NEWUSDT")))
        if method == "ticker_24h":
            return await client.ticker_24h(str(kwargs.get("symbol", "NEWUSDT")))
        raise AssertionError(f"unknown method {method}")
    finally:
        await http.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "payload"),
    [
        ("exchange_info", ["not-an-object"]),
        ("exchange_info", "nope"),
        ("exchange_info", {"serverTime": 1}),
        ("exchange_info", {"symbols": "not-a-list"}),
        ("exchange_info", {"symbols": [{"status": "TRADING"}]}),
        ("exchange_info", {"symbols": [None]}),
        ("exchange_info", {"symbols": ["NEWUSDT"]}),
        ("klines", {"not": "a list"}),
        ("klines", "nope"),
        ("klines", [{"bad": "row"}]),
        ("klines", [[1704067200000, "1"]]),
        ("klines", [[1704067200000, "bad", "2", "0.5", "1.5", "10", 1704070799999, "11", 1]]),
        ("klines", [[1704067200000, "1", "2", "0.5", "1.5", "10", 1704070799999, "11", "count"]]),
        ("klines", [[None, "1", "2", "0.5", "1.5", "10", 1704070799999, "11", 1]]),
        ("agg_trades", {"oops": True}),
        ("agg_trades", "nope"),
        ("agg_trades", [{"a": 1, "p": "1"}]),
        ("agg_trades", [{"a": 1, "p": "bad", "q": "1", "T": 1704067260000}]),
        ("agg_trades", [None]),
        ("agg_trades", ["not-a-record"]),
        ("recent_trades", "nope"),
        ("recent_trades", {"id": 1}),
        ("recent_trades", [{"id": 1, "price": "1"}]),
        ("recent_trades", [{"id": 1, "price": "x", "qty": "1", "time": 1704067260000}]),
        ("recent_trades", [None]),
        ("order_book", ["not-object"]),
        ("order_book", "nope"),
        ("order_book", {"asks": []}),
        ("order_book", {"bids": "x", "asks": []}),
        ("order_book", {"bids": [], "asks": {"p": "1"}}),
        ("order_book", {"bids": ["not-a-pair"], "asks": []}),
        ("order_book", {"bids": [["1.0"]], "asks": []}),
        ("order_book", {"bids": [["bad", "1"]], "asks": []}),
        ("order_book", {"bids": [None], "asks": []}),
        ("order_book", {"bids": [], "asks": [["1.0", "nope"]]}),
        ("order_book", {"bids": [], "asks": [], "lastUpdateId": "nope"}),
        ("ticker_24h", ["array"]),
        ("ticker_24h", "nope"),
        ("ticker_24h", {}),
        ("ticker_24h", {"symbol": "NEWUSDT"}),
        ("ticker_24h", {"symbol": "NEWUSDT", "lastPrice": "1.1"}),
        ("ticker_24h", {"symbol": "NEWUSDT", "lastPrice": "bad", "volume": "1"}),
        ("ticker_24h", {"symbol": "NEWUSDT", "lastPrice": "NaN", "volume": "1"}),
        ("ticker_24h", {"symbol": "NEWUSDT", "lastPrice": "Infinity", "volume": "1"}),
        ("ticker_24h", {"lastPrice": "1", "volume": "1"}),
    ],
)
async def test_malformed_http_200_payloads_raise_parse_error(method: str, payload: object) -> None:
    with pytest.raises(ParseError):
        await _call_binance(method, payload)


@pytest.mark.asyncio
async def test_valid_empty_payloads_are_preserved() -> None:
    assert await _call_binance("exchange_info", {"serverTime": 1704067200000, "symbols": []}) == []
    assert await _call_binance("klines", []) == []
    assert await _call_binance("agg_trades", []) == []
    assert await _call_binance("recent_trades", []) == []
    book = await _call_binance("order_book", {"bids": [], "asks": [], "lastUpdateId": 0})
    assert book.bids == ()
    assert book.asks == ()
    assert book.last_update_id == 0
