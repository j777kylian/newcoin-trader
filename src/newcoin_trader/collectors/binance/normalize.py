"""Normalize Binance Spot public REST payloads into domain records."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from newcoin_trader.collectors.normalization import (
    guard_parse,
    parse_decimal,
    parse_int,
    parse_required_venue_time,
    parse_venue_time,
    require_list,
    require_mapping,
)
from newcoin_trader.domain.enums import Chain, Side, Venue
from newcoin_trader.domain.market import (
    Kline,
    OrderBookL2,
    OrderBookLevel,
    Ticker24h,
    TradeTick,
)
from newcoin_trader.domain.tokens import NewListingEvent
from newcoin_trader.errors import ParseError

BINANCE_AGG_SOURCE = "binance:aggTrades"
BINANCE_TRADE_SOURCE = "binance:trades"
BINANCE_KLINE_ROW_LEN = 12


def _require_keys(item: dict[str, Any], keys: tuple[str, ...], *, context: str) -> None:
    missing = [key for key in keys if key not in item]
    if missing:
        raise ParseError(f"{context}: missing required fields {missing}")


def _require_nonempty_str(value: Any, *, context: str) -> str:
    if not isinstance(value, str) or not value:
        raise ParseError(f"{context}: expected non-empty string")
    return value


def _require_bool(item: dict[str, Any], key: str, *, context: str) -> bool:
    if key not in item:
        raise ParseError(f"{context}: missing required boolean {key}")
    value = item[key]
    if not isinstance(value, bool):
        raise ParseError(f"{context}: {key} must be a boolean")
    return value


def _require_id(value: Any, *, context: str) -> str:
    parsed = parse_int(value, context=context)
    if parsed < 0:
        raise ParseError(f"{context}: must be nonnegative")
    return str(parsed)


def _positive_decimal(value: Any, *, context: str) -> Decimal:
    result = parse_decimal(value, context=context)
    if result <= 0:
        raise ParseError(f"{context}: must be positive")
    return result


def _nonnegative_decimal(value: Any, *, context: str) -> Decimal:
    result = parse_decimal(value, context=context)
    if result < 0:
        raise ParseError(f"{context}: must be nonnegative")
    return result


def _optional_decimal(item: dict[str, Any], key: str, *, context: str) -> Decimal | None:
    if key not in item or item[key] in (None, ""):
        return None
    return parse_decimal(item[key], context=context)


def _optional_present_nonnegative_decimal(item: dict[str, Any], key: str, *, context: str) -> Decimal | None:
    """Absent → None. Present null/empty/malformed/negative → ParseError."""
    if key not in item:
        return None
    value = item[key]
    if value is None or value == "":
        raise ParseError(f"{context}: present null/empty is invalid")
    return _nonnegative_decimal(value, context=context)


def _optional_nonnegative_int(item: dict[str, Any], key: str, *, context: str) -> int | None:
    if key not in item or item[key] is None:
        return None
    value = parse_int(item[key], context=context)
    if value < 0:
        raise ParseError(f"{context}: must be nonnegative")
    return value


def _normalize_book_level(level: Any, *, context: str) -> OrderBookLevel:
    if not isinstance(level, (list, tuple)) or len(level) != 2:
        raise ParseError(f"{context}: expected exact [price, quantity] pair")
    return OrderBookLevel(
        price=_positive_decimal(level[0], context=f"{context}.price"),
        quantity=_nonnegative_decimal(level[1], context=f"{context}.qty"),
    )


@guard_parse("exchangeInfo")
def normalize_exchange_info(payload: Any) -> list[NewListingEvent]:
    data = require_mapping(payload, context="exchangeInfo")
    _require_keys(data, ("serverTime", "symbols"), context="exchangeInfo")
    seen_at = parse_required_venue_time(data["serverTime"], context="exchangeInfo.serverTime")
    symbols = require_list(data["symbols"], context="exchangeInfo.symbols")
    events: list[NewListingEvent] = []
    for raw in symbols:
        item = require_mapping(raw, context="exchangeInfo.symbol")
        if "status" not in item:
            raise ParseError("exchangeInfo.symbol: status is required")
        status = item["status"]
        if not isinstance(status, str):
            raise ParseError("exchangeInfo.symbol: status must be a string")
        _require_keys(item, ("symbol",), context="exchangeInfo.symbol")
        symbol = _require_nonempty_str(item["symbol"], context="exchangeInfo.symbol")
        if status != "TRADING":
            continue
        onboard = item.get("onboardDate")
        created = parse_venue_time(onboard) if onboard else None
        events.append(
            NewListingEvent(
                token_address=symbol,
                chain=Chain.BINANCE,
                symbol=symbol,
                name=str(item.get("baseAsset", symbol)),
                created_time=created,
                first_seen_time=seen_at,
                source="binance",
                venue=Venue.BINANCE,
                provenance={"endpoint": "/api/v3/exchangeInfo"},
            )
        )
    return events


@guard_parse("kline")
def normalize_kline(row: Any, *, symbol: str, interval: str) -> Kline:
    if not isinstance(row, list) or len(row) != BINANCE_KLINE_ROW_LEN:
        raise ParseError(f"kline row must be a list with exactly {BINANCE_KLINE_ROW_LEN} fields")
    open_time = parse_required_venue_time(row[0], context="kline.open_time")
    close_time = parse_required_venue_time(row[6], context="kline.close_time")
    if close_time < open_time:
        raise ParseError("kline: close_time must be >= open_time")
    trade_count = parse_int(row[8], context="kline.trade_count")
    if trade_count < 0:
        raise ParseError("kline.trade_count: must be nonnegative")
    return Kline(
        token_address=symbol,
        chain=Chain.BINANCE.value,
        open_time=open_time,
        close_time=close_time,
        open=_positive_decimal(row[1], context="kline.open"),
        high=_positive_decimal(row[2], context="kline.high"),
        low=_positive_decimal(row[3], context="kline.low"),
        close=_positive_decimal(row[4], context="kline.close"),
        volume=_nonnegative_decimal(row[5], context="kline.volume"),
        quote_volume=_nonnegative_decimal(row[7], context="kline.quote_volume"),
        trade_count=trade_count,
        interval=interval,
        source="binance",
        venue=Venue.BINANCE,
    )


@guard_parse("aggTrade")
def normalize_agg_trade(payload: Any, *, symbol: str) -> TradeTick:
    item = require_mapping(payload, context="aggTrade")
    _require_keys(item, ("a", "p", "q", "T", "m"), context="aggTrade")
    is_buyer_maker = _require_bool(item, "m", context="aggTrade")
    return TradeTick(
        token_address=symbol,
        chain=Chain.BINANCE.value,
        timestamp=parse_required_venue_time(item["T"], context="aggTrade.T"),
        side=Side.SELL if is_buyer_maker else Side.BUY,
        amount=_positive_decimal(item["q"], context="aggTrade.q"),
        price=_positive_decimal(item["p"], context="aggTrade.p"),
        external_trade_id=_require_id(item["a"], context="aggTrade.a"),
        source=BINANCE_AGG_SOURCE,
        provenance={"kind": "aggTrade", "endpoint": "/api/v3/aggTrades"},
    )


@guard_parse("trade")
def normalize_recent_trade(payload: Any, *, symbol: str) -> TradeTick:
    item = require_mapping(payload, context="trade")
    _require_keys(item, ("id", "price", "qty", "time", "isBuyerMaker"), context="trade")
    is_buyer_maker = _require_bool(item, "isBuyerMaker", context="trade")
    return TradeTick(
        token_address=symbol,
        chain=Chain.BINANCE.value,
        timestamp=parse_required_venue_time(item["time"], context="trade.time"),
        side=Side.SELL if is_buyer_maker else Side.BUY,
        amount=_positive_decimal(item["qty"], context="trade.qty"),
        price=_positive_decimal(item["price"], context="trade.price"),
        external_trade_id=_require_id(item["id"], context="trade.id"),
        source=BINANCE_TRADE_SOURCE,
        provenance={"kind": "trade", "endpoint": "/api/v3/trades"},
    )


@guard_parse("depth")
def normalize_order_book(payload: Any, *, symbol: str, timestamp: datetime) -> OrderBookL2:
    item = require_mapping(payload, context="depth")
    _require_keys(item, ("bids", "asks", "lastUpdateId"), context="depth")
    last_update_id = item["lastUpdateId"]
    if isinstance(last_update_id, bool) or not isinstance(last_update_id, int):
        raise ParseError("depth.lastUpdateId: must be an integral int")
    if last_update_id < 0:
        raise ParseError("depth.lastUpdateId: must be nonnegative")
    bids = tuple(
        _normalize_book_level(level, context="depth.bids") for level in require_list(item["bids"], context="depth.bids")
    )
    asks = tuple(
        _normalize_book_level(level, context="depth.asks") for level in require_list(item["asks"], context="depth.asks")
    )
    return OrderBookL2(
        token_address=symbol,
        chain=Chain.BINANCE.value,
        timestamp=timestamp,
        bids=bids,
        asks=asks,
        last_update_id=last_update_id,
        source="binance",
    )


@guard_parse("ticker24h")
def normalize_ticker_24h(payload: Any) -> Ticker24h:
    item = require_mapping(payload, context="ticker24h")
    _require_keys(item, ("symbol", "lastPrice", "volume", "closeTime"), context="ticker24h")
    symbol = _require_nonempty_str(item["symbol"], context="ticker24h.symbol")
    close_time = parse_required_venue_time(item["closeTime"], context="ticker24h.closeTime")
    return Ticker24h(
        token_address=symbol,
        chain=Chain.BINANCE.value,
        timestamp=close_time,
        last_price=_positive_decimal(item["lastPrice"], context="ticker24h.lastPrice"),
        volume=_nonnegative_decimal(item["volume"], context="ticker24h.volume"),
        quote_volume=_optional_present_nonnegative_decimal(item, "quoteVolume", context="ticker24h.quoteVolume"),
        price_change=_optional_decimal(item, "priceChange", context="ticker24h.priceChange"),
        trade_count=_optional_nonnegative_int(item, "count", context="ticker24h.count"),
        source="binance",
    )
