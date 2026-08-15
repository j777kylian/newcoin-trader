"""Binance official public Spot REST collector (read-only)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from newcoin_trader.collectors.binance.normalize import (
    normalize_agg_trade,
    normalize_exchange_info,
    normalize_kline,
    normalize_order_book,
    normalize_recent_trade,
    normalize_ticker_24h,
)
from newcoin_trader.collectors.http import GetJsonClient
from newcoin_trader.domain.market import Kline, OrderBookL2, Ticker24h, TradeTick
from newcoin_trader.domain.tokens import NewListingEvent
from newcoin_trader.errors import ParseError


def _require_list_payload(payload: Any, *, context: str) -> list[Any]:
    if not isinstance(payload, list):
        raise ParseError(f"{context}: expected top-level list, got {type(payload).__name__}")
    return payload


class BinanceClient:
    def __init__(self, *, http: GetJsonClient, base_url: str = "https://api.binance.com") -> None:
        self._http = http
        self._base = base_url.rstrip("/")

    async def exchange_info(self) -> list[NewListingEvent]:
        payload = await self._http.get_json(f"{self._base}/api/v3/exchangeInfo")
        return normalize_exchange_info(payload)

    async def klines(
        self,
        symbol: str,
        *,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 500,
    ) -> list[Kline]:
        params: dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        payload = await self._http.get_json(f"{self._base}/api/v3/klines", params=params)
        rows = _require_list_payload(payload, context="klines")
        return [normalize_kline(row, symbol=symbol, interval=interval) for row in rows]

    async def agg_trades(
        self,
        symbol: str,
        *,
        from_id: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 500,
    ) -> list[TradeTick]:
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if from_id is not None:
            params["fromId"] = from_id
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        payload = await self._http.get_json(f"{self._base}/api/v3/aggTrades", params=params)
        rows = _require_list_payload(payload, context="aggTrades")
        return [normalize_agg_trade(row, symbol=symbol) for row in rows]

    async def recent_trades(self, symbol: str, *, limit: int = 500) -> list[TradeTick]:
        payload = await self._http.get_json(
            f"{self._base}/api/v3/trades",
            params={"symbol": symbol, "limit": limit},
        )
        rows = _require_list_payload(payload, context="trades")
        return [normalize_recent_trade(row, symbol=symbol) for row in rows]

    async def order_book(self, symbol: str, *, limit: int = 100) -> OrderBookL2:
        payload = await self._http.get_json(
            f"{self._base}/api/v3/depth",
            params={"symbol": symbol, "limit": limit},
        )
        return normalize_order_book(
            payload,
            symbol=symbol,
            timestamp=datetime.now(UTC),
        )

    async def ticker_24h(self, symbol: str) -> Ticker24h:
        payload = await self._http.get_json(
            f"{self._base}/api/v3/ticker/24hr",
            params={"symbol": symbol},
        )
        return normalize_ticker_24h(payload)
