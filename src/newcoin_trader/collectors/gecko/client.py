"""GeckoTerminal public pool and OHLCV collector."""

from __future__ import annotations

from typing import Any

from newcoin_trader.collectors.gecko.normalize import normalize_ohlcv, normalize_pool
from newcoin_trader.collectors.http import GetJsonClient
from newcoin_trader.domain.market import Kline, PoolSnapshot


class GeckoTerminalClient:
    def __init__(
        self,
        *,
        http: GetJsonClient,
        base_url: str = "https://api.geckoterminal.com/api/v2",
    ) -> None:
        self._http = http
        self._base = base_url.rstrip("/")

    async def get_pool(self, network: str, pool_address: str) -> PoolSnapshot:
        payload = await self._http.get_json(
            f"{self._base}/networks/{network}/pools/{pool_address}",
            params={"include": "base_token,quote_token"},
        )
        return normalize_pool(payload, network=network)

    async def pool_ohlcv(
        self,
        network: str,
        pool_address: str,
        *,
        timeframe: str = "minute",
        aggregate: int = 1,
        limit: int = 100,
        before_timestamp: int | None = None,
        currency: str = "usd",
    ) -> list[Kline]:
        params: dict[str, Any] = {
            "aggregate": aggregate,
            "limit": limit,
            "currency": currency,
        }
        if before_timestamp is not None:
            params["before_timestamp"] = before_timestamp
        payload = await self._http.get_json(
            f"{self._base}/networks/{network}/pools/{pool_address}/ohlcv/{timeframe}",
            params=params,
        )
        interval = f"{aggregate}{timeframe[0]}"
        return normalize_ohlcv(
            payload,
            pool_address=pool_address,
            network=network,
            interval=interval,
        )
