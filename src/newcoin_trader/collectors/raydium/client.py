"""Raydium read-only pool and quote collector. Never submits swaps."""

from __future__ import annotations

from typing import Any

from newcoin_trader.collectors.http import GetJsonClient
from newcoin_trader.collectors.raydium.normalize import normalize_pool_list, normalize_quote
from newcoin_trader.domain.market import PoolQuote, PoolSnapshot


class RaydiumClient:
    """Public GET wrappers. Swap transaction submission endpoints are intentionally absent."""

    def __init__(
        self,
        *,
        http: GetJsonClient,
        pool_base_url: str = "https://api-v3.raydium.io",
        quote_base_url: str = "https://transaction-v1.raydium.io",
    ) -> None:
        self._http = http
        self._pool_base = pool_base_url.rstrip("/")
        self._quote_base = quote_base_url.rstrip("/")

    async def list_pools(
        self,
        *,
        page: int = 1,
        page_size: int = 100,
        pool_type: str = "all",
    ) -> list[PoolSnapshot]:
        payload = await self._http.get_json(
            f"{self._pool_base}/pools/info/list",
            params={
                "poolType": pool_type,
                "poolSortField": "default",
                "sortType": "desc",
                "pageSize": page_size,
                "page": page,
            },
        )
        return normalize_pool_list(payload)

    async def quote_swap_base_in(
        self,
        *,
        input_mint: str,
        output_mint: str,
        amount: str,
        slippage_bps: int = 50,
        tx_version: str = "V0",
    ) -> PoolQuote:
        """Read-only quote compute. Does not build, sign, or submit a swap."""
        params: dict[str, Any] = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": amount,
            "slippageBps": slippage_bps,
            "txVersion": tx_version,
        }
        payload = await self._http.get_json(
            f"{self._quote_base}/compute/swap-base-in",
            params=params,
        )
        return normalize_quote(payload)
