"""Birdeye public discovery collector (API key via env, read-only)."""

from __future__ import annotations

from typing import Any

from newcoin_trader.collectors.birdeye.normalize import normalize_new_pairs, normalize_new_tokens
from newcoin_trader.collectors.http import GetJsonClient
from newcoin_trader.domain.tokens import NewListingEvent


class BirdeyeClient:
    def __init__(
        self,
        *,
        http: GetJsonClient,
        api_key: str,
        base_url: str = "https://public-api.birdeye.so",
        chain: str = "solana",
    ) -> None:
        self._http = http
        self._api_key = api_key
        self._base = base_url.rstrip("/")
        self._chain = chain

    def _headers(self) -> dict[str, str]:
        return {"X-API-KEY": self._api_key, "x-chain": self._chain, "accept": "application/json"}

    async def discover_new_tokens(
        self,
        *,
        limit: int = 10,
        time_to: int | None = None,
        meme_platform_enabled: bool = False,
    ) -> list[NewListingEvent]:
        params: dict[str, Any] = {"limit": limit, "meme_platform_enabled": str(meme_platform_enabled).lower()}
        if time_to is not None:
            params["time_to"] = time_to
        payload = await self._http.get_json(
            f"{self._base}/defi/v2/tokens/new_listing",
            params=params,
            headers=self._headers(),
        )
        return normalize_new_tokens(payload, chain=self._chain)

    async def discover_new_pairs(
        self,
        *,
        limit: int = 10,
        offset: int = 0,
    ) -> list[NewListingEvent]:
        payload = await self._http.get_json(
            f"{self._base}/defi/v3/search",
            params={
                "chain": self._chain,
                "keyword": "",
                "target": "pair",
                "sort_by": "creation_time",
                "sort_type": "desc",
                "offset": offset,
                "limit": limit,
            },
            headers=self._headers(),
        )
        return normalize_new_pairs(payload, chain=self._chain)
