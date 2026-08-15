"""Birdeye / Raydium / GeckoTerminal collectors (mock transport only)."""

from __future__ import annotations

import inspect
import json
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from newcoin_trader.collectors.birdeye.client import BirdeyeClient
from newcoin_trader.collectors.gecko.client import GeckoTerminalClient
from newcoin_trader.collectors.http import AsyncHttpClient
from newcoin_trader.collectors.raydium import client as raydium_client_mod
from newcoin_trader.collectors.raydium.client import RaydiumClient

ROOT = Path(__file__).resolve().parents[3]


def _load(rel: str) -> object:
    return json.loads((ROOT / "fixtures" / rel).read_text(encoding="utf-8"))


@pytest.mark.asyncio
async def test_birdeye_new_tokens_and_pairs() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/tokens/new_listing"):
            return httpx.Response(200, json=_load("birdeye/new_tokens.json"))
        if request.url.path.endswith("/defi/v3/search"):
            return httpx.Response(200, json=_load("birdeye/new_pairs.json"))
        return httpx.Response(404)

    http = AsyncHttpClient(transport=httpx.MockTransport(handler), max_attempts=1, rate_limit_per_second=1000)
    client = BirdeyeClient(http=http, api_key="test-key", chain="solana")
    tokens = await client.discover_new_tokens(limit=1)
    pairs = await client.discover_new_pairs(limit=1)
    await http.aclose()
    assert tokens[0].symbol == "TOPCAT"
    assert tokens[0].liquidity == Decimal("15507.41635596545")
    assert pairs[0].token_address.startswith("MemeMint")
    assert pairs[0].pair_address.startswith("PairAddress")


@pytest.mark.asyncio
async def test_raydium_pools_and_read_only_quote() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "/pools/info/list" in request.url.path:
            return httpx.Response(200, json=_load("raydium/pools.json"))
        if "/compute/swap-base-in" in request.url.path:
            assert request.method == "GET"
            return httpx.Response(200, json=_load("raydium/quote.json"))
        return httpx.Response(404)

    http = AsyncHttpClient(transport=httpx.MockTransport(handler), max_attempts=1, rate_limit_per_second=1000)
    client = RaydiumClient(http=http)
    pools = await client.list_pools(page=1, page_size=1)
    quote = await client.quote_swap_base_in(
        input_mint="So11111111111111111111111111111111111111112",
        output_mint="MemeMint111111111111111111111111111111111",
        amount="1000000000",
    )
    await http.aclose()
    assert pools[0].liquidity == Decimal("25000.5")
    assert quote.output_amount == Decimal("165234567")
    assert quote.provenance is not None
    assert quote.provenance["read_only"] == "true"


def test_raydium_module_has_no_swap_submission() -> None:
    source = inspect.getsource(raydium_client_mod)
    assert "/transaction/swap" not in source
    assert 'method="POST"' not in source
    assert 'request("POST"' not in source
    assert not hasattr(RaydiumClient, "submit_swap")
    assert not hasattr(RaydiumClient, "build_transaction")


@pytest.mark.asyncio
async def test_gecko_pool_and_ohlcv() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/ohlcv/minute"):
            return httpx.Response(200, json=_load("gecko/ohlcv.json"))
        if "/pools/" in request.url.path:
            return httpx.Response(200, json=_load("gecko/pool.json"))
        return httpx.Response(404)

    http = AsyncHttpClient(transport=httpx.MockTransport(handler), max_attempts=1, rate_limit_per_second=1000)
    client = GeckoTerminalClient(http=http)
    pool = await client.get_pool("solana", "PoolAddress111111111111111111111111111")
    ohlcv = await client.pool_ohlcv(
        "solana",
        "PoolAddress111111111111111111111111111",
        timeframe="minute",
        limit=1,
        before_timestamp=1704067300,
    )
    await http.aclose()
    assert pool.price == Decimal("0.00125")
    assert ohlcv[0].close == Decimal("0.0011")
