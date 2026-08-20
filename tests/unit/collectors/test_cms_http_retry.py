"""Phase 8.1C: CMS-scoped CloudFront 429 recovery (mocked transport only)."""

from __future__ import annotations

import httpx
import pytest

from newcoin_trader.collectors.binance.announcements import CMS_429_COOLDOWNS, create_cms_http_client
from newcoin_trader.collectors.http import AsyncHttpClient
from newcoin_trader.errors import RateLimitError


@pytest.mark.asyncio
async def test_cms_429_with_retry_after_obey_header() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return httpx.Response(200, json={"ok": True})

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    client = create_cms_http_client(
        transport=httpx.MockTransport(handler),
        sleep=fake_sleep,
        max_attempts=3,
        rate_limit_per_second=1000.0,
    )
    payload = await client.get_json("https://www.binance.com/bapi/composite/v1/public/cms/article/list/query")
    await client.aclose()
    assert payload == {"ok": True}
    assert calls["n"] == 2
    assert sleeps == [7.0]


@pytest.mark.asyncio
async def test_cms_429_without_retry_after_uses_escalating_cooldown() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] <= 3:
            return httpx.Response(429)
        return httpx.Response(200, json={"ok": True})

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    client = create_cms_http_client(
        transport=httpx.MockTransport(handler),
        sleep=fake_sleep,
        max_attempts=4,
        rate_limit_per_second=1000.0,
    )
    payload = await client.get_json("https://www.binance.com/bapi/composite/v1/public/cms/article/list/query")
    await client.aclose()
    assert payload == {"ok": True}
    assert calls["n"] == 4
    assert sleeps == list(CMS_429_COOLDOWNS[:3])


@pytest.mark.asyncio
async def test_cms_429_retry_exhaustion_fails_cleanly() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429)

    async def fake_sleep(seconds: float) -> None:
        return None

    client = create_cms_http_client(
        transport=httpx.MockTransport(handler),
        sleep=fake_sleep,
        max_attempts=3,
        rate_limit_per_second=1000.0,
    )
    with pytest.raises(RateLimitError):
        await client.get_json("https://www.binance.com/bapi/composite/v1/public/cms/article/list/query")
    await client.aclose()


@pytest.mark.asyncio
async def test_non_cms_client_429_without_retry_after_uses_generic_backoff() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429)
        return httpx.Response(200, json={"ok": True})

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    client = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=3,
        backoff_seconds=0.25,
        rate_limit_per_second=1000.0,
        sleep=fake_sleep,
    )
    payload = await client.get_json("https://api.binance.com/api/v3/ping")
    await client.aclose()
    assert payload == {"ok": True}
    assert sleeps == [0.25]
