"""HTTP retry, backoff, and rate-limit behavior (mocked transport only)."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from newcoin_trader.collectors.http import AsyncHttpClient
from newcoin_trader.collectors.rate_limit import AsyncRateLimiter
from newcoin_trader.errors import (
    AuthError,
    ParseError,
    RateLimitError,
    RetryableHttpError,
    TimeoutError,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.mark.asyncio
async def test_rate_limiter_spaces_requests() -> None:
    clock = FakeClock()
    limiter = AsyncRateLimiter(
        rate_per_second=2.0,
        capacity=2.0,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    await limiter.acquire()
    await limiter.acquire()
    await limiter.acquire()
    assert clock.sleeps
    assert clock.sleeps[-1] > 0


@pytest.mark.asyncio
async def test_retries_on_429_and_respects_retry_after() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "2"}, json={"msg": "slow"})
        return httpx.Response(200, json={"ok": True})

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    client = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=3,
        backoff_seconds=0.5,
        rate_limit_per_second=1000.0,
        sleep=fake_sleep,
        timeout_seconds=5.0,
    )
    payload = await client.get_json("https://example.test/v1")
    assert payload == {"ok": True}
    assert calls["n"] == 2
    assert sleeps == [2.0]
    await client.aclose()


@pytest.mark.asyncio
async def test_retries_on_503() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503, json={"msg": "down"})
        return httpx.Response(200, json={"ok": True})

    async def fake_sleep(seconds: float) -> None:
        return None

    client = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=4,
        backoff_seconds=0.01,
        rate_limit_per_second=1000.0,
        sleep=fake_sleep,
    )
    payload = await client.get_json("https://example.test/v1")
    assert payload == {"ok": True}
    assert calls["n"] == 3
    await client.aclose()


@pytest.mark.asyncio
async def test_does_not_retry_401() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(401, json={"msg": "nope"})

    client = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=4,
        backoff_seconds=0.01,
        rate_limit_per_second=1000.0,
        sleep=asyncio.sleep,
    )
    with pytest.raises(AuthError):
        await client.get_json("https://example.test/v1")
    assert calls["n"] == 1
    await client.aclose()


@pytest.mark.asyncio
async def test_does_not_retry_parse_error_on_200_invalid_json() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, content=b"not-json", headers={"content-type": "application/json"})

    client = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=4,
        backoff_seconds=0.01,
        rate_limit_per_second=1000.0,
        sleep=asyncio.sleep,
    )
    with pytest.raises(ParseError):
        await client.get_json("https://example.test/v1")
    assert calls["n"] == 1
    await client.aclose()


@pytest.mark.asyncio
async def test_retries_timeouts_then_succeeds() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ReadTimeout("slow", request=request)
        return httpx.Response(200, json={"ok": True})

    async def fake_sleep(seconds: float) -> None:
        return None

    client = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=3,
        backoff_seconds=0.01,
        rate_limit_per_second=1000.0,
        sleep=fake_sleep,
    )
    payload = await client.get_json("https://example.test/v1")
    assert payload == {"ok": True}
    assert calls["n"] == 2
    await client.aclose()


@pytest.mark.asyncio
async def test_exhausted_429_raises_rate_limit_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "1"})

    async def fake_sleep(seconds: float) -> None:
        return None

    client = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=2,
        backoff_seconds=0.01,
        rate_limit_per_second=1000.0,
        sleep=fake_sleep,
    )
    with pytest.raises(RateLimitError):
        await client.get_json("https://example.test/v1")
    await client.aclose()


@pytest.mark.asyncio
async def test_exhausted_5xx_raises_retryable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502)

    async def fake_sleep(seconds: float) -> None:
        return None

    client = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=2,
        backoff_seconds=0.01,
        rate_limit_per_second=1000.0,
        sleep=fake_sleep,
    )
    with pytest.raises(RetryableHttpError):
        await client.get_json("https://example.test/v1")
    await client.aclose()


@pytest.mark.asyncio
async def test_exhausted_timeout_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("nope", request=request)

    async def fake_sleep(seconds: float) -> None:
        return None

    client = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=2,
        backoff_seconds=0.01,
        rate_limit_per_second=1000.0,
        sleep=fake_sleep,
    )
    with pytest.raises(TimeoutError):
        await client.get_json("https://example.test/v1")
    await client.aclose()


def test_retryable_status_helper() -> None:
    from newcoin_trader.collectors.retry import is_retryable_status, parse_retry_after

    assert is_retryable_status(429)
    assert is_retryable_status(503)
    assert not is_retryable_status(401)
    assert not is_retryable_status(400)
    assert parse_retry_after({"Retry-After": "3"}) == 3.0
    assert parse_retry_after({}) is None
