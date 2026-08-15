"""Async HTTP wrapper: timeouts, retries, rate limits. Structurally GET-only."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol

import httpx

from newcoin_trader.collectors.rate_limit import AsyncRateLimiter
from newcoin_trader.collectors.retry import (
    AUTH_STATUS_CODES,
    backoff_for_attempt,
    is_retryable_status,
    parse_retry_after,
)
from newcoin_trader.errors import (
    AuthError,
    CollectorError,
    NotFoundError,
    ParseError,
    RateLimitError,
    RetryableHttpError,
    TimeoutError,
)

SleepFn = Callable[[float], Awaitable[None]]


class GetJsonClient(Protocol):
    """Collector-facing transport: JSON GET only. No POST/PUT/PATCH/DELETE."""

    async def get_json(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any: ...

    async def aclose(self) -> None: ...


class AsyncHttpClient:
    """Concrete GET-only HTTP client used by all market-data collectors."""

    def __init__(
        self,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_seconds: float = 15.0,
        max_attempts: int = 4,
        backoff_seconds: float = 0.25,
        rate_limit_per_second: float = 8.0,
        headers: Mapping[str, str] | None = None,
        sleep: SleepFn = asyncio.sleep,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._owns_client = client is None
        timeout = httpx.Timeout(timeout_seconds)
        self._client = client or httpx.AsyncClient(
            timeout=timeout,
            transport=transport,
            headers=dict(headers or {}),
        )
        self._max_attempts = max_attempts
        self._backoff_seconds = backoff_seconds
        self._sleep = sleep
        self._limiter = AsyncRateLimiter(
            rate_per_second=rate_limit_per_second,
            sleep=sleep,
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def get_json(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        response = await self._get(url, params=params, headers=headers)
        try:
            return response.json()
        except ValueError as exc:
            raise ParseError(f"invalid JSON from {url}") from exc

    async def _get(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        last_error: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            await self._limiter.acquire()
            try:
                response = await self._client.request(
                    "GET",
                    url,
                    params=params,
                    headers=headers,
                )
            except httpx.TimeoutException as exc:
                last_error = TimeoutError(str(exc))
                if attempt >= self._max_attempts:
                    raise last_error from exc
                await self._sleep(backoff_for_attempt(attempt, self._backoff_seconds))
                continue

            if response.status_code in AUTH_STATUS_CODES:
                raise AuthError(f"{response.status_code} for {url}")
            if response.status_code == 404:
                raise NotFoundError(f"404 for {url}")
            if is_retryable_status(response.status_code):
                retry_after = parse_retry_after(response.headers)
                wait = retry_after if retry_after is not None else backoff_for_attempt(attempt, self._backoff_seconds)
                if response.status_code == 429:
                    last_error = RateLimitError(
                        f"429 for {url}",
                        retry_after_seconds=retry_after,
                    )
                else:
                    last_error = RetryableHttpError(
                        f"{response.status_code} for {url}",
                        status_code=response.status_code,
                    )
                if attempt >= self._max_attempts:
                    raise last_error
                await self._sleep(wait)
                continue
            if response.status_code >= 400:
                raise CollectorError(f"{response.status_code} for {url}: {response.text[:200]}")
            return response

        assert last_error is not None
        raise last_error
