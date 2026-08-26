"""Bounded async JSON-RPC POST boundary for Base evidence acquisition."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Protocol
from urllib.parse import urlsplit, urlunsplit

import httpx

RPC_METHODS = frozenset({"eth_chainId", "eth_getBlockByNumber", "eth_getLogs", "eth_getTransactionReceipt", "eth_call"})


class RpcError(RuntimeError):
    """Base class for a refused or failed JSON-RPC request."""


class RpcMethodError(RpcError):
    pass


class RpcResponseError(RpcError):
    def __init__(self, code: int) -> None:
        self.code = code
        super().__init__(f"JSON-RPC error {code}")


class RpcTransportError(RpcError):
    def __init__(
        self, message: str, *, retryable: bool, retry_exhausted: bool = False, malformed: bool = False
    ) -> None:
        self.retryable = retryable
        self.retry_exhausted = retry_exhausted
        self.malformed = malformed
        super().__init__(message)


@dataclass(frozen=True)
class RpcCallResult:
    method: str
    request_id: int
    provider_id: str
    sanitized_endpoint: str
    attempt_count: int
    result: object | None = None
    has_result: bool = False
    rpc_error: RpcResponseError | None = None

    def __post_init__(self) -> None:
        if self.has_result == (self.rpc_error is not None):
            raise ValueError("RpcCallResult requires exactly one result presence or rpc_error")


@dataclass(frozen=True)
class RpcCapabilityProfile:
    """Immutable endpoint capabilities and hard limits; callers cannot widen a run."""

    profile_version: str = "base_rpc_capability_v1"
    provider_id: str = "base_rpc_v1"
    sanitized_endpoint: str = ""
    chain_id: int = 8453
    supports_finalized_tag: bool = True
    supports_eip1898_block_hash_call: bool = True
    archive_eth_call_available: bool = True
    receipt_history_available: bool = True
    retryable_rpc_codes: tuple[int, ...] = (-32016,)
    range_limit_rpc_codes: tuple[int, ...] = (-32005,)
    max_block_span: int = 50_000
    max_split_depth: int = 20
    factory_request_budget: int = 2_000
    exact_pool_request_budget: int = 2_000
    max_attempts: int = 3
    timeout_seconds: float = 15.0
    factory_candidate_ceiling: int = 50_000
    exact_pool_candidate_ceiling: int = 100_000
    log_result_cap: int = 10_000

    def __post_init__(self) -> None:
        for name in ("profile_version", "provider_id"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError(f"{name} required")
        if self.sanitized_endpoint:
            if sanitize_rpc_endpoint(self.sanitized_endpoint) != self.sanitized_endpoint:
                raise ValueError("sanitized_endpoint must be canonical credential-free https URL")
        if self.chain_id != 8453:
            raise ValueError("chain_id must equal Base 8453")
        for name in (
            "supports_finalized_tag",
            "supports_eip1898_block_hash_call",
            "archive_eth_call_available",
            "receipt_history_available",
        ):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"{name} must be bool")
        if not isinstance(self.retryable_rpc_codes, tuple) or any(
            isinstance(code, bool) or not isinstance(code, int) for code in self.retryable_rpc_codes
        ):
            raise ValueError("retryable_rpc_codes must be a tuple of integer RPC codes")
        if not isinstance(self.range_limit_rpc_codes, tuple) or any(
            isinstance(code, bool) or not isinstance(code, int) for code in self.range_limit_rpc_codes
        ):
            raise ValueError("range_limit_rpc_codes must be a tuple of integer RPC codes")
        if set(self.retryable_rpc_codes) & set(self.range_limit_rpc_codes):
            raise ValueError("range_limit_rpc_codes must not overlap retryable_rpc_codes")
        limits = (
            ("max_block_span", self.max_block_span, 50_000),
            ("max_split_depth", self.max_split_depth, 20),
            ("factory_request_budget", self.factory_request_budget, 2_000),
            ("exact_pool_request_budget", self.exact_pool_request_budget, 2_000),
            ("max_attempts", self.max_attempts, 3),
            ("factory_candidate_ceiling", self.factory_candidate_ceiling, 50_000),
            ("exact_pool_candidate_ceiling", self.exact_pool_candidate_ceiling, 100_000),
            ("log_result_cap", self.log_result_cap, 100_000),
        )
        for name, value, ceiling in limits:
            if isinstance(value, bool) or not isinstance(value, int) or value < 1 or value > ceiling:
                raise ValueError(f"{name} must be an int in 1..{ceiling}")
        if not isinstance(self.timeout_seconds, (int, float)) or isinstance(self.timeout_seconds, bool):
            raise ValueError("timeout_seconds must be numeric")
        if not 0 < self.timeout_seconds <= 15:
            raise ValueError("timeout_seconds must be in (0, 15]")


class RpcPostTransport(Protocol):
    async def post_json(self, endpoint: str, payload: dict[str, object], *, timeout_seconds: float) -> object: ...


class _HttpxRpcTransport:
    async def post_json(self, endpoint: str, payload: dict[str, object], *, timeout_seconds: float) -> object:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds)) as client:
            response = await client.post(endpoint, json=payload)
            response.raise_for_status()
            return response.json()


def sanitize_rpc_endpoint(endpoint: object) -> str:
    """Return public provenance only: HTTPS scheme, host, and optional port."""
    if not isinstance(endpoint, str):
        raise ValueError("rpc endpoint must be a string")
    parsed = urlsplit(endpoint.strip())
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise ValueError("rpc endpoint must be credential-free https URL")
    if parsed.fragment:
        raise ValueError("rpc endpoint must not contain fragment")
    host = parsed.hostname
    if host is None:
        raise ValueError("rpc endpoint host required")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("rpc endpoint port malformed") from exc
    rendered_host = f"[{host.lower()}]" if ":" in host else host.lower()
    return urlunsplit(("https", rendered_host if port is None else f"{rendered_host}:{port}", "", "", ""))


def _transport_endpoint(endpoint: object) -> str:
    sanitize_rpc_endpoint(endpoint)
    parsed = urlsplit(str(endpoint).strip())
    return urlunsplit(("https", parsed.netloc, parsed.path.rstrip("/"), parsed.query, ""))


def is_retryable_rpc_error(error: RpcError, profile: RpcCapabilityProfile) -> bool:
    return (isinstance(error, RpcTransportError) and error.retryable) or (
        isinstance(error, RpcResponseError) and error.code in profile.retryable_rpc_codes
    )


def _transport_error(exc: BaseException) -> RpcTransportError:
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return RpcTransportError(f"HTTP status {status}", retryable=status == 429 or 500 <= status <= 599)
    if isinstance(exc, (httpx.TimeoutException, httpx.ConnectError)):
        return RpcTransportError("RPC transport timeout/connect failure", retryable=True)
    if isinstance(exc, (httpx.HTTPError, ValueError)):
        return RpcTransportError("RPC transport failure", retryable=False)
    return RpcTransportError("RPC transport failure", retryable=False)


class BaseRpcProvider:
    """One sanitized endpoint and five allowlisted JSON-RPC methods only."""

    def __init__(
        self,
        endpoint: str,
        *,
        transport: RpcPostTransport | None = None,
        profile: RpcCapabilityProfile | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._endpoint = _transport_endpoint(endpoint)
        self.endpoint = sanitize_rpc_endpoint(endpoint)
        configured = replace(profile or RpcCapabilityProfile())
        if configured.sanitized_endpoint and configured.sanitized_endpoint != self.endpoint:
            raise ValueError("profile sanitized_endpoint must equal provider endpoint")
        self._profile_snapshot = replace(configured, sanitized_endpoint=self.endpoint)
        self.profile = replace(self._profile_snapshot)
        self._transport = transport or _HttpxRpcTransport()
        self._sleep = sleep
        self._next_id = 1

    def _validated_profile(self) -> RpcCapabilityProfile:
        if self.profile != self._profile_snapshot:
            raise ValueError("provider profile was mutated")
        return self._profile_snapshot

    async def call(
        self, method: str, params: Sequence[object], *, on_attempt: Callable[[], None] | None = None
    ) -> RpcCallResult:
        profile = self._validated_profile()
        if type(method) is not str:
            raise RpcMethodError("unsupported RPC method")
        if method not in RPC_METHODS:
            raise RpcMethodError("unsupported RPC method")
        if not isinstance(params, Sequence) or isinstance(params, (str, bytes)):
            raise ValueError("RPC params must be a sequence")
        for attempt in range(1, profile.max_attempts + 1):
            if on_attempt is not None:
                on_attempt()
            request_id = self._next_id
            self._next_id += 1
            payload: dict[str, object] = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": list(params)}
            try:
                response = await self._transport.post_json(
                    self._endpoint, payload, timeout_seconds=float(profile.timeout_seconds)
                )
            except BaseException as exc:
                error = _transport_error(exc)
                if attempt == profile.max_attempts or not is_retryable_rpc_error(error, profile):
                    if attempt == profile.max_attempts and is_retryable_rpc_error(error, profile):
                        raise RpcTransportError("RPC retry exhausted", retryable=False, retry_exhausted=True) from None
                    raise error from None
                await self._sleep(0.25 * attempt)
                continue
            parsed = self._parse_response(method, request_id, attempt, response, profile)
            if (
                parsed.rpc_error is None
                or attempt == profile.max_attempts
                or not is_retryable_rpc_error(parsed.rpc_error, profile)
            ):
                return parsed
            await self._sleep(0.25 * attempt)
        raise RpcTransportError("RPC retry exhausted", retryable=False, retry_exhausted=True)

    def _parse_response(
        self, method: str, request_id: int, attempt_count: int, response: object, profile: RpcCapabilityProfile
    ) -> RpcCallResult:
        if not isinstance(response, Mapping) or "jsonrpc" not in response or response["jsonrpc"] != "2.0":
            raise RpcTransportError("malformed JSON-RPC response", retryable=False, malformed=True)
        if "id" not in response or type(response["id"]) is not int or response["id"] != request_id:
            raise RpcTransportError("JSON-RPC response id mismatch", retryable=False, malformed=True)
        if "error" in response:
            error = response["error"]
            if "result" in response:
                raise RpcTransportError("malformed JSON-RPC response", retryable=False, malformed=True)
            if (
                not isinstance(error, Mapping)
                or isinstance(error.get("code"), bool)
                or not isinstance(error.get("code"), int)
            ):
                raise RpcTransportError("malformed JSON-RPC error", retryable=False, malformed=True)
            return RpcCallResult(
                method=method,
                request_id=request_id,
                provider_id=profile.provider_id,
                sanitized_endpoint=self.endpoint,
                attempt_count=attempt_count,
                rpc_error=RpcResponseError(error["code"]),
            )
        if "result" not in response:
            raise RpcTransportError("JSON-RPC response missing result", retryable=False, malformed=True)
        return RpcCallResult(
            method=method,
            request_id=request_id,
            provider_id=profile.provider_id,
            sanitized_endpoint=self.endpoint,
            attempt_count=attempt_count,
            result=response["result"],
            has_result=True,
        )


__all__ = [
    "BaseRpcProvider",
    "RPC_METHODS",
    "RpcCallResult",
    "RpcCapabilityProfile",
    "RpcError",
    "RpcMethodError",
    "RpcPostTransport",
    "RpcResponseError",
    "RpcTransportError",
    "is_retryable_rpc_error",
    "sanitize_rpc_endpoint",
]
