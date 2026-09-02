"""Pump-local bounded JSON-RPC POST boundary for qualification only."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol
from urllib.parse import urlsplit, urlunsplit

import httpx

PUMP_RPC_METHODS = frozenset(
    {"getSlot", "getAccountInfo", "getSignaturesForAddress", "getTransaction", "getBlock", "getBlockTime"}
)


class PumpRpcError(RuntimeError):
    """Sanitized Pump RPC boundary error."""


class PumpRpcMethodError(PumpRpcError):
    pass


class PumpRpcTransportError(PumpRpcError):
    pass


class PumpRpcHistoryUnavailableError(PumpRpcError):
    pass


class PumpRpcQualificationCapError(PumpRpcError):
    pass


@dataclass(frozen=True)
class PumpRpcCallResult:
    method: str
    provider_origin: str
    attempt_count: int
    result: object = field(repr=False)


class PumpRpcPostTransport(Protocol):
    async def post_json(self, endpoint: str, payload: dict[str, object], *, timeout_seconds: float) -> object: ...


class _HttpxPumpRpcTransport:
    async def post_json(self, endpoint: str, payload: dict[str, object], *, timeout_seconds: float) -> object:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds)) as client:
            response = await client.post(endpoint, json=payload)
            response.raise_for_status()
            return response.json()


def sanitize_pump_rpc_endpoint(endpoint: object) -> str:
    """Expose only a credential-free HTTPS origin; transport retains its explicit input."""
    if not isinstance(endpoint, str):
        raise ValueError("Pump RPC endpoint must be a string")
    parsed = urlsplit(endpoint.strip())
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password or parsed.fragment:
        raise ValueError("Pump RPC endpoint must be credential-free HTTPS")
    try:
        port = parsed.port
    except ValueError as error:
        raise ValueError("Pump RPC endpoint port is malformed") from error
    if parsed.hostname is None:
        raise ValueError("Pump RPC endpoint host required")
    host = parsed.hostname.lower()
    rendered = f"[{host}]" if ":" in host else host
    return urlunsplit(("https", rendered if port is None else f"{rendered}:{port}", "", "", ""))


def _transport_endpoint(endpoint: str) -> str:
    sanitize_pump_rpc_endpoint(endpoint)
    parsed = urlsplit(endpoint.strip())
    return urlunsplit(("https", parsed.netloc, parsed.path.rstrip("/"), parsed.query, ""))


def _finalized_config(value: object) -> bool:
    return isinstance(value, Mapping) and value.get("commitment") == "finalized"


def _validate_method_params(method: str, params: Sequence[object]) -> None:
    if method == "getSlot":
        valid = len(params) == 1 and _finalized_config(params[0])
    elif method == "getAccountInfo":
        valid = (
            len(params) == 2
            and isinstance(params[0], str)
            and _finalized_config(params[1])
            and isinstance(params[1], Mapping)
            and params[1].get("encoding") == "jsonParsed"
        )
    elif method == "getSignaturesForAddress":
        valid = len(params) == 2 and isinstance(params[0], str) and _finalized_config(params[1])
    elif method == "getTransaction":
        valid = (
            len(params) == 2
            and isinstance(params[0], str)
            and _finalized_config(params[1])
            and isinstance(params[1], Mapping)
            and params[1].get("encoding") == "jsonParsed"
            and type(params[1].get("maxSupportedTransactionVersion")) is int
            and params[1].get("maxSupportedTransactionVersion") == 0
        )
    elif method == "getBlock":
        valid = (
            len(params) == 2
            and isinstance(params[0], int)
            and not isinstance(params[0], bool)
            and _finalized_config(params[1])
            and isinstance(params[1], Mapping)
            and params[1].get("encoding") == "json"
            and params[1].get("transactionDetails") == "signatures"
            and type(params[1].get("maxSupportedTransactionVersion")) is int
            and params[1].get("maxSupportedTransactionVersion") == 0
        )
    else:  # getBlockTime has no commitment parameter.
        valid = len(params) == 1 and isinstance(params[0], int) and not isinstance(params[0], bool)
    if not valid:
        raise PumpRpcMethodError("Pump RPC method requires finalized read-only parameters")


def _retryable_exception(error: BaseException) -> bool:
    return isinstance(error, (httpx.TimeoutException, httpx.ConnectError)) or (
        isinstance(error, httpx.HTTPStatusError)
        and (error.response.status_code == 429 or error.response.status_code >= 500)
    )


class PumpRpcProvider:
    """Explicit-endpoint, read-only Pump RPC client; no qualification is invoked at construction."""

    def __init__(
        self,
        endpoint: str,
        *,
        transport: PumpRpcPostTransport | None = None,
        timeout_seconds: float = 15.0,
        max_attempts: int = 3,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not 0 < timeout_seconds <= 15
        ):
            raise ValueError("Pump RPC timeout must be in (0, 15]")
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or not 1 <= max_attempts <= 3:
            raise ValueError("Pump RPC max attempts must be in 1..3")
        self._endpoint = _transport_endpoint(endpoint)
        self.provider_origin = sanitize_pump_rpc_endpoint(endpoint)
        self._transport = transport or _HttpxPumpRpcTransport()
        self._timeout_seconds = float(timeout_seconds)
        self._max_attempts = max_attempts
        self._sleep = sleep
        self._next_id = 1

    async def call(
        self, method: str, params: Sequence[object], *, attempt_budget: list[int] | None = None
    ) -> PumpRpcCallResult:
        if type(method) is not str or method not in PUMP_RPC_METHODS:
            raise PumpRpcMethodError("unsupported RPC method")
        if not isinstance(params, Sequence) or isinstance(params, (str, bytes)):
            raise PumpRpcMethodError("Pump RPC params must be a sequence")
        _validate_method_params(method, params)
        attempt_budget = [500] if attempt_budget is None else attempt_budget
        if len(attempt_budget) != 1 or type(attempt_budget[0]) is not int or not 0 <= attempt_budget[0] <= 500:
            raise ValueError("Pump qualification attempt budget is invalid")
        for attempt in range(1, self._max_attempts + 1):
            if attempt_budget[0] < 1:
                raise PumpRpcQualificationCapError("qualification attempt cap exhausted")
            attempt_budget[0] -= 1
            request_id = self._next_id
            self._next_id += 1
            payload: dict[str, object] = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": list(params)}
            try:
                raw = await self._transport.post_json(self._endpoint, payload, timeout_seconds=self._timeout_seconds)
                result, retryable_rpc, rpc_error, rpc_error_code = self._parse(request_id, raw)
            except BaseException as error:
                if attempt < self._max_attempts and _retryable_exception(error):
                    await self._sleep(0.25 * attempt)
                    continue
                raise PumpRpcTransportError("Pump RPC transport failure") from None
            if retryable_rpc:
                if attempt < self._max_attempts:
                    await self._sleep(0.25 * attempt)
                    continue
                raise PumpRpcTransportError("Pump RPC retry exhausted")
            if rpc_error:
                if rpc_error_code == -32004:
                    raise PumpRpcHistoryUnavailableError("Pump RPC history unavailable")
                raise PumpRpcTransportError("Pump RPC response error")
            return PumpRpcCallResult(
                method=method, provider_origin=self.provider_origin, attempt_count=attempt, result=result
            )
        raise PumpRpcTransportError("Pump RPC retry exhausted")

    @staticmethod
    def _parse(request_id: int, raw: object) -> tuple[object, bool, bool, int | None]:
        if not isinstance(raw, Mapping) or raw.get("jsonrpc") != "2.0" or type(raw.get("id")) is not int:
            raise PumpRpcTransportError("malformed Pump RPC response")
        if raw["id"] != request_id or ("result" in raw) == ("error" in raw):
            raise PumpRpcTransportError("malformed Pump RPC response")
        if "error" in raw:
            error = raw["error"]
            if not isinstance(error, Mapping) or type(error.get("code")) is not int:
                raise PumpRpcTransportError("malformed Pump RPC response")
            return None, error["code"] in {-32005}, True, error["code"]
        return raw["result"], False, False, None


__all__ = [
    "PUMP_RPC_METHODS",
    "PumpRpcCallResult",
    "PumpRpcError",
    "PumpRpcMethodError",
    "PumpRpcPostTransport",
    "PumpRpcProvider",
    "PumpRpcQualificationCapError",
    "PumpRpcTransportError",
    "sanitize_pump_rpc_endpoint",
]
