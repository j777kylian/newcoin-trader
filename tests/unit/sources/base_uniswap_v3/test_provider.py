"""Offline JSON-RPC boundary tests for Phase 8C.6."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from newcoin_trader.sources.base_uniswap_v3.provider import (
    BaseRpcProvider,
    RpcCapabilityProfile,
    RpcMethodError,
    RpcTransportError,
    sanitize_rpc_endpoint,
)


class FakeTransport:
    def __init__(self, responses: list[object]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, object], float]] = []

    async def post_json(self, endpoint: str, payload: dict[str, object], *, timeout_seconds: float) -> object:
        self.calls.append((endpoint, payload, timeout_seconds))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_provider_posts_only_allowlisted_rpc_method_and_returns_a_contextual_result() -> None:
    transport = FakeTransport([{"jsonrpc": "2.0", "id": 1, "result": "0x2105"}])
    provider = BaseRpcProvider("https://rpc.example.invalid/", transport=transport)

    async def run() -> None:
        response = await provider.call("eth_chainId", [])
        assert response.result == "0x2105"
        assert response.rpc_error is None
        assert response.method == "eth_chainId"
        assert response.request_id == 1
        assert response.provider_id == provider.profile.provider_id
        assert response.sanitized_endpoint == "https://rpc.example.invalid"
        assert response.attempt_count == 1
        assert transport.calls == [
            ("https://rpc.example.invalid", {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}, 15.0)
        ]
        with pytest.raises(RpcMethodError):
            await provider.call("eth_blockNumber", [])

    asyncio.run(run())


def test_provider_retries_injected_http_429_then_returns_the_second_result() -> None:
    request = httpx.Request("POST", "https://rpc.example.invalid/secret")
    response = httpx.Response(429, request=request)
    transport = FakeTransport(
        [
            httpx.HTTPStatusError("429 https://rpc.example.invalid/secret", request=request, response=response),
            {"jsonrpc": "2.0", "id": 2, "result": "0x2105"},
        ]
    )
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    result = asyncio.run(
        BaseRpcProvider("https://rpc.example.invalid", transport=transport, sleep=sleep).call("eth_chainId", [])
    )
    assert result.result == "0x2105"
    assert result.attempt_count == 2
    assert len(transport.calls) == 2
    assert sleeps == [0.25]


def test_provider_exposes_rpc_error_without_confusing_it_with_a_result_and_retries_only_profile_codes() -> None:
    profile = RpcCapabilityProfile(retryable_rpc_codes=(-32005,), range_limit_rpc_codes=(-32016,))
    transport = FakeTransport([{"jsonrpc": "2.0", "id": 1, "error": {"code": -32016, "message": "secret endpoint"}}])
    result = asyncio.run(
        BaseRpcProvider("https://rpc.example.invalid", transport=transport, profile=profile).call("eth_chainId", [])
    )
    assert result.result is None
    assert result.rpc_error is not None
    assert result.rpc_error.code == -32016
    assert "secret" not in str(result.rpc_error)
    assert len(transport.calls) == 1


def test_provider_preserves_a_valid_null_result_and_counts_each_retry_attempt() -> None:
    transport = FakeTransport(
        [
            httpx.ConnectError("https://rpc.example.invalid/private"),
            {"jsonrpc": "2.0", "id": 2, "result": None},
        ]
    )
    attempts: list[str] = []

    result = asyncio.run(
        BaseRpcProvider("https://rpc.example.invalid", transport=transport, sleep=lambda _: asyncio.sleep(0)).call(
            "eth_getTransactionReceipt", ["0x" + "11" * 32], on_attempt=lambda: attempts.append("attempt")
        )
    )

    assert result.result is None
    assert result.rpc_error is None
    assert result.has_result is True
    assert attempts == ["attempt", "attempt"]


@pytest.mark.parametrize(
    "failure",
    [
        httpx.HTTPStatusError(
            "hidden",
            request=httpx.Request("POST", "https://rpc.example.invalid/secret"),
            response=httpx.Response(503, request=httpx.Request("POST", "https://rpc.example.invalid/secret")),
        ),
        httpx.TimeoutException("https://rpc.example.invalid/secret"),
    ],
)
def test_provider_retries_retryable_transport_failures_then_reports_sanitized_exhaustion(failure: Exception) -> None:
    transport = FakeTransport([failure, failure, failure])
    with pytest.raises(RpcTransportError) as raised:
        asyncio.run(BaseRpcProvider("https://rpc.example.invalid", transport=transport).call("eth_chainId", []))
    assert raised.value.retry_exhausted is True
    assert "secret" not in str(raised.value)
    assert len(transport.calls) == 3


def test_provider_refuses_nonretryable_and_malformed_json_without_endpoint_secret() -> None:
    transport = FakeTransport([{"jsonrpc": "2.0", "id": 1, "result": "0x1", "error": {"code": -1}}])
    with pytest.raises(RpcTransportError) as raised:
        asyncio.run(BaseRpcProvider("https://rpc.example.invalid", transport=transport).call("eth_chainId", []))
    assert raised.value.retryable is False
    assert "secret" not in str(raised.value)
    assert len(transport.calls) == 1


def test_profile_has_bound_base_capabilities_and_exact_candidate_ceilings() -> None:
    profile = RpcCapabilityProfile()
    assert profile.profile_version
    assert profile.provider_id
    assert profile.chain_id == 8453
    assert profile.supports_finalized_tag is True
    assert profile.supports_eip1898_block_hash_call is True
    assert profile.archive_eth_call_available is True
    assert profile.receipt_history_available is True
    assert profile.factory_candidate_ceiling == 50_000
    assert profile.exact_pool_candidate_ceiling == 100_000
    with pytest.raises(ValueError):
        RpcCapabilityProfile(factory_candidate_ceiling=50_001)
    with pytest.raises(ValueError):
        RpcCapabilityProfile(exact_pool_candidate_ceiling=100_001)


def test_profile_and_endpoint_are_bounded_and_safe() -> None:
    assert sanitize_rpc_endpoint(" https://RPC.example.invalid/path/ ") == "https://rpc.example.invalid"
    with pytest.raises(ValueError):
        sanitize_rpc_endpoint("http://rpc.example.invalid")
    with pytest.raises(ValueError):
        sanitize_rpc_endpoint("https://user:pass@rpc.example.invalid")
    with pytest.raises(ValueError):
        RpcCapabilityProfile(max_block_span=50_001)
    with pytest.raises(ValueError):
        RpcCapabilityProfile(timeout_seconds=15.1)


def test_provider_strictly_types_methods_and_json_rpc_response_keys_and_ids() -> None:
    class StringSubclass(str):
        pass

    with pytest.raises(RpcMethodError):
        provider = BaseRpcProvider("https://rpc.example.invalid", transport=FakeTransport([]))
        asyncio.run(provider.call(StringSubclass("eth_chainId"), []))
    for response in (
        {"id": 1, "result": "0x2105"},
        {"jsonrpc": "2.0", "result": "0x2105"},
        {"jsonrpc": "2.0", "id": True, "result": "0x2105"},
    ):
        with pytest.raises(RpcTransportError) as raised:
            provider = BaseRpcProvider("https://rpc.example.invalid", transport=FakeTransport([response]))
            asyncio.run(provider.call("eth_chainId", []))
        assert raised.value.malformed is True


def test_provider_keeps_raw_endpoint_transport_only_and_exposes_only_origin_provenance() -> None:
    transport = FakeTransport([{"jsonrpc": "2.0", "id": 1, "result": "0x2105"}])
    provider = BaseRpcProvider("https://RPC.example.invalid/private-key?api=secret", transport=transport)
    result = asyncio.run(provider.call("eth_chainId", []))
    assert transport.calls[0][0] == "https://RPC.example.invalid/private-key?api=secret"
    assert provider.profile.sanitized_endpoint == "https://rpc.example.invalid"
    assert result.sanitized_endpoint == "https://rpc.example.invalid"
    assert "secret" not in str(result)


def test_provider_sanitizes_arbitrary_injected_transport_runtime_errors() -> None:
    transport = FakeTransport([RuntimeError("SECRET_API_KEY=https://rpc.example.invalid/private")])
    with pytest.raises(RpcTransportError) as raised:
        asyncio.run(BaseRpcProvider("https://rpc.example.invalid", transport=transport).call("eth_chainId", []))
    assert raised.value.retryable is False
    assert "SECRET_API_KEY" not in str(raised.value)
    assert "private" not in str(raised.value)


def test_provider_normalizes_injected_rpc_errors_without_their_secret_text() -> None:
    injected = RpcTransportError("TRANSPORT_SECRET=https://rpc.example.invalid/private", retryable=True)
    transport = FakeTransport([injected])

    with pytest.raises(RpcTransportError, match="^RPC transport failure$") as raised:
        asyncio.run(BaseRpcProvider("https://rpc.example.invalid", transport=transport).call("eth_chainId", []))

    assert raised.value is not injected
    assert raised.value.retryable is False
    assert len(transport.calls) == 1
    assert "TRANSPORT_SECRET" not in str(raised.value)
    assert "private" not in str(raised.value)


def test_provider_normalizes_injected_base_exception_without_secret_text() -> None:
    secret = "BASE_EXCEPTION_SECRET=https://rpc.example.invalid/private"

    class BaseExceptionTransport:
        async def post_json(self, endpoint: str, payload: dict[str, object], *, timeout_seconds: float) -> object:
            raise BaseException(secret)

    with pytest.raises(RpcTransportError, match="^RPC transport failure$") as raised:
        asyncio.run(
            BaseRpcProvider("https://rpc.example.invalid", transport=BaseExceptionTransport()).call("eth_chainId", [])
        )

    assert type(raised.value) is RpcTransportError
    assert raised.value.retryable is False
    assert raised.value.retry_exhausted is False
    assert secret not in str(raised.value)


def test_provider_rejects_even_valid_public_profile_mutation_before_transport() -> None:
    transport = FakeTransport([])
    provider = BaseRpcProvider("https://rpc.example.invalid", transport=transport)
    object.__setattr__(provider.profile, "max_attempts", 1)

    with pytest.raises(ValueError, match="provider profile was mutated"):
        asyncio.run(provider.call("eth_chainId", []))

    assert transport.calls == []


def test_profile_reconstruction_rejects_mutation_and_range_limit_codes_do_not_retry() -> None:
    profile = RpcCapabilityProfile(retryable_rpc_codes=(-32016,), range_limit_rpc_codes=(-32005,))
    with pytest.raises(ValueError):
        RpcCapabilityProfile(retryable_rpc_codes=(-32005,), range_limit_rpc_codes=(-32005,))
    transport = FakeTransport([{"jsonrpc": "2.0", "id": 1, "error": {"code": -32005, "message": "secret"}}])
    provider = BaseRpcProvider("https://rpc.example.invalid", transport=transport, profile=profile)
    result = asyncio.run(provider.call("eth_getLogs", []))
    assert result.rpc_error is not None and result.rpc_error.code == -32005
    assert len(transport.calls) == 1
    provider = BaseRpcProvider("https://rpc.example.invalid", transport=FakeTransport([]))
    object.__setattr__(provider.profile, "max_attempts", 4)
    with pytest.raises(ValueError):
        asyncio.run(provider.call("eth_chainId", []))
