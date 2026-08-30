"""Offline Pump RPC qualification boundary tests; injected transport only."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from newcoin_trader.sources.pumpfun.evidence import PUMP_PROGRAM_ADDRESS
from newcoin_trader.sources.pumpfun.qualification import (
    PumpQualificationConfig,
    PumpQualificationReceipt,
    PumpQualificationStatus,
    qualify_pump_source,
)
from newcoin_trader.sources.pumpfun.rpc import (
    PUMP_RPC_METHODS,
    PumpRpcMethodError,
    PumpRpcProvider,
    PumpRpcQualificationCapError,
    PumpRpcTransportError,
    sanitize_pump_rpc_endpoint,
)

PROGRAMDATA = "5Q544fKrFoe6tsEbCbWLZ8cM1iPV5QpZ2AJwFXB8i8VB"
SIGNATURE = "4vJ9JU1bJJE96FWSJKvHsmmFQy9rato4sVZ2BJTaL7h"
LOADER = "BPFLoaderUpgradeab1e11111111111111111111111"


class FakeTransport:
    def __init__(self, responses: list[object]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, object], float]] = []

    async def post_json(self, endpoint: str, payload: dict[str, object], *, timeout_seconds: float) -> object:
        self.calls.append((endpoint, payload, timeout_seconds))
        value = self.responses.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value


def _response(request_id: int, result: object) -> dict[str, object]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _qualification_responses() -> list[object]:
    return [
        _response(1, 100),
        _response(
            2,
            {
                "context": {"slot": 101},
                "value": {
                    "owner": LOADER,
                    "data": {"parsed": {"type": "program", "info": {"programData": PROGRAMDATA}}},
                },
            },
        ),
        _response(
            3,
            {
                "context": {"slot": 101},
                "value": {
                    "owner": LOADER,
                    "data": {"parsed": {"type": "programData", "info": {"slot": 88}}},
                },
            },
        ),
        _response(
            4, [{"signature": SIGNATURE, "slot": 99, "err": {"InstructionError": 0}, "blockTime": 1_754_006_499}]
        ),
        _response(5, 101),
    ]


def test_pump_rpc_allowlist_and_endpoint_origin_never_expose_path_or_query() -> None:
    transport = FakeTransport([_response(1, 100)])
    provider = PumpRpcProvider("https://RPC.example.invalid/secret-path?api=TOP_SECRET", transport=transport)
    result = asyncio.run(provider.call("getSlot", [{"commitment": "finalized"}]))

    assert PUMP_RPC_METHODS == frozenset(
        {"getSlot", "getAccountInfo", "getSignaturesForAddress", "getTransaction", "getBlock", "getBlockTime"}
    )
    assert result.provider_origin == "https://rpc.example.invalid"
    assert transport.calls[0][0].endswith("secret-path?api=TOP_SECRET")
    assert "TOP_SECRET" not in str(result)
    with pytest.raises(PumpRpcMethodError, match="unsupported RPC method"):
        asyncio.run(provider.call("sendTransaction", []))
    assert (
        sanitize_pump_rpc_endpoint("https://RPC.example.invalid/secret?api=TOP_SECRET") == "https://rpc.example.invalid"
    )


def test_pump_rpc_shared_cap_counts_retry_attempts_before_transport() -> None:
    request = httpx.Request("POST", "https://rpc.example.invalid/secret")
    transport = FakeTransport(
        [
            httpx.ConnectError("LEAKED=https://rpc.example.invalid/secret", request=request),
            _response(2, 100),
        ]
    )
    provider = PumpRpcProvider(
        "https://rpc.example.invalid/secret",
        transport=transport,
        max_attempts=2,
        sleep=lambda _: asyncio.sleep(0),
    )

    result = asyncio.run(provider.call("getSlot", [{"commitment": "finalized"}], attempt_budget=[2]))
    assert result.attempt_count == 2
    assert len(transport.calls) == 2
    assert "LEAKED" not in str(result)
    with pytest.raises(PumpRpcQualificationCapError, match="qualification attempt cap exhausted"):
        asyncio.run(provider.call("getSlot", [{"commitment": "finalized"}], attempt_budget=[0]))
    assert len(transport.calls) == 2


def test_qualification_freezes_finalized_upper_slot_before_program_and_page_traversal() -> None:
    transport = FakeTransport(_qualification_responses())
    receipt = asyncio.run(
        qualify_pump_source(
            PumpRpcProvider("https://rpc.example.invalid/private?token=SECRET", transport=transport),
            config=PumpQualificationConfig(page_limit=1, max_pages=1, attempt_cap=9),
        )
    )

    assert [call[1]["method"] for call in transport.calls] == [
        "getSlot",
        "getAccountInfo",
        "getAccountInfo",
        "getSignaturesForAddress",
        "getSlot",
    ]
    assert transport.calls[0][1]["params"] == [{"commitment": "finalized"}]
    assert transport.calls[1][1]["params"] == [
        PUMP_PROGRAM_ADDRESS,
        {"commitment": "finalized", "encoding": "jsonParsed", "withContext": True},
    ]
    assert transport.calls[2][1]["params"] == [
        PROGRAMDATA,
        {"commitment": "finalized", "encoding": "jsonParsed", "withContext": True},
    ]
    assert transport.calls[-2][1]["params"] == [
        PUMP_PROGRAM_ADDRESS,
        {"commitment": "finalized", "limit": 1},
    ]
    assert transport.calls[-1][1]["params"] == [{"commitment": "finalized"}]
    assert receipt.frozen_upper_slot == 101
    assert receipt.frozen_lower_slot == 99
    assert receipt.candidate_count == 1
    assert receipt.decoded_count == receipt.first_buy_count == 0
    assert receipt.ambiguous_count == 1
    assert receipt.rpc_attempts == 5
    assert receipt.status is PumpQualificationStatus.INCONCLUSIVE
    assert receipt.price_evidence_result == "DEFERRED"
    assert receipt.provider_origin == "https://rpc.example.invalid"
    assert receipt.raw_binding_result == "FAIL"
    assert receipt.receipt_digest
    assert "SECRET" not in str(receipt)


def test_qualification_never_leaks_provider_response_or_exception_text() -> None:
    transport = FakeTransport(
        [
            {"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "RAW_BODY_SECRET=abc"}},
        ]
    )
    provider = PumpRpcProvider("https://rpc.example.invalid/private?token=SECRET", transport=transport)
    with pytest.raises(PumpRpcTransportError) as raised:
        asyncio.run(qualify_pump_source(provider))
    assert "RAW_BODY_SECRET" not in str(raised.value)
    assert "SECRET" not in str(raised.value)

    provider = PumpRpcProvider(
        "https://rpc.example.invalid/private?token=SECRET",
        transport=FakeTransport([RuntimeError("EXCEPTION_SECRET=https://rpc.example.invalid/private")]),
    )
    with pytest.raises(PumpRpcTransportError) as raised:
        asyncio.run(qualify_pump_source(provider))
    assert "EXCEPTION_SECRET" not in str(raised.value)
    assert "SECRET" not in str(raised.value)

    provider = PumpRpcProvider(
        "https://rpc.example.invalid/private?token=SECRET",
        transport=FakeTransport([PumpRpcTransportError("TYPED_SECRET=/private?token=SECRET")]),
    )
    with pytest.raises(PumpRpcTransportError) as raised:
        asyncio.run(qualify_pump_source(provider))
    assert "TYPED_SECRET" not in str(raised.value)
    assert "SECRET" not in str(raised.value)


def test_qualification_output_module_has_no_wallet_or_trading_path() -> None:
    import inspect

    import newcoin_trader.sources.pumpfun.qualification as qualification

    source = inspect.getsource(qualification).lower()
    assert "wallet" not in source
    assert "sendtransaction" not in source
    assert "trading" not in source


def test_receipt_copy_and_construct_cannot_forge_status_or_digest() -> None:
    receipt = asyncio.run(
        qualify_pump_source(
            PumpRpcProvider("https://rpc.example.invalid", transport=FakeTransport(_qualification_responses())),
            config=PumpQualificationConfig(page_limit=1, max_pages=1, attempt_cap=9),
        )
    )

    with pytest.raises(ValueError, match="status"):
        receipt.model_copy(update={"status": PumpQualificationStatus.PASS})
    with pytest.raises(TypeError, match="not a public construction boundary"):
        PumpQualificationReceipt.model_construct(**{**receipt.__dict__, "status": PumpQualificationStatus.PASS})
    with pytest.raises(ValueError, match="actual raw account, page, and terminal evidence"):
        receipt.model_copy(update={"raw_account_digest": "0" * 64})


def test_qualification_rejects_programdata_context_or_deploy_slot_after_frozen_upper_slot() -> None:
    responses = _qualification_responses()
    programdata = responses[2]
    assert isinstance(programdata, dict)
    programdata["result"] = {
        "context": {"slot": 102},
        "value": {
            "owner": LOADER,
            "data": {"parsed": {"type": "programData", "info": {"slot": 102}}},
        },
    }
    with pytest.raises(ValueError, match="evidence exceeds frozen finalized upper slot"):
        asyncio.run(
            qualify_pump_source(
                PumpRpcProvider("https://rpc.example.invalid", transport=FakeTransport(responses)),
                config=PumpQualificationConfig(page_limit=1, max_pages=1, attempt_cap=9),
            )
        )


def test_receipt_rejects_recomputed_terminal_proof_tampering() -> None:
    receipt = asyncio.run(
        qualify_pump_source(
            PumpRpcProvider("https://rpc.example.invalid", transport=FakeTransport(_qualification_responses())),
            config=PumpQualificationConfig(page_limit=1, max_pages=1, attempt_cap=9),
        )
    )
    with pytest.raises(ValueError, match="configured page cap"):
        receipt.model_copy(
            update={
                "page_count": 3,
                "terminal_cursor": "attacker-controlled-terminal",
                "terminal_reason": "exhausted",
            }
        )
    with pytest.raises(ValueError, match="configured page cap"):
        receipt.model_copy(update={"terminal_reason": "lower_bound_reached"})
    with pytest.raises(TypeError, match="not a public construction boundary"):
        PumpQualificationReceipt.model_construct(**{**receipt.__dict__, "terminal_reason": "lower_bound_reached"})


def test_receipt_rejects_frozen_slot_tampering_against_raw_evidence() -> None:
    receipt = asyncio.run(
        qualify_pump_source(
            PumpRpcProvider("https://rpc.example.invalid", transport=FakeTransport(_qualification_responses())),
            config=PumpQualificationConfig(page_limit=1, max_pages=1, attempt_cap=9),
        )
    )
    with pytest.raises(ValueError, match="frozen slot interval"):
        receipt.model_copy(update={"frozen_upper_slot": 99})
    with pytest.raises(TypeError, match="not a public construction boundary"):
        PumpQualificationReceipt.model_construct(**{**receipt.__dict__, "frozen_lower_slot": 98})
    with pytest.raises(ValueError, match="receipt digest does not bind content"):
        receipt.model_copy(update={"frozen_upper_slot": 102})
    with pytest.raises(ValueError, match="attempt accounting"):
        receipt.model_copy(update={"rpc_attempts": 501})
    with pytest.raises(ValueError, match="provider origin must be sanitized"):
        receipt.model_copy(update={"provider_origin": "https://rpc.example.invalid/secret?token=LEAK"})
    with pytest.raises(ValueError, match="controlled factory boundary"):
        PumpQualificationReceipt.model_validate(dict(receipt.__dict__))
    with pytest.raises(ValueError, match="exactly one page"):
        PumpQualificationConfig(max_pages=2)
    for non_integer in (True, "1", 1.0):
        with pytest.raises(ValueError):
            PumpQualificationConfig(max_pages=non_integer)  # type: ignore[arg-type]
    assert not hasattr(PumpQualificationReceipt, "create")
