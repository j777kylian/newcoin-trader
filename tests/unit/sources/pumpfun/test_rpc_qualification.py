"""Offline Pump RPC qualification boundary tests; injected transport only."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from newcoin_trader.sources.pumpfun.evidence import (
    PUMP_PROGRAM_ADDRESS,
    PumpMintSelectionProof,
    VerifiedPumpLaunchUniverse,
)
from newcoin_trader.sources.pumpfun.qualification import (
    PumpLiveSliceConfig,
    PumpLiveSliceResult,
    PumpQualificationConfig,
    PumpQualificationReceipt,
    PumpQualificationStatus,
    acquire_pump_live_slice,
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
    with pytest.raises(PumpRpcMethodError, match="requires finalized read-only parameters"):
        asyncio.run(
            provider.call(
                "getBlock", [100, {"commitment": "finalized", "encoding": "json", "transactionDetails": "full"}]
            )
        )
    for invalid_version in (False, 0.0):
        with pytest.raises(PumpRpcMethodError, match="requires finalized read-only parameters"):
            asyncio.run(
                provider.call(
                    "getBlock",
                    [
                        100,
                        {
                            "commitment": "finalized",
                            "encoding": "json",
                            "transactionDetails": "full",
                            "maxSupportedTransactionVersion": invalid_version,
                        },
                    ],
                )
            )
    block_provider = PumpRpcProvider("https://rpc.example.invalid", transport=FakeTransport([_response(1, {})]))
    assert (
        asyncio.run(
            block_provider.call(
                "getBlock",
                [
                    100,
                    {
                        "commitment": "finalized",
                        "encoding": "json",
                        "transactionDetails": "full",
                        "maxSupportedTransactionVersion": 0,
                    },
                ],
            )
        ).result
        == {}
    )
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


def test_qualification_waits_for_final_anchor_to_cover_retained_page() -> None:
    responses = _qualification_responses()
    responses[4] = _response(5, 100)
    responses.append(_response(6, 101))
    receipt = asyncio.run(
        qualify_pump_source(
            PumpRpcProvider("https://rpc.example.invalid", transport=FakeTransport(responses)),
            config=PumpQualificationConfig(page_limit=1, max_pages=1, attempt_cap=9),
        )
    )
    assert receipt.frozen_upper_slot == 101
    assert receipt.rpc_attempts == 6


def _live_slice_responses() -> list[object]:
    unsupported = "3N5R8Zy6iDbeNqjR9JH4tt3FjhHVwLwWLCwT6mKrRj4D"
    blockhash = "3u111111111111111111111111111111111111111111"
    responses = _qualification_responses()
    responses[3] = _response(
        4,
        [
            {"signature": SIGNATURE, "slot": 99, "err": None, "blockTime": 1_754_006_499},
            {"signature": unsupported, "slot": 98, "err": None, "blockTime": 1_754_006_498},
        ],
    )
    return responses[:4] + [
        _response(
            5,
            {
                "slot": 99,
                "meta": {"err": None, "innerInstructions": []},
                "transaction": {
                    "signatures": [SIGNATURE],
                    "message": {"instructions": [{"programId": "other", "data": "", "accounts": []}]},
                },
            },
        ),
        _response(
            6,
            {
                "blockhash": blockhash,
                "previousBlockhash": None,
                "blockHeight": 99,
                "blockTime": 1_754_006_499,
                "transactions": [{"transaction": {"signatures": [SIGNATURE]}}],
            },
        ),
        _response(7, 101),
    ]


def test_live_slice_fetches_only_capped_raw_evidence_and_preserves_unsupported_disposition() -> None:
    transport = FakeTransport(_live_slice_responses())
    result = asyncio.run(
        acquire_pump_live_slice(
            PumpRpcProvider("https://rpc.example.invalid/private?token=SECRET", transport=transport),
            config=PumpLiveSliceConfig(page_limit=2, candidate_cap=1, attempt_cap=9),
        )
    )

    assert [call[1]["method"] for call in transport.calls] == [
        "getSlot",
        "getAccountInfo",
        "getAccountInfo",
        "getSignaturesForAddress",
        "getTransaction",
        "getBlock",
        "getSlot",
    ]
    assert transport.calls[4][1]["params"] == [
        SIGNATURE,
        {"commitment": "finalized", "encoding": "jsonParsed", "maxSupportedTransactionVersion": 0},
    ]
    assert [item.disposition.value for item in result.signature_page.classifications] == [
        "unsupported_or_no_relevant_instruction",
        "candidate_cap_reached",
    ]
    assert result.signature_page.classifications[0].candidate is None
    assert result.first_buy is None
    assert result.status is PumpQualificationStatus.INCONCLUSIVE
    assert result.raw_binding_result == "FAIL"
    assert result.price_evidence_result == "DEFERRED"
    assert result.rpc_attempts == 7
    assert "SECRET" not in str(result)


def _live_buy_responses() -> list[object]:
    mint = "So11111111111111111111111111111111111111112"
    buy = "3N5R8Zy6iDbeNqjR9JH4tt3FjhHVwLwWLCwT6mKrRj4D"
    create = "2G4YbnQw5v5C5mR8h3D7sMjn6Qq5qP2e9u8Y4k6A1xT"
    blockhash = "3u111111111111111111111111111111111111111111"

    def transaction(signature: str, slot: int, data: str) -> dict[str, object]:
        return {
            "slot": slot,
            "meta": {"err": None, "innerInstructions": []},
            "transaction": {
                "signatures": [signature],
                "message": {
                    "instructions": [
                        {
                            "programId": PUMP_PROGRAM_ADDRESS,
                            "data": data,
                            "accounts": [SIGNATURE, SIGNATURE, mint, PROGRAMDATA],
                        }
                    ]
                },
            },
        }

    def block(signature: str, slot: int) -> dict[str, object]:
        return {
            "blockhash": blockhash,
            "previousBlockhash": None,
            "blockHeight": slot,
            "blockTime": 1_754_006_400 + slot,
            "transactions": [{"transaction": {"signatures": [signature]}}],
        }

    responses = _qualification_responses()
    responses[3] = _response(
        4,
        [
            {"signature": buy, "slot": 99, "err": None, "blockTime": 1_754_006_499},
            {"signature": create, "slot": 98, "err": None, "blockTime": 1_754_006_498},
        ],
    )
    return responses[:4] + [
        _response(5, transaction(buy, 99, "66063d1201daebea")),
        _response(6, block(buy, 99)),
        _response(7, transaction(create, 98, "181ec828051c0777")),
        _response(8, block(create, 98)),
        _response(9, 101),
    ]


def test_live_slice_selects_source_only_first_buy_only_from_matching_bounded_proof() -> None:
    config = PumpLiveSliceConfig(page_limit=2, candidate_cap=2, attempt_cap=12)
    seed = asyncio.run(
        acquire_pump_live_slice(
            PumpRpcProvider("https://rpc.example.invalid", transport=FakeTransport(_live_buy_responses())),
            config=config,
        )
    )
    proof = PumpMintSelectionProof(
        mint="So11111111111111111111111111111111111111112",
        universe=VerifiedPumpLaunchUniverse(
            profile=seed.profile,
            pages=(seed.signature_page,),
            lower_signature="2G4YbnQw5v5C5mR8h3D7sMjn6Qq5qP2e9u8Y4k6A1xT",
            upper_signature="3N5R8Zy6iDbeNqjR9JH4tt3FjhHVwLwWLCwT6mKrRj4D",
            terminal_reason="lower_bound_reached",
            terminal_cursor="2G4YbnQw5v5C5mR8h3D7sMjn6Qq5qP2e9u8Y4k6A1xT",
            expected_candidate_count=2,
            page_digests=(seed.signature_page.raw_page.raw_payload_digest,),
        ),
    )

    result = asyncio.run(
        acquire_pump_live_slice(
            PumpRpcProvider("https://rpc.example.invalid", transport=FakeTransport(_live_buy_responses())),
            config=config,
            selection_proof=proof,
        )
    )

    assert result.first_buy is not None
    assert result.first_buy.signature == "3N5R8Zy6iDbeNqjR9JH4tt3FjhHVwLwWLCwT6mKrRj4D"
    assert result.status is PumpQualificationStatus.INCONCLUSIVE
    assert result.raw_binding_result == "FAIL"
    assert result.price_evidence_result == "DEFERRED"
    with pytest.raises(ValueError, match="live slice"):
        result.model_copy(update={"status": PumpQualificationStatus.PASS, "rpc_attempts": 999})
    with pytest.raises(TypeError, match="not a public construction boundary"):
        PumpLiveSliceResult.model_construct(**result.__dict__)


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
    responses.extend(_response(request_id, 101) for request_id in range(6, 10))
    with pytest.raises(PumpRpcQualificationCapError, match="attempt cap exhausted"):
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
