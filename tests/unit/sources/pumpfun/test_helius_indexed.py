from __future__ import annotations

import pytest

from newcoin_trader.sources.pumpfun.evidence import PUMP_PROGRAM_ADDRESS
from newcoin_trader.sources.pumpfun.helius_indexed import (
    HeliusIndexedDiscoveryResult,
    HeliusIndexedError,
    HeliusIndexedHistoryClient,
    HeliusIndexedPumpDiscoveryProtocolV1,
    IndexedPumpCandidateClaim,
)

SIG = "3N5R8Zy6iDbeNqjR9JH4tt3FjhHVwLwWLCwT6mKrRj4D"
MINT = "So11111111111111111111111111111111111111112"
MARKET = "5Q544fKrFoe6tsEbCbWLZ8cM1iPV5QpZ2AJwFXB8i8VB"


def _row(*, source_time: int = 110, slot: int = 20) -> dict[str, object]:
    return {
        "slot": slot,
        "blockTime": source_time,
        "transactionIndex": 0,
        "meta": {"err": None, "innerInstructions": []},
        "transaction": {
            "signatures": [SIG],
            "message": {
                "instructions": [
                    {
                        "programId": PUMP_PROGRAM_ADDRESS,
                        "data": "TQC5U4sttDH1Zpfx",
                        "accounts": [SIG, SIG, MINT, MARKET],
                    }
                ]
            },
        },
    }


class _Transport:
    def __init__(self, responses: list[object]) -> None:
        self.responses = responses
        self.calls: list[dict[str, object]] = []

    async def post_json(self, endpoint: str, payload: dict[str, object], *, timeout_seconds: float) -> object:
        self.calls.append(payload)
        return self.responses.pop(0)


def _response(request_id: int, data: list[object], token: str | None = None) -> dict[str, object]:
    result: dict[str, object] = {"data": data}
    if token is not None:
        result["paginationToken"] = token
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


@pytest.mark.asyncio
async def test_discovery_binds_complete_pages_and_source_only_claims() -> None:
    transport = _Transport([_response(1, [_row()], "next"), _response(2, [])])
    result = await HeliusIndexedHistoryClient(
        "https://example.test/private", transport=transport, sleep=lambda _: _noop()
    ).discover(HeliusIndexedPumpDiscoveryProtocolV1(window_start=100, window_end=120))
    assert result.raw_transaction_count == 1
    assert result.deduplicated_candidate_count == 1
    assert result.terminal_proof == "null_paginationToken"
    assert result.pages[1].request_token_digest == result.pages[0].response_token_digest
    claim = result.pages[0].candidate_claims[0]
    assert (claim.method, claim.source_time) == ("create", 110)
    with pytest.raises(TypeError):
        IndexedPumpCandidateClaim.model_construct(**claim.model_dump())
    with pytest.raises(TypeError):
        HeliusIndexedDiscoveryResult.model_construct(**result.model_dump())
    with pytest.raises(ValueError, match="digest"):
        claim.model_copy(update={"slot": 21})


@pytest.mark.asyncio
async def test_discovery_rejects_looping_token_and_out_of_window_row() -> None:
    loop = _Transport([_response(1, [], "again"), _response(2, [], "again")])
    protocol = HeliusIndexedPumpDiscoveryProtocolV1(window_start=100, window_end=120)
    client = HeliusIndexedHistoryClient("https://example.test", transport=loop, sleep=lambda _: _noop())
    with pytest.raises(HeliusIndexedError, match="non-advancing"):
        await client.discover(protocol)
    duplicate = _Transport([_response(1, [_row(), _row()])])
    with pytest.raises(ValueError, match="duplicate indexed transaction"):
        await HeliusIndexedHistoryClient("https://example.test", transport=duplicate).discover(protocol)
    out = _Transport([_response(1, [_row(source_time=120)])])
    with pytest.raises(HeliusIndexedError, match="outside"):
        await HeliusIndexedHistoryClient("https://example.test", transport=out).discover(protocol)


async def _noop() -> None:
    return None
