from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime

import httpx
import pytest

from newcoin_trader.sources.pumpfun.evidence import PUMP_PROGRAM_ADDRESS
from newcoin_trader.sources.pumpfun.helius_indexed import (
    HeliusIndexedDiscoveryResult,
    HeliusIndexedError,
    HeliusIndexedHistoryClient,
    HeliusIndexedPumpDiscoveryProtocolV1,
    IndexedPumpCandidateClaim,
    PumpCorpusV2WindowPlan,
    recover_pump_corpus_v2_source_manifest,
    recover_pump_corpus_v2_window_plan,
    write_pump_corpus_v2_source_manifest,
    write_pump_corpus_v2_window_plan,
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
                        "accounts": [
                            MINT,
                            SIG,
                            MARKET,
                            "11111111111111111111111111111112",
                            "11111111111111111111111111111113",
                            "11111111111111111111111111111114",
                            "11111111111111111111111111111115",
                            SIG,
                            "11111111111111111111111111111116",
                            "11111111111111111111111111111117",
                            "11111111111111111111111111111118",
                            "11111111111111111111111111111119",
                            "1111111111111111111111111111111A",
                            "1111111111111111111111111111111B",
                        ],
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
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


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
async def test_discovery_retries_transient_transport_failure_within_frozen_attempt_cap() -> None:
    transport = _Transport([httpx.ConnectError("transient transport failure"), _response(2, [])])
    result = await HeliusIndexedHistoryClient(
        "https://example.test", transport=transport, sleep=lambda _: _noop()
    ).discover(HeliusIndexedPumpDiscoveryProtocolV1(window_start=100, window_end=120, max_attempts=2))
    assert result.usage.attempts == 2
    assert len(transport.calls) == 2


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
    unsafe = _Transport([RuntimeError("https://host.example/path?api-key=SECRET")])
    with pytest.raises(HeliusIndexedError) as error:
        await HeliusIndexedHistoryClient("https://example.test", transport=unsafe).discover(protocol)
    assert "SECRET" not in str(error.value)


def test_corpus_v2_freeze_persists_mint_free_source_coordinates_for_fresh_recovery(tmp_path) -> None:
    plan = PumpCorpusV2WindowPlan.freeze(reference_time=datetime(2026, 9, 1, 12, tzinfo=UTC), extension_blocks=2)
    result = __import__("asyncio").run(
        HeliusIndexedHistoryClient(
            "https://example.test/private",
            transport=_Transport([_response(1, [_row(source_time=plan.protocols[0].window_start)])]),
            sleep=lambda _: _noop(),
        ).discover(plan.protocols[0])
    )

    plan_path = write_pump_corpus_v2_window_plan(tmp_path, plan=plan)
    assert recover_pump_corpus_v2_window_plan(tmp_path).plan_digest == plan.plan_digest
    manifest_path = write_pump_corpus_v2_source_manifest(tmp_path, plan=plan, discoveries=(result,))
    recovered_plan, recovered = recover_pump_corpus_v2_source_manifest(tmp_path)
    fresh = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import json,sys; from pathlib import Path; "
                "from newcoin_trader.sources.pumpfun.helius_indexed import recover_pump_corpus_v2_source_manifest; "
                "plan,manifest=recover_pump_corpus_v2_source_manifest(Path(sys.argv[1])); "
                "payload={'plan': plan.plan_digest, "
                "'coordinates': [item.model_dump(mode='json') for item in manifest.coordinates]}; "
                "print(json.dumps(payload))"
            ),
            str(tmp_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    fresh_payload = json.loads(fresh.stdout)

    assert plan_path.exists()
    assert recovered_plan.plan_digest == plan.plan_digest
    assert fresh_payload["plan"] == plan.plan_digest
    assert fresh_payload["coordinates"] == [item.model_dump(mode="json") for item in recovered.coordinates]
    assert recovered.manifest_digest
    assert recovered.coordinates[0].signature == SIG
    assert recovered.coordinates[0].source_time == plan.protocols[0].window_start
    assert "mint" not in manifest_path.read_text(encoding="utf-8").lower()
    with pytest.raises(ValueError, match="controlled construction"):
        PumpCorpusV2WindowPlan.model_validate(plan.model_dump(mode="python"))
    blocked = tmp_path / "blocked"
    blocked.mkdir()
    with pytest.raises(ValueError, match="frozen window plan"):
        write_pump_corpus_v2_source_manifest(blocked, plan=plan, discoveries=(result,))
    assert not (blocked / "source_window_plan_v2.json").exists()
    write_pump_corpus_v2_window_plan(blocked, plan=plan)
    (blocked / "source_coordinate_manifest_v2.json").write_text("occupied", encoding="utf-8")
    with pytest.raises(FileExistsError):
        write_pump_corpus_v2_source_manifest(blocked, plan=plan, discoveries=(result,))
    with pytest.raises(FileExistsError):
        write_pump_corpus_v2_source_manifest(tmp_path, plan=plan, discoveries=(result,))


async def _noop() -> None:
    return None
