"""Offline evidence-acquisition tests for Phase 8C.6."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

import newcoin_trader.sources.base_uniswap_v3.acquisition as acquisition
from newcoin_trader.sources.base_uniswap_v3.acquisition import (
    AcquisitionError,
    AcquisitionFailure,
    BaseEvidenceAcquirer,
)
from newcoin_trader.sources.base_uniswap_v3.contracts import CHAIN_ID, FACTORY_ADDRESS, POOL_CREATED_TOPIC
from newcoin_trader.sources.base_uniswap_v3.models import (
    CapPolicy,
    FactoryDeploymentAnchor,
    ScanKind,
    ScanLedgerEntry,
    ScanStatus,
    VerifiedBlock,
    VerifiedReceipt,
    validate_ordered_scan_ledger_completeness,
)
from newcoin_trader.sources.base_uniswap_v3.provider import BaseRpcProvider, RpcCapabilityProfile

FACTORY_TX = "0x" + "aa" * 32
BLOCK_HASH = "0x" + "bb" * 32
TOKEN = "0x4200000000000000000000000000000000000006"


class FakeTransport:
    def __init__(self, results: list[object]) -> None:
        self.results = results
        self.payloads: list[dict[str, object]] = []

    async def post_json(self, _endpoint: str, payload: dict[str, object], *, timeout_seconds: float) -> object:
        assert timeout_seconds <= 15
        self.payloads.append(payload)
        result = self.results.pop(0)
        if isinstance(result, dict) and "error" in result:
            return {"jsonrpc": "2.0", "id": payload["id"], **result}
        return {"jsonrpc": "2.0", "id": payload["id"], "result": result}


def _anchor() -> FactoryDeploymentAnchor:
    receipt = VerifiedReceipt.model_validate(
        {
            "transaction_hash": FACTORY_TX,
            "block_hash": BLOCK_HASH,
            "block_number": 100,
            "transaction_index": 0,
            "status": 1,
            "contract_address": FACTORY_ADDRESS,
        }
    )
    block = VerifiedBlock.model_validate(
        {"number": 100, "hash": BLOCK_HASH, "timestamp": datetime.fromtimestamp(1, UTC)}
    )
    return FactoryDeploymentAnchor.model_validate(
        {
            "factory_address": FACTORY_ADDRESS,
            "deployment_transaction_hash": FACTORY_TX,
            "deployment_block_number": 100,
            "deployment_block_hash": BLOCK_HASH,
            "anchor_version": "base_factory_deployment_receipt_v1",
            "receipt": receipt,
            "block": block,
        }
    )


def test_acquirer_binds_chain_finality_and_verifies_the_exact_anchor() -> None:
    transport = FakeTransport(
        [
            hex(CHAIN_ID),
            hex(CHAIN_ID),
            {"number": "0x64", "hash": BLOCK_HASH, "timestamp": "0x1"},
            {"number": "0x64", "hash": BLOCK_HASH, "timestamp": "0x1"},
            hex(CHAIN_ID),
            {
                "transactionHash": FACTORY_TX,
                "blockHash": BLOCK_HASH,
                "blockNumber": "0x64",
                "transactionIndex": "0x0",
                "status": "0x1",
                "contractAddress": FACTORY_ADDRESS,
            },
            {"number": "0x64", "hash": BLOCK_HASH, "timestamp": "0x1"},
        ]
    )
    acquirer = BaseEvidenceAcquirer(
        BaseRpcProvider("https://rpc.example.invalid", transport=transport),
        now=lambda: datetime(2024, 1, 1, tzinfo=UTC),
    )

    async def run() -> None:
        assert await acquirer.verify_chain() == CHAIN_ID
        finality = await acquirer.read_finality_boundary()
        assert finality.policy == "rpc_finalized_tag_number_hash_v1"
        assert await acquirer.verify_factory_deployment(_anchor()) == _anchor()

    asyncio.run(run())


def test_acquirer_refuses_finality_without_the_profile_capability() -> None:
    profile = RpcCapabilityProfile(supports_finalized_tag=False)
    transport = FakeTransport([hex(CHAIN_ID)])
    acquirer = BaseEvidenceAcquirer(
        BaseRpcProvider("https://rpc.example.invalid", transport=transport, profile=profile)
    )
    with pytest.raises(AcquisitionError) as raised:
        asyncio.run(acquirer.read_finality_boundary())
    assert raised.value.result.failure is AcquisitionFailure.UNSUPPORTED_PROVIDER_CAPABILITY
    assert [payload["method"] for payload in transport.payloads] == ["eth_chainId"]


def test_acquisition_failure_labels_are_complete_and_null_receipt_is_deterministic() -> None:
    required = {
        "TRANSPORT_FAILURE",
        "RETRY_EXHAUSTED",
        "RPC_ERROR",
        "MALFORMED_RESPONSE",
        "UNSUPPORTED_PROVIDER_CAPABILITY",
        "FINALITY_MISMATCH",
        "INCOMPLETE_SCAN",
        "CAP_AMBIGUITY",
        "REQUEST_BUDGET_EXHAUSTED",
        "SPLIT_DEPTH_EXHAUSTED",
        "RECEIPT_UNAVAILABLE",
        "RECEIPT_MISMATCH",
        "BLOCK_UNAVAILABLE",
        "BLOCK_MISMATCH",
        "DECIMALS_UNAVAILABLE",
        "DECIMALS_MALFORMED",
    }
    assert {item.value for item in AcquisitionFailure} == required
    acquirer = BaseEvidenceAcquirer(BaseRpcProvider("https://rpc.example.invalid", transport=FakeTransport([None])))
    with pytest.raises(AcquisitionError) as raised:
        asyncio.run(acquirer._receipt(FACTORY_TX))
    assert raised.value.result.failure is AcquisitionFailure.RECEIPT_UNAVAILABLE
    assert "rpc.example" not in str(raised.value)


def test_active_scan_budget_counts_provider_retry_attempts() -> None:
    transport = FakeTransport(
        [
            {"error": {"code": -32016, "message": "secret"}},
            [],
        ]
    )
    profile = RpcCapabilityProfile(max_attempts=2, log_result_cap=2)
    acquirer = BaseEvidenceAcquirer(
        BaseRpcProvider("https://rpc.example.invalid", transport=transport, profile=profile)
    )
    from newcoin_trader.sources.base_uniswap_v3.scan_ledger import InMemoryScanLedger

    async def evidence(item: dict[str, Any]) -> dict[str, Any]:
        return item

    with pytest.raises(AcquisitionError) as raised:
        asyncio.run(
            acquirer._scan(
                ledger=InMemoryScanLedger(),
                kind=ScanKind.FACTORY_POOL_CREATED,
                address=FACTORY_ADDRESS,
                topic0=POOL_CREATED_TOPIC,
                lower=1,
                upper=1,
                budget=1,
                ceiling=2,
                evidence=evidence,
                digest=str,
            )
        )
    assert raised.value.result.failure is AcquisitionFailure.REQUEST_BUDGET_EXHAUSTED
    assert len(transport.payloads) == 1


def test_acquirer_rejects_noncanonical_finality_reread() -> None:
    transport = FakeTransport(
        [
            hex(CHAIN_ID),
            {"number": "0x64", "hash": BLOCK_HASH, "timestamp": "0x1"},
            {"number": "0x64", "hash": "0x" + "cc" * 32, "timestamp": "0x1"},
        ]
    )
    acquirer = BaseEvidenceAcquirer(BaseRpcProvider("https://rpc.example.invalid", transport=transport))
    with pytest.raises(AcquisitionError) as raised:
        asyncio.run(acquirer.read_finality_boundary())
    assert raised.value.result.failure is AcquisitionFailure.BLOCK_MISMATCH


def test_scan_leaves_only_completed_entries_and_rejects_duplicate_raw_identity_before_evidence() -> None:
    raw = {
        "address": FACTORY_ADDRESS,
        "topics": [POOL_CREATED_TOPIC],
        "blockNumber": "0x1",
        "blockHash": BLOCK_HASH,
        "transactionHash": FACTORY_TX,
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "data": "0x",
    }
    transport = FakeTransport([[raw], [{**raw, "blockNumber": "0x2"}]])
    profile = RpcCapabilityProfile(max_block_span=1, log_result_cap=3)
    acquirer = BaseEvidenceAcquirer(
        BaseRpcProvider("https://rpc.example.invalid", transport=transport, profile=profile)
    )
    from newcoin_trader.sources.base_uniswap_v3.scan_ledger import InMemoryScanLedger

    ledger = InMemoryScanLedger()
    evidence_calls: list[dict[str, Any]] = []

    async def evidence(item: dict[str, Any]) -> dict[str, Any]:
        evidence_calls.append(item)
        return item

    with pytest.raises(ValueError, match="duplicate"):
        asyncio.run(
            acquirer._scan(
                ledger=ledger,
                kind=ScanKind.FACTORY_POOL_CREATED,
                address=FACTORY_ADDRESS,
                topic0=POOL_CREATED_TOPIC,
                lower=1,
                upper=2,
                budget=10,
                ceiling=10,
                evidence=evidence,
                digest=lambda values: str(len(values)),
            )
        )
    assert len(evidence_calls) == 1
    assert all(entry.status is not ScanStatus.INCOMPLETE for entry in ledger.entries)


def test_scan_cap_split_records_only_incomplete_parent_and_completed_leaves() -> None:
    transport = FakeTransport([[], [], []])
    profile = RpcCapabilityProfile(max_block_span=10, log_result_cap=1)
    acquirer = BaseEvidenceAcquirer(
        BaseRpcProvider("https://rpc.example.invalid", transport=transport, profile=profile)
    )
    from newcoin_trader.sources.base_uniswap_v3.scan_ledger import InMemoryScanLedger

    ledger = InMemoryScanLedger()

    async def evidence(item: dict[str, Any]) -> dict[str, Any]:
        return item

    # A cap-hitting parent must split before decoding/collecting its candidates.
    transport.results[0] = [
        {
            "address": FACTORY_ADDRESS,
            "topics": [POOL_CREATED_TOPIC],
            "blockNumber": "0x1",
            "blockHash": BLOCK_HASH,
            "transactionHash": FACTORY_TX,
            "logIndex": "0x0",
        }
    ]
    asyncio.run(
        acquirer._scan(
            ledger=ledger,
            kind=ScanKind.FACTORY_POOL_CREATED,
            address=FACTORY_ADDRESS,
            topic0=POOL_CREATED_TOPIC,
            lower=1,
            upper=2,
            budget=10,
            ceiling=10,
            evidence=evidence,
            digest=lambda values: str(len(values)),
        )
    )
    incomplete = [entry for entry in ledger.entries if entry.status is ScanStatus.INCOMPLETE]
    completed = [
        entry for entry in ledger.entries if entry.status in {ScanStatus.COMPLETED_EMPTY, ScanStatus.COMPLETED_NONEMPTY}
    ]
    assert len(incomplete) == 1
    assert len(completed) == 2
    assert all(entry.parent_scan_id == incomplete[0].scan_id for entry in completed)


def test_public_anchor_acquisition_chain_gates_before_any_receipt_or_header_rpc() -> None:
    transport = FakeTransport(
        [
            hex(CHAIN_ID),
            {
                "transactionHash": FACTORY_TX,
                "blockHash": BLOCK_HASH,
                "blockNumber": "0x64",
                "transactionIndex": "0x0",
                "status": "0x1",
                "contractAddress": FACTORY_ADDRESS,
            },
            {"number": "0x64", "hash": BLOCK_HASH, "timestamp": "0x1"},
        ]
    )
    acquirer = BaseEvidenceAcquirer(BaseRpcProvider("https://rpc.example.invalid", transport=transport))
    assert asyncio.run(acquirer.verify_factory_deployment(_anchor())) == _anchor()
    assert [payload["method"] for payload in transport.payloads] == [
        "eth_chainId",
        "eth_getTransactionReceipt",
        "eth_getBlockByNumber",
    ]


def test_receipt_capability_blocks_transport_and_decimals_require_exact_32_byte_abi_word() -> None:
    profile = RpcCapabilityProfile(receipt_history_available=False)
    transport = FakeTransport([])
    provider = BaseRpcProvider("https://rpc.example.invalid", transport=transport, profile=profile)
    acquirer = BaseEvidenceAcquirer(provider)
    with pytest.raises(AcquisitionError) as raised:
        asyncio.run(acquirer._receipt(FACTORY_TX))
    assert raised.value.result.failure is AcquisitionFailure.UNSUPPORTED_PROVIDER_CAPABILITY
    assert transport.payloads == []


def test_transient_log_rpc_code_exhausts_retries_without_range_splitting() -> None:
    transport = FakeTransport([{"error": {"code": -32016}}] * 3)
    acquirer = BaseEvidenceAcquirer(BaseRpcProvider("https://rpc.example.invalid", transport=transport))
    from newcoin_trader.sources.base_uniswap_v3.scan_ledger import InMemoryScanLedger

    async def evidence(item: dict[str, Any]) -> dict[str, Any]:
        return item

    with pytest.raises(AcquisitionError) as raised:
        asyncio.run(
            acquirer._scan(
                ledger=InMemoryScanLedger(),
                kind=ScanKind.FACTORY_POOL_CREATED,
                address=FACTORY_ADDRESS,
                topic0=POOL_CREATED_TOPIC,
                lower=1,
                upper=2,
                budget=10,
                ceiling=10,
                evidence=evidence,
                digest=str,
            )
        )
    assert raised.value.result.failure is AcquisitionFailure.RETRY_EXHAUSTED
    assert len(transport.payloads) == 3


def _proof_entry(scan_id: str, status: ScanStatus, start: int, end: int, parent: str | None = None) -> ScanLedgerEntry:
    return ScanLedgerEntry.model_validate(
        {
            "scan_id": scan_id,
            "parent_scan_id": parent,
            "scan_kind": ScanKind.FACTORY_POOL_CREATED,
            "status": status,
            "from_block": start,
            "to_block": end,
            "response_count": 0,
            "response_digest": "",
            "configured_cap": 10,
            "cap_policy": CapPolicy.REFUSE_ON_HIT,
            "possible_truncation": False,
            "address": FACTORY_ADDRESS,
            "topic0": POOL_CREATED_TOPIC,
            "provider_endpoint": "https://rpc.example.invalid",
            "provider_version": "test",
        }
    )


def test_scan_completeness_accepts_nested_incomplete_split_with_terminal_siblings() -> None:
    validate_ordered_scan_ledger_completeness(
        (
            _proof_entry("root", ScanStatus.INCOMPLETE, 1, 4),
            _proof_entry("left", ScanStatus.INCOMPLETE, 1, 2, "root"),
            _proof_entry("right", ScanStatus.COMPLETED_EMPTY, 3, 4, "root"),
            _proof_entry("one", ScanStatus.COMPLETED_EMPTY, 1, 1, "left"),
            _proof_entry("two", ScanStatus.COMPLETED_EMPTY, 2, 2, "left"),
        ),
        scan_kind=ScanKind.FACTORY_POOL_CREATED,
        address=FACTORY_ADDRESS,
        topic0=POOL_CREATED_TOPIC,
        lower_block=1,
        upper_block=4,
    )


def test_cap_split_does_not_treat_parent_response_as_completed_candidate_identity() -> None:
    raw = {
        "address": FACTORY_ADDRESS,
        "topics": [POOL_CREATED_TOPIC],
        "blockNumber": "0x1",
        "blockHash": BLOCK_HASH,
        "transactionHash": FACTORY_TX,
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "data": "0x",
    }
    second_raw = {**raw, "blockNumber": "0x2", "transactionHash": "0x" + "cc" * 32}
    transport = FakeTransport([[raw, second_raw], [raw], [second_raw]])
    acquirer = BaseEvidenceAcquirer(
        BaseRpcProvider(
            "https://rpc.example.invalid", transport=transport, profile=RpcCapabilityProfile(log_result_cap=2)
        )
    )
    from newcoin_trader.sources.base_uniswap_v3.scan_ledger import InMemoryScanLedger

    evidence_calls: list[dict[str, Any]] = []

    async def evidence(item: dict[str, Any]) -> dict[str, Any]:
        evidence_calls.append(item)
        return item

    asyncio.run(
        acquirer._scan(
            ledger=InMemoryScanLedger(),
            kind=ScanKind.FACTORY_POOL_CREATED,
            address=FACTORY_ADDRESS,
            topic0=POOL_CREATED_TOPIC,
            lower=1,
            upper=2,
            budget=10,
            ceiling=10,
            evidence=evidence,
            digest=lambda values: str(len(values)),
        )
    )
    assert evidence_calls == [raw, second_raw]


def test_read_finality_boundary_refreshes_finalized_and_numeric_reads_every_time() -> None:
    block = {"number": "0x64", "hash": BLOCK_HASH, "timestamp": "0x1"}
    transport = FakeTransport([block, block, block, block])
    acquirer = BaseEvidenceAcquirer(BaseRpcProvider("https://rpc.example.invalid", transport=transport))

    async def run() -> None:
        await acquirer._read_finality_boundary()
        await acquirer._read_finality_boundary()

    asyncio.run(run())
    assert [payload["params"][0] for payload in transport.payloads] == ["finalized", "0x64", "finalized", "0x64"]


def test_decimals_null_result_is_unavailable_not_malformed(monkeypatch: pytest.MonkeyPatch) -> None:
    block = VerifiedBlock.model_validate(
        {"number": 100, "hash": BLOCK_HASH, "timestamp": datetime.fromtimestamp(1, UTC)}
    )
    monkeypatch.setattr(
        acquisition,
        "strict_reconstruct_model",
        lambda _model, _value: SimpleNamespace(block=block, creation=SimpleNamespace(token0=TOKEN, token1=TOKEN)),
    )
    acquirer = BaseEvidenceAcquirer(BaseRpcProvider("https://rpc.example.invalid", transport=FakeTransport([None])))
    with pytest.raises(AcquisitionError) as raised:
        asyncio.run(acquirer._acquire_token_decimals(object()))
    assert raised.value.result.failure is AcquisitionFailure.DECIMALS_UNAVAILABLE
