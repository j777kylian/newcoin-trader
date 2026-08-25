"""Adapter causality, allowlist, proof, and fact-only tests for Phase 8C.4."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from newcoin_trader.domain.early_market_events import (
    EarlyMarketEventKind,
    EventAvailability,
    EventAvailabilityStatus,
    HistoricalEarlyMarketEventFact,
)
from newcoin_trader.sources.base_uniswap_v3.adapter import (
    AdapterRequest,
    adapt_dex_first_trade,
    build_eligible_scope,
    compute_realized_execution_price,
    select_earliest_valid_swap,
)
from newcoin_trader.sources.base_uniswap_v3.contracts import (
    CHAIN_ID,
    FACTORY_ADDRESS,
    POOL_CREATED_TOPIC,
    PROTOCOL_VERSION,
    SWAP_TOPIC,
)
from newcoin_trader.sources.base_uniswap_v3.models import (
    CanonicalPoolCreatedEvidence,
    CanonicalSwapEvidence,
    CapPolicy,
    ExactPoolHistoryScanProof,
    FactoryPoolCreatedRecord,
    FactoryUniverseScanProof,
    FinalityBoundary,
    ScanKind,
    ScanLedgerEntry,
    ScanStatus,
    SwapLogRecord,
    VerifiedBlock,
    VerifiedExactPoolSwapScanResult,
    VerifiedFactoryUniverse,
    VerifiedReceipt,
    compute_canonical_pool_created_candidates_digest,
    compute_canonical_swap_candidates_digest,
)

TOKEN0 = "0x4200000000000000000000000000000000000006"
TOKEN1 = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
POOL = "0x6c561b446416e2b78e1a75e721ae6e4e60bfa7ff"
OTHER_POOL = "0x7c561b446416e2b78e1a75e721ae6e4e60bfa7f0"
SENDER = "0x1111111111111111111111111111111111111111"
RECIPIENT = "0x2222222222222222222222222222222222222222"
BLOCK_HASH = "0x" + ("ab" * 32)
TX_HASH = "0x" + ("cd" * 32)
OTHER_TX = "0x" + ("ef" * 32)
TS = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
DEPLOYMENT_LOWER = 1
CREATION_BLOCK = 100
FINALITY_NUMBER = 200


def _topic_address(addr: str) -> str:
    bare = addr.lower().removeprefix("0x")
    return "0x" + ("0" * 24) + bare


def _int_word(value: int) -> str:
    if value < 0:
        value = (1 << 256) + value
    return f"{value:064x}"


def _int24_word(value: int) -> str:
    if value < 0:
        low = (1 << 24) + value
        full = (((1 << 232) - 1) << 24) | low
        return f"{full:064x}"
    return f"{value:064x}"


def _address_word(addr: str) -> str:
    bare = addr.lower().removeprefix("0x")
    return ("0" * 24) + bare


def _fee_topic(fee: int) -> str:
    return "0x" + f"{fee:064x}"


def _creation(**overrides: object) -> FactoryPoolCreatedRecord:
    payload: dict[str, object] = {
        "chain_id": CHAIN_ID,
        "protocol_version": PROTOCOL_VERSION,
        "factory_address": FACTORY_ADDRESS,
        "token0": TOKEN0,
        "token1": TOKEN1,
        "fee": 500,
        "tick_spacing": 10,
        "pool_address": POOL,
        "block_number": CREATION_BLOCK,
        "block_hash": BLOCK_HASH,
        "transaction_hash": TX_HASH,
        "transaction_index": 0,
        "log_index": 1,
    }
    payload.update(overrides)
    return FactoryPoolCreatedRecord.model_validate(payload)


def _swap(**overrides: object) -> SwapLogRecord:
    payload: dict[str, object] = {
        "chain_id": CHAIN_ID,
        "protocol_version": PROTOCOL_VERSION,
        "pool_address": POOL,
        "sender": SENDER,
        "recipient": RECIPIENT,
        "amount0": -(10**18),
        "amount1": 2000_000000,
        "sqrt_price_x96": 79228162514264337593543950336,
        "liquidity": 1_000_000,
        "tick": 10,
        "block_number": CREATION_BLOCK,
        "block_hash": BLOCK_HASH,
        "transaction_hash": TX_HASH,
        "transaction_index": 0,
        "log_index": 2,
    }
    payload.update(overrides)
    return SwapLogRecord.model_validate(payload)


def _encode_swap_data(swap: SwapLogRecord) -> str:
    return "0x" + "".join(
        [
            _int_word(swap.amount0),
            _int_word(swap.amount1),
            _int_word(swap.sqrt_price_x96),
            _int_word(swap.liquidity),
            _int24_word(swap.tick),
        ]
    )


def _raw_swap_for(swap: SwapLogRecord, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "address": swap.pool_address,
        "topics": [SWAP_TOPIC, _topic_address(swap.sender), _topic_address(swap.recipient)],
        "data": _encode_swap_data(swap),
        "blockNumber": hex(swap.block_number),
        "blockHash": swap.block_hash,
        "transactionHash": swap.transaction_hash,
        "transactionIndex": hex(swap.transaction_index),
        "logIndex": hex(swap.log_index),
    }
    payload.update(overrides)
    return payload


def _raw_pool_created_for(creation: FactoryPoolCreatedRecord, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "address": FACTORY_ADDRESS,
        "topics": [
            POOL_CREATED_TOPIC,
            _topic_address(creation.token0),
            _topic_address(creation.token1),
            _fee_topic(creation.fee),
        ],
        "data": "0x" + _int24_word(creation.tick_spacing) + _address_word(creation.pool_address),
        "blockNumber": hex(creation.block_number),
        "blockHash": creation.block_hash,
        "transactionHash": creation.transaction_hash,
        "transactionIndex": hex(creation.transaction_index),
        "logIndex": hex(creation.log_index),
    }
    payload.update(overrides)
    return payload


def _finality(**overrides: object) -> FinalityBoundary:
    payload: dict[str, object] = {
        "number": FINALITY_NUMBER,
        "hash": "0x" + ("11" * 32),
        "policy": "number_hash",
        "version": "8c.4.0",
        "source": "fixture",
        "verified_timestamp": TS,
    }
    payload.update(overrides)
    return FinalityBoundary.model_validate(payload)


def _ledger_entry(**overrides: object) -> ScanLedgerEntry:
    payload: dict[str, object] = {
        "scan_id": "scan-1",
        "parent_scan_id": None,
        "scan_kind": ScanKind.POOL_SWAP,
        "status": ScanStatus.COMPLETED_NONEMPTY,
        "from_block": CREATION_BLOCK,
        "to_block": FINALITY_NUMBER,
        "response_count": 1,
        "response_digest": "0xabc",
        "configured_cap": 1000,
        "cap_policy": CapPolicy.REFUSE_ON_HIT,
        "possible_truncation": False,
        "address": POOL,
        "topic0": SWAP_TOPIC,
        "provider_endpoint": "https://example.invalid",
        "provider_version": "1",
        "attempt": 0,
        "split_depth": 0,
    }
    payload.update(overrides)
    return ScanLedgerEntry.model_validate(payload)


def _factory_proof(
    *,
    finality: FinalityBoundary | None = None,
    candidates: tuple[CanonicalPoolCreatedEvidence, ...] | None = None,
    **entry_overrides: object,
) -> FactoryUniverseScanProof:
    boundary = finality or _finality()
    candidates = candidates or (_creation_evidence(),)
    digest = compute_canonical_pool_created_candidates_digest(candidates)
    entry = _ledger_entry(
        scan_id="factory-1",
        scan_kind=ScanKind.FACTORY_POOL_CREATED,
        from_block=DEPLOYMENT_LOWER,
        to_block=boundary.number,
        address=FACTORY_ADDRESS,
        topic0=POOL_CREATED_TOPIC,
        response_count=len(candidates),
        response_digest=digest,
        **entry_overrides,
    )
    return FactoryUniverseScanProof.model_validate(
        {
            "factory_address": FACTORY_ADDRESS,
            "topic0": POOL_CREATED_TOPIC,
            "deployment_lower_block": DEPLOYMENT_LOWER,
            "finality": boundary,
            "entries": (entry,),
        }
    )


def _pool_proof(
    *,
    finality: FinalityBoundary | None = None,
    candidates: tuple[CanonicalSwapEvidence, ...] | None = None,
    **entry_overrides: object,
) -> ExactPoolHistoryScanProof:
    boundary = finality or _finality()
    if candidates is not None:
        digest = compute_canonical_swap_candidates_digest(candidates)
        entry_overrides = {
            "response_count": len(candidates),
            "response_digest": digest,
            **entry_overrides,
        }
    entry = _ledger_entry(
        scan_id="pool-1",
        scan_kind=ScanKind.POOL_SWAP,
        from_block=CREATION_BLOCK,
        to_block=boundary.number,
        address=POOL,
        topic0=SWAP_TOPIC,
        **entry_overrides,
    )
    return ExactPoolHistoryScanProof.model_validate(
        {
            "pool_address": POOL,
            "topic0": SWAP_TOPIC,
            "creation_block": CREATION_BLOCK,
            "finality": boundary,
            "entries": (entry,),
        }
    )


def _creation_evidence(creation: FactoryPoolCreatedRecord | None = None) -> CanonicalPoolCreatedEvidence:
    creation = creation or _creation()
    return CanonicalPoolCreatedEvidence.model_validate(
        {
            "raw_log": _raw_pool_created_for(creation),
            "creation": creation,
            "receipt": {
                "transaction_hash": creation.transaction_hash,
                "block_hash": creation.block_hash,
                "block_number": creation.block_number,
                "transaction_index": creation.transaction_index,
                "status": 1,
            },
            "block": {
                "number": creation.block_number,
                "hash": creation.block_hash,
                "timestamp": TS,
            },
        }
    )


def _factory_universe(
    candidates: tuple[CanonicalPoolCreatedEvidence, ...] | None = None,
    *,
    finality: FinalityBoundary | None = None,
    factory_proof: FactoryUniverseScanProof | None = None,
) -> VerifiedFactoryUniverse:
    candidates = candidates or (_creation_evidence(),)
    proof = factory_proof or _factory_proof(finality=finality, candidates=candidates)
    return VerifiedFactoryUniverse.from_complete_candidates(
        factory_scan_proof=proof,
        candidates=candidates,
    )


def _swap_evidence(swap: SwapLogRecord | None = None) -> CanonicalSwapEvidence:
    swap = swap or _swap()
    return CanonicalSwapEvidence.model_validate(
        {
            "raw_log": _raw_swap_for(swap),
            "swap": swap,
            "receipt": {
                "transaction_hash": swap.transaction_hash,
                "block_hash": swap.block_hash,
                "block_number": swap.block_number,
                "transaction_index": swap.transaction_index,
                "status": 1,
            },
            "block": {
                "number": swap.block_number,
                "hash": swap.block_hash,
                "timestamp": TS,
            },
        }
    )


def _scan_result(
    candidates: tuple[CanonicalSwapEvidence, ...],
    *,
    finality: FinalityBoundary | None = None,
    pool_proof: ExactPoolHistoryScanProof | None = None,
) -> VerifiedExactPoolSwapScanResult:
    ordered = tuple(sorted(candidates, key=lambda item: item.swap.order_key))
    proof = pool_proof or _pool_proof(finality=finality, candidates=ordered)
    return VerifiedExactPoolSwapScanResult.from_complete_candidates(
        pool_address=POOL,
        pool_scan_proof=proof,
        candidates=ordered,
    )


def _request(
    *,
    creation: FactoryPoolCreatedRecord | None = None,
    swaps: tuple[SwapLogRecord, ...] | None = None,
    finality: FinalityBoundary | None = None,
    allowlist: tuple[str, ...] = (POOL,),
    factory_universe: VerifiedFactoryUniverse | None = None,
    scan_result: VerifiedExactPoolSwapScanResult | None = None,
    swap_candidates: tuple[CanonicalSwapEvidence, ...] | None = None,
) -> AdapterRequest:
    creation = creation or _creation()
    boundary = finality or _finality()
    if scan_result is None:
        if swaps is None:
            swaps = (_swap(),)
        candidates = swap_candidates or tuple(_swap_evidence(item) for item in swaps)
        scan_result = _scan_result(candidates, finality=boundary)
    return AdapterRequest.model_validate(
        {
            "creation_evidence": _creation_evidence(creation),
            "exact_pool_swap_scan_result": scan_result,
            "factory_universe": factory_universe
            or _factory_universe(
                (_creation_evidence(creation),),
                finality=boundary,
            ),
            "explicit_pool_allowlist": allowlist,
            "finality": boundary,
            "quote_allowlist": (TOKEN1,),
            "event_id": "dex-first-trade-1",
            "provenance_ref": "prov://base/uniswap_v3/1",
        }
    )


def test_later_same_block_log_accepted_prior_rejected() -> None:
    creation = _creation(log_index=1)
    later = _swap(log_index=2)
    prior = _swap(log_index=0)
    assert select_earliest_valid_swap((later,), creation=creation) == later
    with pytest.raises(ValueError, match="strictly after"):
        select_earliest_valid_swap((prior,), creation=creation)


def test_scan_result_rejects_omitted_earlier_canonical_swap() -> None:
    earlier = _swap(log_index=2)
    later = _swap(log_index=3)
    complete = (_swap_evidence(earlier), _swap_evidence(later))
    proof = _pool_proof(candidates=complete)

    with pytest.raises(ValueError, match="candidate.*count|digest|scan result"):
        adapt_dex_first_trade(
            _request(
                scan_result=_scan_result((_swap_evidence(later),), pool_proof=proof),
            )
        )


def test_scan_result_rejects_count_mismatch() -> None:
    only = _swap_evidence()
    digest = compute_canonical_swap_candidates_digest((only,))
    proof = _pool_proof(response_count=2, response_digest=digest)
    with pytest.raises(ValueError, match="candidate.*count|digest|scan result"):
        VerifiedExactPoolSwapScanResult.from_complete_candidates(
            pool_address=POOL,
            pool_scan_proof=proof,
            candidates=(only,),
        )


def test_scan_result_rejects_candidate_digest_mismatch() -> None:
    only = _swap_evidence()
    proof = _pool_proof(candidates=(only,))
    with pytest.raises(ValueError, match="candidate.*count|digest|scan result"):
        VerifiedExactPoolSwapScanResult.model_validate(
            {
                "pool_address": POOL,
                "pool_scan_proof": proof,
                "candidates": (only,),
                "candidate_count": 1,
                "candidate_digest": "0x" + ("11" * 32),
                "aggregate_scan_digest": proof.entries[0].response_digest,
            }
        )


def test_complete_two_swap_scan_selects_earlier_regardless_input_order() -> None:
    earlier_ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
    later_ts = datetime(2024, 6, 1, 13, 0, 0, tzinfo=UTC)
    earlier = _swap(log_index=2, transaction_hash=TX_HASH)
    later = _swap(log_index=3, transaction_hash=OTHER_TX)

    def _evidence_at(swap: SwapLogRecord, ts: datetime) -> CanonicalSwapEvidence:
        base = _swap_evidence(swap)
        return CanonicalSwapEvidence.model_validate(
            {
                "raw_log": base.raw_log,
                "swap": base.swap,
                "receipt": base.receipt,
                "block": {"number": swap.block_number, "hash": swap.block_hash, "timestamp": ts},
            }
        )

    later_ev = _evidence_at(later, later_ts)
    earlier_ev = _evidence_at(earlier, earlier_ts)
    # Caller supplies later first; scan result / adapter must still select earlier.
    fact = adapt_dex_first_trade(_request(scan_result=_scan_result((later_ev, earlier_ev))))
    assert fact.source_event_time == earlier_ts
    assert fact.first_trade_time == earlier_ts
    assert select_earliest_valid_swap((later, earlier), creation=_creation()).log_index == 2


def test_singleton_scan_valid_only_with_proof_count_one() -> None:
    only = _swap_evidence()
    valid = _scan_result((only,))
    assert valid.candidate_count == 1
    assert valid.pool_scan_proof.entries[0].response_count == 1
    fact = adapt_dex_first_trade(_request(scan_result=valid))
    assert fact.first_trade_time == TS

    two_digest = compute_canonical_swap_candidates_digest((only, _swap_evidence(_swap(log_index=3))))
    bad_proof = _pool_proof(response_count=2, response_digest=two_digest)
    with pytest.raises(ValueError, match="candidate.*count|digest|scan result|singleton"):
        VerifiedExactPoolSwapScanResult.from_complete_candidates(
            pool_address=POOL,
            pool_scan_proof=bad_proof,
            candidates=(only,),
        )


def test_verified_scan_model_copy_rejects_omitted_candidate() -> None:
    earlier = _swap_evidence(_swap(log_index=2))
    later = _swap_evidence(_swap(log_index=3, transaction_hash=OTHER_TX))
    valid = _scan_result((earlier, later))

    with pytest.raises((ValueError, ValidationError), match="candidate.*count|digest|scan result"):
        valid.model_copy(update={"candidates": (later,)})


def test_adapter_request_model_copy_rejects_tampered_scan() -> None:
    earlier = _swap_evidence(_swap(log_index=2))
    later = _swap_evidence(_swap(log_index=3, transaction_hash=OTHER_TX))
    valid_scan = _scan_result((earlier, later))
    # Bypass VerifiedExactPoolSwapScanResult.model_copy; AdapterRequest must still revalidate.
    tampered_scan = VerifiedExactPoolSwapScanResult.model_construct(
        pool_address=valid_scan.pool_address,
        pool_scan_proof=valid_scan.pool_scan_proof,
        candidates=(later,),
        candidate_count=valid_scan.candidate_count,
        candidate_digest=valid_scan.candidate_digest,
        aggregate_scan_digest=valid_scan.aggregate_scan_digest,
    )

    with pytest.raises((ValueError, ValidationError), match="candidate.*count|digest|scan result"):
        _request(scan_result=valid_scan).model_copy(update={"exact_pool_swap_scan_result": tampered_scan})


def test_adapter_request_model_copy_rejects_nested_proof_tamper() -> None:
    earlier = _swap_evidence(_swap(log_index=2))
    later = _swap_evidence(_swap(log_index=3, transaction_hash=OTHER_TX))
    valid_scan = _scan_result((earlier, later))
    entry = valid_scan.pool_scan_proof.entries[0]
    entry_payload = entry.model_dump(mode="python")
    entry_payload["response_count"] = 1
    tampered_entry = ScanLedgerEntry.model_construct(**entry_payload)
    tampered_proof = ExactPoolHistoryScanProof.model_construct(
        pool_address=valid_scan.pool_scan_proof.pool_address,
        topic0=valid_scan.pool_scan_proof.topic0,
        creation_block=valid_scan.pool_scan_proof.creation_block,
        finality=valid_scan.pool_scan_proof.finality,
        entries=(tampered_entry,),
    )
    tampered_scan = VerifiedExactPoolSwapScanResult.model_construct(
        pool_address=valid_scan.pool_address,
        pool_scan_proof=tampered_proof,
        candidates=valid_scan.candidates,
        candidate_count=valid_scan.candidate_count,
        candidate_digest=valid_scan.candidate_digest,
        aggregate_scan_digest=valid_scan.aggregate_scan_digest,
    )

    with pytest.raises((ValueError, ValidationError), match="candidate.*count|digest|scan result|ledger"):
        _request(scan_result=valid_scan).model_copy(update={"exact_pool_swap_scan_result": tampered_scan})


def _adapter_request_model_construct(
    *,
    base: AdapterRequest,
    creation_evidence: CanonicalPoolCreatedEvidence | None = None,
    exact_pool_swap_scan_result: VerifiedExactPoolSwapScanResult | None = None,
) -> AdapterRequest:
    return AdapterRequest.model_construct(
        creation_evidence=creation_evidence or base.creation_evidence,
        exact_pool_swap_scan_result=exact_pool_swap_scan_result or base.exact_pool_swap_scan_result,
        factory_universe=base.factory_universe,
        explicit_pool_allowlist=base.explicit_pool_allowlist,
        finality=base.finality,
        quote_allowlist=base.quote_allowlist,
        event_id=base.event_id,
        provenance_ref=base.provenance_ref,
    )


def test_adapt_rejects_model_construct_omitted_candidate() -> None:
    earlier = _swap_evidence(_swap(log_index=2))
    later = _swap_evidence(_swap(log_index=3, transaction_hash=OTHER_TX))
    valid = _request(scan_result=_scan_result((earlier, later)))
    tampered_scan = VerifiedExactPoolSwapScanResult.model_construct(
        pool_address=valid.exact_pool_swap_scan_result.pool_address,
        pool_scan_proof=valid.exact_pool_swap_scan_result.pool_scan_proof,
        candidates=(later,),
        candidate_count=valid.exact_pool_swap_scan_result.candidate_count,
        candidate_digest=valid.exact_pool_swap_scan_result.candidate_digest,
        aggregate_scan_digest=valid.exact_pool_swap_scan_result.aggregate_scan_digest,
    )
    constructed = _adapter_request_model_construct(
        base=valid,
        exact_pool_swap_scan_result=tampered_scan,
    )
    with pytest.raises((ValueError, ValidationError), match="candidate.*count|digest|scan result"):
        adapt_dex_first_trade(constructed)


def test_adapt_rejects_model_construct_digest_count_mismatch() -> None:
    only = _swap_evidence()
    valid = _request(scan_result=_scan_result((only,)))
    tampered_scan = VerifiedExactPoolSwapScanResult.model_construct(
        pool_address=valid.exact_pool_swap_scan_result.pool_address,
        pool_scan_proof=valid.exact_pool_swap_scan_result.pool_scan_proof,
        candidates=(only,),
        candidate_count=2,
        candidate_digest="0x" + ("11" * 32),
        aggregate_scan_digest=valid.exact_pool_swap_scan_result.aggregate_scan_digest,
    )
    constructed = _adapter_request_model_construct(
        base=valid,
        exact_pool_swap_scan_result=tampered_scan,
    )
    with pytest.raises((ValueError, ValidationError), match="candidate.*count|digest|scan result"):
        adapt_dex_first_trade(constructed)


def test_adapt_rejects_model_construct_nested_proof_tamper() -> None:
    earlier = _swap_evidence(_swap(log_index=2))
    later = _swap_evidence(_swap(log_index=3, transaction_hash=OTHER_TX))
    valid = _request(scan_result=_scan_result((earlier, later)))
    scan = valid.exact_pool_swap_scan_result
    entry = scan.pool_scan_proof.entries[0]
    entry_payload = entry.model_dump(mode="python")
    entry_payload["response_count"] = 1
    tampered_entry = ScanLedgerEntry.model_construct(**entry_payload)
    tampered_proof = ExactPoolHistoryScanProof.model_construct(
        pool_address=scan.pool_scan_proof.pool_address,
        topic0=scan.pool_scan_proof.topic0,
        creation_block=scan.pool_scan_proof.creation_block,
        finality=scan.pool_scan_proof.finality,
        entries=(tampered_entry,),
    )
    tampered_scan = VerifiedExactPoolSwapScanResult.model_construct(
        pool_address=scan.pool_address,
        pool_scan_proof=tampered_proof,
        candidates=scan.candidates,
        candidate_count=scan.candidate_count,
        candidate_digest=scan.candidate_digest,
        aggregate_scan_digest=scan.aggregate_scan_digest,
    )
    constructed = _adapter_request_model_construct(
        base=valid,
        exact_pool_swap_scan_result=tampered_scan,
    )
    with pytest.raises((ValueError, ValidationError), match="candidate.*count|digest|scan result|ledger"):
        adapt_dex_first_trade(constructed)


def test_adapt_rejects_model_construct_invalid_pool_created_evidence() -> None:
    valid = _request()
    creation = valid.creation_evidence.creation
    bad_evidence = CanonicalPoolCreatedEvidence.model_construct(
        raw_log={},
        creation=creation,
        receipt=valid.creation_evidence.receipt,
        block=valid.creation_evidence.block,
    )
    constructed = _adapter_request_model_construct(base=valid, creation_evidence=bad_evidence)
    with pytest.raises((ValueError, ValidationError), match="raw_log|PoolCreated|factory|binding|missing"):
        adapt_dex_first_trade(constructed)


def test_adapt_rejects_model_construct_invalid_swap_evidence() -> None:
    swap = _swap()
    valid_evidence = _swap_evidence(swap)
    bad_swap_evidence = CanonicalSwapEvidence.model_construct(
        raw_log={},
        swap=swap,
        receipt=valid_evidence.receipt,
        block=valid_evidence.block,
    )
    # Keep ledger digests aligned with the fabricated singleton candidate so the
    # failure is specifically nested CanonicalSwapEvidence revalidation.
    digest = compute_canonical_swap_candidates_digest((valid_evidence,))
    proof = _pool_proof(candidates=(valid_evidence,))
    tampered_scan = VerifiedExactPoolSwapScanResult.model_construct(
        pool_address=POOL,
        pool_scan_proof=proof,
        candidates=(bad_swap_evidence,),
        candidate_count=1,
        candidate_digest=digest,
        aggregate_scan_digest=digest,
    )
    constructed = _adapter_request_model_construct(
        base=_request(scan_result=_scan_result((valid_evidence,))),
        exact_pool_swap_scan_result=tampered_scan,
    )
    with pytest.raises((ValueError, ValidationError), match="raw_log|Swap|topic|binding|missing|malformed"):
        adapt_dex_first_trade(constructed)


def test_adapt_fact_route_uses_model_validate_source_time_only_availability() -> None:
    fact = adapt_dex_first_trade(_request())
    assert fact.availability.status is EventAvailabilityStatus.SOURCE_TIME_ONLY
    # Adapter fact path must emit availability that re-validates (not construct-only).
    revalidated = EventAvailability.model_validate(fact.availability.model_dump(mode="python"))
    assert revalidated.status is EventAvailabilityStatus.SOURCE_TIME_ONLY
    assert revalidated.received_time is None
    assert revalidated.decision_available_time is None
    # Domain HistoricalEarlyMarketEventFact.model_construct is out of scope; adapter
    # entry must still refuse constructed invalid nested proof/evidence (covered above).


def test_bare_factory_record_cannot_establish_eligible_scope() -> None:
    bare = FactoryPoolCreatedRecord.model_construct(
        factory_address=FACTORY_ADDRESS,
        pool_address=POOL,
    )
    with pytest.raises((TypeError, ValueError, ValidationError)):
        build_eligible_scope((bare,), explicit_pool_allowlist=(POOL,))


def test_constructed_factory_universe_cannot_bypass_candidate_binding() -> None:
    universe = _factory_universe()
    tampered = VerifiedFactoryUniverse.model_construct(
        factory_scan_proof=universe.factory_scan_proof,
        candidates=universe.candidates,
        candidate_count=0,
        candidate_digest=universe.candidate_digest,
        aggregate_scan_digest=universe.aggregate_scan_digest,
    )
    with pytest.raises(ValueError, match="factory universe candidate count"):
        build_eligible_scope(tampered, explicit_pool_allowlist=(POOL,))


def test_point_observation_candidate_is_not_publicly_importable() -> None:
    with pytest.raises(ImportError):
        exec("from newcoin_trader.sources.base_uniswap_v3 import PointObservationCandidate", {})


def test_verified_factory_universe_intersects_matching_allowlist() -> None:
    universe = _factory_universe()
    assert build_eligible_scope(universe, explicit_pool_allowlist=(POOL,)) == (_creation(),)


def test_unproven_allowlist_rejected() -> None:
    with pytest.raises(ValueError, match="missing from factory universe"):
        build_eligible_scope(_factory_universe(), explicit_pool_allowlist=(OTHER_POOL,))


def test_fact_source_time_only_absent_clocks() -> None:
    fact = adapt_dex_first_trade(_request())
    assert isinstance(fact, HistoricalEarlyMarketEventFact)
    assert fact.event_kind is EarlyMarketEventKind.DEX_FIRST_TRADE
    assert fact.availability.status is EventAvailabilityStatus.SOURCE_TIME_ONLY
    assert fact.availability.received_time is None
    assert fact.availability.decision_available_time is None
    assert fact.source_event_time == TS
    assert fact.first_trade_time == TS
    assert fact.first_market_data_time is None
    assert not hasattr(fact, "observation")


def test_atomic_amounts_decimals_exact_deterministic_price() -> None:
    swap = _swap(amount0=-(10**18), amount1=2_000_000000)
    price, base, quote = compute_realized_execution_price(
        swap,
        token0=TOKEN0,
        token1=TOKEN1,
        quote_allowlist=(TOKEN1,),
        decimals_by_address={TOKEN0: 18, TOKEN1: 6},
    )
    assert base == TOKEN0
    assert quote == TOKEN1
    assert price == Decimal("2000")


def test_realized_price_rejects_same_sign_positive_deltas() -> None:
    with pytest.raises(ValueError, match="opposite signs"):
        compute_realized_execution_price(
            _swap(amount0=10**18, amount1=2_000_000000),
            token0=TOKEN0,
            token1=TOKEN1,
            quote_allowlist=(TOKEN1,),
            decimals_by_address={TOKEN0: 18, TOKEN1: 6},
        )


def test_realized_price_rejects_same_sign_negative_deltas() -> None:
    with pytest.raises(ValueError, match="opposite signs"):
        compute_realized_execution_price(
            _swap(amount0=-(10**18), amount1=-2_000_000000),
            token0=TOKEN0,
            token1=TOKEN1,
            quote_allowlist=(TOKEN1,),
            decimals_by_address={TOKEN0: 18, TOKEN1: 6},
        )


def test_realized_price_rejects_zero_amount0_or_amount1() -> None:
    with pytest.raises(ValueError, match="zero amount0/amount1"):
        compute_realized_execution_price(
            _swap(amount0=0, amount1=2_000_000000),
            token0=TOKEN0,
            token1=TOKEN1,
            quote_allowlist=(TOKEN1,),
            decimals_by_address={TOKEN0: 18, TOKEN1: 6},
        )
    with pytest.raises(ValueError, match="zero amount0/amount1"):
        compute_realized_execution_price(
            _swap(amount0=-(10**18), amount1=0),
            token0=TOKEN0,
            token1=TOKEN1,
            quote_allowlist=(TOKEN1,),
            decimals_by_address={TOKEN0: 18, TOKEN1: 6},
        )


def test_sqrt_price_x96_never_used_as_execution_price() -> None:
    swap_a = _swap(amount0=-(10**18), amount1=2_000_000000, sqrt_price_x96=1)
    swap_b = _swap(amount0=-(10**18), amount1=2_000_000000, sqrt_price_x96=10**40)
    price_a, _, _ = compute_realized_execution_price(
        swap_a,
        token0=TOKEN0,
        token1=TOKEN1,
        quote_allowlist=(TOKEN1,),
        decimals_by_address={TOKEN0: 18, TOKEN1: 6},
    )
    price_b, _, _ = compute_realized_execution_price(
        swap_b,
        token0=TOKEN0,
        token1=TOKEN1,
        quote_allowlist=(TOKEN1,),
        decimals_by_address={TOKEN0: 18, TOKEN1: 6},
    )
    assert price_a == price_b == Decimal("2000")


def test_canonical_binding_rejects_receipt_failure_and_hash_mismatch() -> None:
    swap = _swap()
    with pytest.raises(ValueError, match="receipt status"):
        CanonicalSwapEvidence.model_validate(
            {
                "raw_log": _raw_swap_for(swap),
                "swap": swap,
                "receipt": VerifiedReceipt.model_validate(
                    {
                        "transaction_hash": swap.transaction_hash,
                        "block_hash": swap.block_hash,
                        "block_number": swap.block_number,
                        "transaction_index": swap.transaction_index,
                        "status": 0,
                    }
                ),
                "block": VerifiedBlock.model_validate(
                    {"number": swap.block_number, "hash": swap.block_hash, "timestamp": TS}
                ),
            }
        )
    with pytest.raises(ValueError, match="block_hash"):
        CanonicalSwapEvidence.model_validate(
            {
                "raw_log": _raw_swap_for(swap, blockHash="0x" + ("11" * 32)),
                "swap": swap,
                "receipt": {
                    "transaction_hash": swap.transaction_hash,
                    "block_hash": swap.block_hash,
                    "block_number": swap.block_number,
                    "transaction_index": swap.transaction_index,
                    "status": 1,
                },
                "block": {"number": swap.block_number, "hash": swap.block_hash, "timestamp": TS},
            }
        )


def test_finality_boundary_hash_required_when_equal_number() -> None:
    swap = _swap(block_number=FINALITY_NUMBER, block_hash=BLOCK_HASH)
    boundary = _finality(number=FINALITY_NUMBER, hash="0x" + ("11" * 32))
    with pytest.raises(ValueError, match="exact boundary hash"):
        adapt_dex_first_trade(
            _request(
                swaps=(swap,),
                finality=boundary,
            )
        )


def test_canonical_swap_evidence_rejects_non_swap_topic_with_decoded_swap() -> None:
    swap = _swap()
    raw = _raw_swap_for(swap)
    raw["topics"] = ["0x" + ("11" * 32), _topic_address(SENDER), _topic_address(RECIPIENT)]
    with pytest.raises(ValueError, match="Swap topic"):
        CanonicalSwapEvidence.model_validate(
            {
                "raw_log": raw,
                "swap": swap,
                "receipt": {
                    "transaction_hash": swap.transaction_hash,
                    "block_hash": swap.block_hash,
                    "block_number": swap.block_number,
                    "transaction_index": swap.transaction_index,
                    "status": 1,
                },
                "block": {"number": swap.block_number, "hash": swap.block_hash, "timestamp": TS},
            }
        )


def test_canonical_swap_rejects_zero_data_with_manual_valid_decoded_swap() -> None:
    swap = _swap()
    raw = _raw_swap_for(swap, data="0x" + ("00" * 160))
    with pytest.raises(ValueError, match="mismatch|malformed|Swap"):
        CanonicalSwapEvidence.model_validate(
            {
                "raw_log": raw,
                "swap": swap,
                "receipt": {
                    "transaction_hash": swap.transaction_hash,
                    "block_hash": swap.block_hash,
                    "block_number": swap.block_number,
                    "transaction_index": swap.transaction_index,
                    "status": 1,
                },
                "block": {"number": swap.block_number, "hash": swap.block_hash, "timestamp": TS},
            }
        )


def test_canonical_swap_rejects_malformed_data_word_count() -> None:
    swap = _swap()
    with pytest.raises(ValueError, match="malformed|ABI data"):
        CanonicalSwapEvidence.model_validate(
            {
                "raw_log": _raw_swap_for(swap, data="0x" + ("00" * 64)),
                "swap": swap,
                "receipt": {
                    "transaction_hash": swap.transaction_hash,
                    "block_hash": swap.block_hash,
                    "block_number": swap.block_number,
                    "transaction_index": swap.transaction_index,
                    "status": 1,
                },
                "block": {"number": swap.block_number, "hash": swap.block_hash, "timestamp": TS},
            }
        )


def test_canonical_swap_rejects_removed_log() -> None:
    swap = _swap()
    with pytest.raises(ValueError, match="removed"):
        CanonicalSwapEvidence.model_validate(
            {
                "raw_log": _raw_swap_for(swap, removed=True),
                "swap": swap,
                "receipt": {
                    "transaction_hash": swap.transaction_hash,
                    "block_hash": swap.block_hash,
                    "block_number": swap.block_number,
                    "transaction_index": swap.transaction_index,
                    "status": 1,
                },
                "block": {"number": swap.block_number, "hash": swap.block_hash, "timestamp": TS},
            }
        )


def test_earlier_valid_candidate_wins_over_later_first_ordering() -> None:
    earlier_ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
    later_ts = datetime(2024, 6, 1, 13, 0, 0, tzinfo=UTC)
    earlier = _swap(log_index=2, transaction_hash=TX_HASH)
    later = _swap(log_index=3, transaction_hash=OTHER_TX)

    def _evidence_at(swap: SwapLogRecord, ts: datetime) -> CanonicalSwapEvidence:
        base = _swap_evidence(swap)
        return CanonicalSwapEvidence.model_validate(
            {
                "raw_log": base.raw_log,
                "swap": base.swap,
                "receipt": base.receipt,
                "block": {"number": swap.block_number, "hash": swap.block_hash, "timestamp": ts},
            }
        )

    # Caller puts later first; adapter must still select earlier.
    fact = adapt_dex_first_trade(
        _request(swap_candidates=(_evidence_at(later, later_ts), _evidence_at(earlier, earlier_ts)))
    )
    assert fact.source_event_time == earlier_ts
    assert fact.first_trade_time == earlier_ts
    assert select_earliest_valid_swap((later, earlier), creation=_creation()).log_index == 2


def test_same_block_strict_prior_in_candidates_rejected() -> None:
    creation = _creation(log_index=1)
    prior = _swap(log_index=0)
    later = _swap(log_index=2)
    with pytest.raises(ValueError, match="strictly after"):
        adapt_dex_first_trade(_request(creation=creation, swaps=(prior, later)))


def test_allowlist_cannot_bypass_missing_creation_evidence_pool() -> None:
    with pytest.raises(ValueError, match="allowlist"):
        adapt_dex_first_trade(_request(allowlist=(OTHER_POOL,)))


def test_fabricated_pool_created_record_alone_rejected() -> None:
    creation = _creation()
    with pytest.raises(ValueError, match="raw_log missing|PoolCreated|factory"):
        CanonicalPoolCreatedEvidence.model_validate(
            {
                "raw_log": {},
                "creation": creation,
                "receipt": {
                    "transaction_hash": creation.transaction_hash,
                    "block_hash": creation.block_hash,
                    "block_number": creation.block_number,
                    "transaction_index": creation.transaction_index,
                    "status": 1,
                },
                "block": {"number": creation.block_number, "hash": creation.block_hash, "timestamp": TS},
            }
        )


def test_canonical_pool_created_rejects_wrong_factory_raw() -> None:
    creation = _creation()
    with pytest.raises(ValueError, match="factory"):
        CanonicalPoolCreatedEvidence.model_validate(
            {
                "raw_log": _raw_pool_created_for(creation, address=OTHER_POOL),
                "creation": creation,
                "receipt": {
                    "transaction_hash": creation.transaction_hash,
                    "block_hash": creation.block_hash,
                    "block_number": creation.block_number,
                    "transaction_index": creation.transaction_index,
                    "status": 1,
                },
                "block": {"number": creation.block_number, "hash": creation.block_hash, "timestamp": TS},
            }
        )


def test_canonical_pool_created_rejects_raw_abi_mismatch() -> None:
    creation = _creation()
    raw = _raw_pool_created_for(creation, data="0x" + _int24_word(99) + _address_word(POOL))
    with pytest.raises(ValueError, match="mismatch|PoolCreated"):
        CanonicalPoolCreatedEvidence.model_validate(
            {
                "raw_log": raw,
                "creation": creation,
                "receipt": {
                    "transaction_hash": creation.transaction_hash,
                    "block_hash": creation.block_hash,
                    "block_number": creation.block_number,
                    "transaction_index": creation.transaction_index,
                    "status": 1,
                },
                "block": {"number": creation.block_number, "hash": creation.block_hash, "timestamp": TS},
            }
        )


def test_canonical_pool_created_rejects_receipt_block_mismatch() -> None:
    creation = _creation()
    with pytest.raises(ValueError, match="binding|receipt"):
        CanonicalPoolCreatedEvidence.model_validate(
            {
                "raw_log": _raw_pool_created_for(creation),
                "creation": creation,
                "receipt": {
                    "transaction_hash": creation.transaction_hash,
                    "block_hash": "0x" + ("11" * 32),
                    "block_number": creation.block_number,
                    "transaction_index": creation.transaction_index,
                    "status": 1,
                },
                "block": {"number": creation.block_number, "hash": creation.block_hash, "timestamp": TS},
            }
        )


def test_creation_not_covered_by_factory_proof_rejected() -> None:
    boundary = _finality()
    # Complete coverage only after creation block — creation itself uncovered.
    entry = _ledger_entry(
        scan_id="factory-gap",
        scan_kind=ScanKind.FACTORY_POOL_CREATED,
        from_block=CREATION_BLOCK + 1,
        to_block=boundary.number,
        address=FACTORY_ADDRESS,
        topic0=POOL_CREATED_TOPIC,
    )
    with pytest.raises(ValueError, match="coverage|gap|incomplete"):
        FactoryUniverseScanProof.model_validate(
            {
                "factory_address": FACTORY_ADDRESS,
                "topic0": POOL_CREATED_TOPIC,
                "deployment_lower_block": DEPLOYMENT_LOWER,
                "finality": boundary,
                "entries": (entry,),
            }
        )


def test_adapter_rejects_when_creation_block_not_in_factory_proof_range() -> None:
    boundary = _finality()
    # Proof validates for a shifted lower bound that still covers [lower, upper],
    # but assert_covers_block for creation fails when entries skip creation.
    early = _ledger_entry(
        scan_id="factory-early",
        scan_kind=ScanKind.FACTORY_POOL_CREATED,
        from_block=DEPLOYMENT_LOWER,
        to_block=CREATION_BLOCK - 1,
        address=FACTORY_ADDRESS,
        topic0=POOL_CREATED_TOPIC,
        response_count=0,
        status=ScanStatus.COMPLETED_EMPTY,
    )
    late = _ledger_entry(
        scan_id="factory-late",
        scan_kind=ScanKind.FACTORY_POOL_CREATED,
        from_block=CREATION_BLOCK + 1,
        to_block=boundary.number,
        address=FACTORY_ADDRESS,
        topic0=POOL_CREATED_TOPIC,
    )
    with pytest.raises(ValueError, match="coverage|gap|incomplete"):
        FactoryUniverseScanProof.model_validate(
            {
                "factory_address": FACTORY_ADDRESS,
                "topic0": POOL_CREATED_TOPIC,
                "deployment_lower_block": DEPLOYMENT_LOWER,
                "finality": boundary,
                "entries": (early, late),
            }
        )


def test_adapter_rejects_mismatched_proof_finality_objects() -> None:
    boundary = _finality()
    other = _finality(source="other-source")
    with pytest.raises(ValueError, match="finality"):
        adapt_dex_first_trade(
            _request(
                finality=boundary,
                scan_result=_scan_result((_swap_evidence(),), finality=other),
            )
        )
