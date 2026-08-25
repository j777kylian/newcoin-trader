"""Scan ledger truncation / completion / proof tests for Phase 8C.4."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from newcoin_trader.sources.base_uniswap_v3.contracts import (
    FACTORY_ADDRESS,
    POOL_CREATED_TOPIC,
    SWAP_TOPIC,
)
from newcoin_trader.sources.base_uniswap_v3.models import (
    CapAmbiguityError,
    CapPolicy,
    ExactPoolHistoryScanProof,
    FactoryUniverseScanProof,
    FinalityBoundary,
    ScanKind,
    ScanLedgerEntry,
    ScanStatus,
)
from newcoin_trader.sources.base_uniswap_v3.scan_ledger import InMemoryScanLedger

TS = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
POOL = "0x6c561b446416e2b78e1a75e721ae6e4e60bfa7ff"


def _finality(number: int = 200) -> FinalityBoundary:
    return FinalityBoundary.model_validate(
        {
            "number": number,
            "hash": "0x" + ("11" * 32),
            "policy": "number_hash",
            "version": "8c.4.0",
            "source": "fixture",
            "verified_timestamp": TS,
        }
    )


def _entry(**overrides: object) -> ScanLedgerEntry:
    payload: dict[str, object] = {
        "scan_id": "scan-1",
        "parent_scan_id": None,
        "scan_kind": ScanKind.POOL_SWAP,
        "status": ScanStatus.COMPLETED_NONEMPTY,
        "from_block": 100,
        "to_block": 200,
        "response_count": 1,
        "response_digest": "0xdigest",
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


def test_detect_possible_truncation_at_cap() -> None:
    assert InMemoryScanLedger.detect_possible_truncation(response_count=100, configured_cap=100) is True
    assert InMemoryScanLedger.detect_possible_truncation(response_count=99, configured_cap=100) is False


def test_detect_possible_truncation_rejects_invalid_cap() -> None:
    with pytest.raises(ValueError, match="configured_cap"):
        InMemoryScanLedger.detect_possible_truncation(response_count=1, configured_cap=0)
    with pytest.raises(ValueError, match="configured_cap"):
        InMemoryScanLedger.detect_possible_truncation(response_count=1, configured_cap=True)  # type: ignore[arg-type]


def test_mark_completed_refuses_cap_hitting_success() -> None:
    ledger = InMemoryScanLedger()
    with pytest.raises(CapAmbiguityError) as excinfo:
        ledger.mark_completed(
            scan_id="scan-1",
            scan_kind=ScanKind.POOL_SWAP,
            from_block=1,
            to_block=10,
            response_count=50,
            configured_cap=50,
        )
    assert excinfo.value.entry.status is ScanStatus.FAILED_CAP_AMBIGUITY
    assert excinfo.value.entry.possible_truncation is True
    assert len(ledger.entries) == 1
    assert ledger.entries[0].status is ScanStatus.FAILED_CAP_AMBIGUITY


def test_mark_completed_empty_and_nonempty() -> None:
    ledger = InMemoryScanLedger()
    empty = ledger.mark_completed(
        scan_id="scan-empty",
        scan_kind=ScanKind.FACTORY_POOL_CREATED,
        from_block=1,
        to_block=2,
        response_count=0,
        configured_cap=100,
    )
    nonempty = ledger.mark_completed(
        scan_id="scan-nonempty",
        scan_kind=ScanKind.POOL_SWAP,
        from_block=1,
        to_block=2,
        response_count=3,
        configured_cap=100,
    )
    assert empty.status is ScanStatus.COMPLETED_EMPTY
    assert nonempty.status is ScanStatus.COMPLETED_NONEMPTY
    assert ledger.entries == (empty, nonempty)


def test_mark_failed_and_never_mutates_existing() -> None:
    ledger = InMemoryScanLedger()
    first = ledger.append_incomplete(
        scan_id="scan-x",
        scan_kind=ScanKind.POOL_SWAP,
        from_block=1,
        to_block=2,
        configured_cap=10,
    )
    failed = ledger.mark_failed(
        scan_id="scan-x",
        scan_kind=ScanKind.POOL_SWAP,
        status=ScanStatus.FAILED_PROVIDER,
        from_block=1,
        to_block=2,
        response_count=0,
        configured_cap=10,
        note="provider error",
    )
    assert first.status is ScanStatus.INCOMPLETE
    assert failed.status is ScanStatus.FAILED_PROVIDER
    assert ledger.entries[0] is first
    assert ledger.entries[1] is failed
    with pytest.raises(ValueError, match="mark_failed"):
        ledger.mark_failed(
            scan_id="scan-x",
            scan_kind=ScanKind.POOL_SWAP,
            status=ScanStatus.COMPLETED_EMPTY,
            from_block=1,
            to_block=2,
            response_count=0,
            configured_cap=10,
        )


def test_incomplete_factory_proof_rejected() -> None:
    with pytest.raises(ValueError, match="unresolved split|incomplete"):
        FactoryUniverseScanProof.model_validate(
            {
                "factory_address": FACTORY_ADDRESS,
                "topic0": POOL_CREATED_TOPIC,
                "deployment_lower_block": 1,
                "finality": _finality(),
                "entries": (
                    _entry(
                        scan_id="factory-incomplete",
                        scan_kind=ScanKind.FACTORY_POOL_CREATED,
                        status=ScanStatus.INCOMPLETE,
                        from_block=1,
                        to_block=200,
                        address=FACTORY_ADDRESS,
                        topic0=POOL_CREATED_TOPIC,
                        response_count=0,
                    ),
                ),
            }
        )


def test_incomplete_pool_proof_rejected() -> None:
    with pytest.raises(ValueError, match="unresolved split|incomplete"):
        ExactPoolHistoryScanProof.model_validate(
            {
                "pool_address": POOL,
                "topic0": SWAP_TOPIC,
                "creation_block": 100,
                "finality": _finality(),
                "entries": (
                    _entry(
                        status=ScanStatus.INCOMPLETE,
                        response_count=0,
                    ),
                ),
            }
        )


def test_gap_in_pool_coverage_rejected() -> None:
    with pytest.raises(ValueError, match="coverage|gap|incomplete"):
        ExactPoolHistoryScanProof.model_validate(
            {
                "pool_address": POOL,
                "topic0": SWAP_TOPIC,
                "creation_block": 100,
                "finality": _finality(),
                "entries": (
                    _entry(scan_id="a", from_block=100, to_block=150),
                    _entry(scan_id="b", from_block=152, to_block=200),
                ),
            }
        )


def test_unresolved_split_rejected() -> None:
    parent = _entry(
        scan_id="parent",
        status=ScanStatus.INCOMPLETE,
        from_block=100,
        to_block=200,
        response_count=0,
        split_depth=0,
    )
    child = _entry(
        scan_id="child",
        parent_scan_id="parent",
        status=ScanStatus.INCOMPLETE,
        from_block=100,
        to_block=150,
        response_count=0,
        split_depth=1,
    )
    with pytest.raises(ValueError, match="unresolved split"):
        ExactPoolHistoryScanProof.model_validate(
            {
                "pool_address": POOL,
                "topic0": SWAP_TOPIC,
                "creation_block": 100,
                "finality": _finality(),
                "entries": (parent, child),
            }
        )


def test_cap_ambiguity_in_proof_rejected() -> None:
    with pytest.raises(ValueError, match="cap ambiguity"):
        ExactPoolHistoryScanProof.model_validate(
            {
                "pool_address": POOL,
                "topic0": SWAP_TOPIC,
                "creation_block": 100,
                "finality": _finality(),
                "entries": (
                    _entry(
                        status=ScanStatus.FAILED_CAP_AMBIGUITY,
                        possible_truncation=True,
                        response_count=1000,
                        configured_cap=1000,
                    ),
                ),
            }
        )


def test_resolved_split_with_complete_children_accepted() -> None:
    parent = _entry(
        scan_id="parent",
        status=ScanStatus.INCOMPLETE,
        from_block=100,
        to_block=200,
        response_count=0,
        split_depth=0,
        note="split",
    )
    left = _entry(
        scan_id="child-l",
        parent_scan_id="parent",
        status=ScanStatus.COMPLETED_NONEMPTY,
        from_block=100,
        to_block=150,
        split_depth=1,
    )
    right = _entry(
        scan_id="child-r",
        parent_scan_id="parent",
        status=ScanStatus.COMPLETED_EMPTY,
        from_block=151,
        to_block=200,
        response_count=0,
        split_depth=1,
    )
    proof = ExactPoolHistoryScanProof.model_validate(
        {
            "pool_address": POOL,
            "topic0": SWAP_TOPIC,
            "creation_block": 100,
            "finality": _finality(),
            "entries": (parent, left, right),
        }
    )
    proof.assert_covers_block(125)
    proof.assert_covers_block(200)
