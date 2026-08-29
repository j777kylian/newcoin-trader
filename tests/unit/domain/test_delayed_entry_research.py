"""Phase 8D.1 delayed-entry research-contract tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from newcoin_trader.domain.delayed_entry_research import (
    CandidateDisposition,
    CandidateDispositionKind,
    CandidateUniverseV1,
    CohortSnapshotV1,
    DelayedEntryProtocolV1,
    DelayedOutcome,
    FieldPITClass,
    ImmediateEntryProtocolV1,
    compute_candidate_universe_digest,
    validate_candidate_cohort,
)
from newcoin_trader.domain.event_study import CellOutcomeStatus
from newcoin_trader.research.event_study_config import DEFAULT_ENTRY_DELAYS, DEFAULT_HOLDING_PERIODS


def _universe(**updates: object) -> CandidateUniverseV1:
    candidate_ids = ("solana:mint-a:pool-1", "solana:mint-b:pool-2")
    payload: dict[str, object] = {
        "universe_id": "solana:raydium:window-1",
        "source_qualification_id": "solana-rpc-qualified-v1",
        "venue": "raydium",
        "chain": "solana",
        "candidate_ids": candidate_ids,
        "candidate_count": len(candidate_ids),
        "candidate_digest": compute_candidate_universe_digest(candidate_ids),
    }
    payload.update(updates)
    return CandidateUniverseV1.model_validate(payload)


def test_immediate_protocol_references_the_exact_frozen_phase3_grids() -> None:
    assert ImmediateEntryProtocolV1.entry_delays is DEFAULT_ENTRY_DELAYS
    assert ImmediateEntryProtocolV1.holding_periods is DEFAULT_HOLDING_PERIODS
    assert ImmediateEntryProtocolV1.version == "ImmediateEntryProtocolV1"


def test_delayed_protocol_has_exact_additive_grid() -> None:
    protocol = DelayedEntryProtocolV1()
    assert protocol.version == "DelayedEntryProtocolV1"
    assert protocol.entry_delays == (
        timedelta(hours=1),
        timedelta(hours=2),
        timedelta(hours=3),
        timedelta(hours=4),
        timedelta(hours=6),
        timedelta(hours=8),
        timedelta(hours=12),
    )
    assert protocol.holding_periods == (
        timedelta(minutes=30),
        timedelta(hours=1),
        timedelta(hours=2),
        timedelta(hours=4),
        timedelta(hours=6),
        timedelta(hours=12),
        timedelta(hours=24),
    )


def test_candidate_universe_rejects_digest_count_and_noncanonical_order() -> None:
    with pytest.raises(ValueError, match="candidate_ids must be sorted"):
        _universe(candidate_ids=("solana:mint-b:pool-2", "solana:mint-a:pool-1"))
    with pytest.raises(ValueError, match="candidate_count"):
        _universe(candidate_count=1)
    with pytest.raises(ValueError, match="candidate_digest"):
        _universe(candidate_digest="0" * 64)


def _cohort(universe: CandidateUniverseV1, rows: tuple[CandidateDisposition, ...]) -> CohortSnapshotV1:
    return CohortSnapshotV1(universe=universe, dispositions=rows)


def test_public_cohort_boundary_keeps_dead_rug_nonexitable_and_missing_rows() -> None:
    universe = _universe()
    rows = (
        CandidateDisposition(candidate_id="solana:mint-a:pool-1", kind=CandidateDispositionKind.RUG),
        CandidateDisposition(candidate_id="solana:mint-b:pool-2", kind=CandidateDispositionKind.MISSING),
    )
    assert validate_candidate_cohort(_cohort(universe, rows)) == rows
    with pytest.raises(ValueError, match="exactly cover"):
        validate_candidate_cohort(_cohort(universe, rows[:1]))


def test_public_cohort_boundary_revalidates_constructed_and_copied_tampering() -> None:
    universe = _universe()
    rows = (
        CandidateDisposition(candidate_id="solana:mint-a:pool-1", kind=CandidateDispositionKind.DEAD),
        CandidateDisposition(candidate_id="solana:mint-b:pool-2", kind=CandidateDispositionKind.NONEXITABLE),
    )
    snapshot = _cohort(universe, rows)
    bad_universe = CandidateUniverseV1.model_construct(**{**universe.model_dump(), "candidate_count": 1})
    bad_snapshot = CohortSnapshotV1.model_construct(
        universe=bad_universe,
        dispositions=rows,
        disposition_digest=snapshot.disposition_digest,
    )
    with pytest.raises(ValueError, match="candidate_count"):
        validate_candidate_cohort(bad_snapshot)
    tampered = CohortSnapshotV1.model_construct(
        universe=snapshot.universe,
        dispositions=(rows[0].model_copy(update={"kind": CandidateDispositionKind.ELIGIBLE}), rows[1]),
        disposition_digest=snapshot.disposition_digest,
    )
    with pytest.raises(ValueError, match="disposition_digest"):
        validate_candidate_cohort(tampered)
    with pytest.raises(ValueError, match="disposition_digest"):
        snapshot.model_copy(
            update={
                "dispositions": (rows[0].model_copy(update={"kind": "eligible"}), rows[1]),
            }
        )
    with pytest.raises(ValueError, match="candidate_digest"):
        universe.model_copy(update={"candidate_digest": "f" * 64})


def test_field_classes_are_descriptive_only_in_phase8d1() -> None:
    assert list(FieldPITClass) == [
        FieldPITClass.HISTORICAL_PIT_VERIFIED,
        FieldPITClass.RECONSTRUCTABLE_ONCHAIN,
        FieldPITClass.REALTIME_ONLY,
        FieldPITClass.PROPRIETARY_ENRICHMENT,
        FieldPITClass.UNVERIFIED,
    ]


def test_outcome_keeps_raw_market_return_distinct_from_cost_sensitivity() -> None:
    outcome = DelayedOutcome(
        candidate_id="solana:mint-a:pool-1",
        entry_delay=timedelta(hours=3),
        holding_period=timedelta(hours=1),
        status=CellOutcomeStatus.COMPLETE,
        raw_market_return=Decimal("0.25"),
        cost_sensitivity_estimate=Decimal("0.08"),
        survival=True,
        liquidity_collapse=False,
        rug_or_nonexitable=False,
        max_drawdown=Decimal("-0.12"),
        observed_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert outcome.raw_market_return == Decimal("0.25")
    assert outcome.cost_sensitivity_estimate == Decimal("0.08")
    assert outcome.warning == "not_executable_pnl_cost_sensitivity_only"
