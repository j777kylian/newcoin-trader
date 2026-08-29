"""Phase 8D.1 source/venue capability matrix tests."""

from __future__ import annotations

from newcoin_trader.domain.delayed_entry_research import FieldPITClass
from newcoin_trader.research.phase8d import source_capability_matrix, venue_qualification_matrix


def test_phase8d_source_matrix_is_explicit_and_does_not_promote_gmgn_current_state() -> None:
    matrix = source_capability_matrix()
    assert matrix["gmgn"]["price"] is FieldPITClass.REALTIME_ONLY
    assert matrix["gmgn"]["smart_money_activity"] is FieldPITClass.PROPRIETARY_ENRICHMENT
    assert matrix["base_rpc"]["first_trade"] is FieldPITClass.RECONSTRUCTABLE_ONCHAIN
    assert matrix["solana_rpc"]["first_trade"] is FieldPITClass.RECONSTRUCTABLE_ONCHAIN


def test_phase8d_venue_matrix_keeps_base_as_reference_and_no_primary_claim_before_source_proof() -> None:
    matrix = venue_qualification_matrix()
    assert matrix["base"].role == "secondary_reference"
    assert matrix["solana"].role == "primary_candidate"
    assert matrix["bsc"].role == "optional_future"
    assert matrix["solana"].historical_pit_complete is False
