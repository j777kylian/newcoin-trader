"""Phase 8D.1 static source/venue qualification descriptors; no I/O."""

from __future__ import annotations

from dataclasses import dataclass

from newcoin_trader.domain.delayed_entry_research import FieldPITClass


@dataclass(frozen=True)
class VenueQualification:
    role: str
    historical_pit_complete: bool
    first_trade_definition: str
    operational_note: str


def source_capability_matrix() -> dict[str, dict[str, FieldPITClass]]:
    """Conservative capability classes; no current-state source is historical PIT fact."""
    return {
        "base_rpc": {
            "first_trade": FieldPITClass.RECONSTRUCTABLE_ONCHAIN,
            "price": FieldPITClass.RECONSTRUCTABLE_ONCHAIN,
            "liquidity": FieldPITClass.RECONSTRUCTABLE_ONCHAIN,
        },
        "solana_rpc": {
            "first_trade": FieldPITClass.RECONSTRUCTABLE_ONCHAIN,
            "price": FieldPITClass.RECONSTRUCTABLE_ONCHAIN,
            "liquidity": FieldPITClass.RECONSTRUCTABLE_ONCHAIN,
        },
        "gmgn": {
            "price": FieldPITClass.REALTIME_ONLY,
            "liquidity": FieldPITClass.REALTIME_ONLY,
            "smart_money_activity": FieldPITClass.PROPRIETARY_ENRICHMENT,
            "holder_count": FieldPITClass.PROPRIETARY_ENRICHMENT,
        },
    }


def venue_qualification_matrix() -> dict[str, VenueQualification]:
    """Evidence-conservative comparison; no primary venue is selected yet."""
    return {
        "solana": VenueQualification(
            role="primary_candidate",
            historical_pit_complete=False,
            first_trade_definition="protocol_specific_canonical_swap_requires_future_proof_chain",
            operational_note="archive-provider and protocol-specific contiguous-universe proof remain required",
        ),
        "base": VenueQualification(
            role="secondary_reference",
            historical_pit_complete=False,
            first_trade_definition="accepted_base_uniswap_v3_factory_slice_only",
            operational_note="qualified provider has a 10-block getLogs operational ceiling; no factory anchor/corpus",
        ),
        "bsc": VenueQualification(
            role="optional_future",
            historical_pit_complete=False,
            first_trade_definition="not_yet_qualified",
            operational_note="no source qualification performed",
        ),
    }
