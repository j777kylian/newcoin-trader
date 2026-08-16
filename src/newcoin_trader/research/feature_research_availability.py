"""Actual feature-availability matrix from existing Phase 1/2 storage."""

from __future__ import annotations

from newcoin_trader.domain.enums import Venue
from newcoin_trader.domain.feature_research import AvailabilityLevel

# Families intentionally excluded (no corresponding storage).
EXCLUDED_FAMILIES: tuple[str, ...] = (
    "holder",
    "creator",
    "social",
    "security",
    "wallet",
)

# Evidence-backed initial Phase 4 treatment (see plan availability matrix).
_MATRIX: dict[Venue, dict[str, AvailabilityLevel]] = {
    Venue.BINANCE: {
        "age": AvailabilityLevel.SUPPORTED,
        "price_momentum": AvailabilityLevel.SUPPORTED,
        "volatility": AvailabilityLevel.SUPPORTED,
        "volume": AvailabilityLevel.SUPPORTED,
        "activity": AvailabilityLevel.PARTIAL,
        "buy_sell_imbalance": AvailabilityLevel.PARTIAL,
        "liquidity": AvailabilityLevel.PARTIAL,
        "venue_chain_identity": AvailabilityLevel.SUPPORTED,
        "holder": AvailabilityLevel.UNSUPPORTED,
        "creator": AvailabilityLevel.UNSUPPORTED,
        "social": AvailabilityLevel.UNSUPPORTED,
        "security": AvailabilityLevel.UNSUPPORTED,
        "wallet": AvailabilityLevel.UNSUPPORTED,
    },
    Venue.GECKO: {
        "age": AvailabilityLevel.SUPPORTED,
        "price_momentum": AvailabilityLevel.SUPPORTED,
        "volatility": AvailabilityLevel.SUPPORTED,
        "volume": AvailabilityLevel.SUPPORTED,
        "activity": AvailabilityLevel.UNSUPPORTED,
        "buy_sell_imbalance": AvailabilityLevel.UNSUPPORTED,
        "liquidity": AvailabilityLevel.PARTIAL,
        "venue_chain_identity": AvailabilityLevel.SUPPORTED,
        "holder": AvailabilityLevel.UNSUPPORTED,
        "creator": AvailabilityLevel.UNSUPPORTED,
        "social": AvailabilityLevel.UNSUPPORTED,
        "security": AvailabilityLevel.UNSUPPORTED,
        "wallet": AvailabilityLevel.UNSUPPORTED,
    },
    Venue.RAYDIUM: {
        "age": AvailabilityLevel.SUPPORTED,
        "price_momentum": AvailabilityLevel.PARTIAL,
        "volatility": AvailabilityLevel.PARTIAL,
        "volume": AvailabilityLevel.PARTIAL,
        "activity": AvailabilityLevel.UNSUPPORTED,
        "buy_sell_imbalance": AvailabilityLevel.UNSUPPORTED,
        "liquidity": AvailabilityLevel.PARTIAL,
        "venue_chain_identity": AvailabilityLevel.SUPPORTED,
        "holder": AvailabilityLevel.UNSUPPORTED,
        "creator": AvailabilityLevel.UNSUPPORTED,
        "social": AvailabilityLevel.UNSUPPORTED,
        "security": AvailabilityLevel.UNSUPPORTED,
        "wallet": AvailabilityLevel.UNSUPPORTED,
    },
    Venue.BIRDEYE: {
        # Discovery venue: treat market families as unsupported unless snapshots exist under another venue.
        "age": AvailabilityLevel.SUPPORTED,
        "price_momentum": AvailabilityLevel.UNSUPPORTED,
        "volatility": AvailabilityLevel.UNSUPPORTED,
        "volume": AvailabilityLevel.UNSUPPORTED,
        "activity": AvailabilityLevel.UNSUPPORTED,
        "buy_sell_imbalance": AvailabilityLevel.UNSUPPORTED,
        "liquidity": AvailabilityLevel.UNSUPPORTED,
        "venue_chain_identity": AvailabilityLevel.SUPPORTED,
        "holder": AvailabilityLevel.UNSUPPORTED,
        "creator": AvailabilityLevel.UNSUPPORTED,
        "social": AvailabilityLevel.UNSUPPORTED,
        "security": AvailabilityLevel.UNSUPPORTED,
        "wallet": AvailabilityLevel.UNSUPPORTED,
    },
}


def classify_family(venue: Venue, family: str) -> AvailabilityLevel:
    table = _MATRIX.get(venue)
    if table is None:
        return AvailabilityLevel.UNSUPPORTED
    return table.get(family, AvailabilityLevel.UNSUPPORTED)


def availability_matrix() -> dict[str, dict[str, AvailabilityLevel]]:
    """Deterministic venue → family → level mapping for artifacts."""
    return {
        venue.value: {family: level for family, level in sorted(families.items())}
        for venue, families in sorted(_MATRIX.items(), key=lambda item: item[0].value)
    }


def availability_matrix_str() -> dict[str, dict[str, str]]:
    return {
        venue: {family: level.value for family, level in families.items()}
        for venue, families in availability_matrix().items()
    }
