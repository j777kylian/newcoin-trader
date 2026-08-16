"""Venue execution capability matrix for Phase 5 (evidence-backed, not aspirational)."""

from __future__ import annotations

from newcoin_trader.domain.enums import Venue
from newcoin_trader.domain.executable_backtest import AvailabilityLevel

# Components reflect persisted historical capability in this codebase's schema.
_MATRIX: dict[Venue, dict[str, AvailabilityLevel]] = {
    Venue.BINANCE: {
        "historical_trades": AvailabilityLevel.SUPPORTED,
        "historical_price": AvailabilityLevel.SUPPORTED,
        "historical_depth": AvailabilityLevel.UNSUPPORTED,  # no depth table; supplied input only
        "historical_liquidity": AvailabilityLevel.PARTIAL,
        "pool_reserves": AvailabilityLevel.UNSUPPORTED,
        "historical_fees": AvailabilityLevel.UNSUPPORTED,
        "depth_walk_when_supplied": AvailabilityLevel.SUPPORTED,
        "modeled_price_fallback": AvailabilityLevel.SUPPORTED,
    },
    Venue.RAYDIUM: {
        "historical_trades": AvailabilityLevel.UNSUPPORTED,
        "historical_price": AvailabilityLevel.PARTIAL,
        "historical_depth": AvailabilityLevel.UNSUPPORTED,
        "historical_liquidity": AvailabilityLevel.PARTIAL,
        "pool_reserves": AvailabilityLevel.UNSUPPORTED,
        "historical_fees": AvailabilityLevel.UNSUPPORTED,
        "depth_walk_when_supplied": AvailabilityLevel.UNSUPPORTED,
        "modeled_liquidity_impact": AvailabilityLevel.SUPPORTED,
    },
    Venue.GECKO: {
        "historical_trades": AvailabilityLevel.UNSUPPORTED,
        "historical_price": AvailabilityLevel.SUPPORTED,
        "historical_depth": AvailabilityLevel.UNSUPPORTED,
        "historical_liquidity": AvailabilityLevel.PARTIAL,
        "pool_reserves": AvailabilityLevel.UNSUPPORTED,
        "historical_fees": AvailabilityLevel.UNSUPPORTED,
        "depth_walk_when_supplied": AvailabilityLevel.UNSUPPORTED,
        "modeled_liquidity_impact": AvailabilityLevel.SUPPORTED,
    },
    Venue.BIRDEYE: {
        "historical_trades": AvailabilityLevel.UNSUPPORTED,
        "historical_price": AvailabilityLevel.UNSUPPORTED,
        "historical_depth": AvailabilityLevel.UNSUPPORTED,
        "historical_liquidity": AvailabilityLevel.UNSUPPORTED,
        "pool_reserves": AvailabilityLevel.UNSUPPORTED,
        "historical_fees": AvailabilityLevel.UNSUPPORTED,
        "depth_walk_when_supplied": AvailabilityLevel.UNSUPPORTED,
        "modeled_liquidity_impact": AvailabilityLevel.UNSUPPORTED,
    },
}


def classify_execution_component(venue: Venue, component: str) -> AvailabilityLevel:
    table = _MATRIX.get(venue)
    if table is None:
        return AvailabilityLevel.UNSUPPORTED
    return table.get(component, AvailabilityLevel.UNSUPPORTED)


def capability_matrix() -> dict[str, dict[str, AvailabilityLevel]]:
    return {
        venue.value: {component: level for component, level in sorted(families.items())}
        for venue, families in sorted(_MATRIX.items(), key=lambda item: item[0].value)
    }


def capability_matrix_str() -> dict[str, dict[str, str]]:
    return {
        venue: {component: level.value for component, level in families.items()}
        for venue, families in capability_matrix().items()
    }
