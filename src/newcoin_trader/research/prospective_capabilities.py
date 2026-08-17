"""Phase 6.5 prospective feed capability labels (evidence-backed, not aspirational)."""

from __future__ import annotations

from enum import StrEnum

from newcoin_trader.domain.enums import Venue


class ProspectiveReadiness(StrEnum):
    READY = "ready"
    PARTIAL = "partial"
    NOT_READY = "not_ready"


# Mirrors the collector capability matrix in the Phase 6.5 plan.
_PROSPECTIVE_MATRIX: dict[Venue, dict[str, str]] = {
    Venue.BINANCE: {
        "readiness": ProspectiveReadiness.READY.value,
        "discovery": "exchangeInfo trading symbols; onboarding time sometimes present",
        "market_state": "public ticker",
        "trades": "recent trades (public GET)",
        "depth": "public L2 /depth",
        "auth": "none",
        "transport": "GetJsonClient GET-only",
        "notes": "Phase 6.5 first venue: one explicit configured symbol, bounded polling",
    },
    Venue.BIRDEYE: {
        "readiness": ProspectiveReadiness.PARTIAL.value,
        "discovery": "new tokens/pairs",
        "market_state": "no market-state client method in current collector",
        "trades": "none",
        "depth": "none",
        "auth": "public-data API key",
        "transport": "GET-only",
        "notes": "not admitted as prospective venue in this pass",
    },
    Venue.RAYDIUM: {
        "readiness": ProspectiveReadiness.PARTIAL.value,
        "discovery": "pool list",
        "market_state": "pool snapshots and read-only quote compute",
        "trades": "none",
        "depth": "no L2",
        "auth": "none",
        "transport": "GET-only",
        "notes": "no normalized prospective event bridge in this pass",
    },
    Venue.GECKO: {
        "readiness": ProspectiveReadiness.NOT_READY.value,
        "discovery": "no discovery",
        "market_state": "pool state / minute OHLCV for supplied pool",
        "trades": "none",
        "depth": "none",
        "auth": "none",
        "transport": "GET-only",
        "notes": "needs externally supplied pool; minute OHLCV only",
    },
}


def prospective_capability_matrix() -> dict[str, dict[str, str]]:
    return {
        venue.value: dict(sorted(fields.items()))
        for venue, fields in sorted(_PROSPECTIVE_MATRIX.items(), key=lambda item: item[0].value)
    }


def prospective_venue_supported(venue: str) -> bool:
    return venue.strip().lower() == Venue.BINANCE.value


__all__ = [
    "ProspectiveReadiness",
    "prospective_capability_matrix",
    "prospective_venue_supported",
]
