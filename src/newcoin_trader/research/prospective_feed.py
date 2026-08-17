"""Phase 6.5 prospective feed protocol, bounds, and venue factory."""

from __future__ import annotations

from datetime import timedelta
from enum import StrEnum
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from newcoin_trader.domain.live_paper import ReplayMarketEvent
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.event_study_config import MAX_EVENTS_MAX, MAX_EVENTS_MIN
from newcoin_trader.research.live_paper_config import (
    DURATION_MAX,
    QUEUE_CAPACITY_MAX,
    QUEUE_CAPACITY_MIN,
)

MAX_POLLS_MIN = 1
MAX_POLLS_MAX = 100_000
MAX_OBS_MIN = 1
MAX_OBS_MAX = 100_000
POLL_INTERVAL_MAX = timedelta(hours=1)


class ProspectiveFeedStatus(StrEnum):
    OK = "ok"
    SOURCE_UNAVAILABLE = "source_unavailable"
    BOUNDS_REACHED = "bounds_reached"
    QUEUE_OVERFLOW = "queue_overflow"


class ProspectiveFeedResult(BaseModel):
    """Bounded prospective collect outcome (no fabricated market input)."""

    model_config = ConfigDict(frozen=True)

    events: tuple[ReplayMarketEvent, ...] = ()
    status: ProspectiveFeedStatus
    poll_count: int = 0
    overflow_count: int = 0
    rejected_count: int = 0
    duplicate_suppressed_count: int = 0
    source_errors: tuple[str, ...] = ()
    observations_emitted: int = 0
    extras: dict[str, Any] = Field(default_factory=dict)


class ProspectiveFeed(Protocol):
    """Injected prospective observation source. Implementations must stay GET-only."""

    async def collect_bounded(self) -> ProspectiveFeedResult: ...


def validate_prospective_feed_bounds(
    *,
    poll_interval: timedelta,
    duration: timedelta,
    max_polls: int,
    max_events: int,
    max_observations_per_token: int,
    max_total_observations: int,
    queue_capacity: int,
) -> None:
    if poll_interval.total_seconds() < 0:
        raise ConfigError("prospective poll_interval must be >= 0")
    if poll_interval > POLL_INTERVAL_MAX:
        raise ConfigError(f"prospective poll_interval must be <= {POLL_INTERVAL_MAX}")
    if duration.total_seconds() <= 0:
        raise ConfigError("prospective duration must be positive (no indefinite loop)")
    if duration > DURATION_MAX:
        raise ConfigError(f"prospective duration must be <= {DURATION_MAX}")
    if max_polls < MAX_POLLS_MIN or max_polls > MAX_POLLS_MAX:
        raise ConfigError(f"max_polls must be in [{MAX_POLLS_MIN}, {MAX_POLLS_MAX}]")
    if max_events < MAX_EVENTS_MIN or max_events > MAX_EVENTS_MAX:
        raise ConfigError(f"max_events must be in [{MAX_EVENTS_MIN}, {MAX_EVENTS_MAX}]")
    if max_observations_per_token < MAX_OBS_MIN or max_observations_per_token > MAX_OBS_MAX:
        raise ConfigError(f"max_observations_per_token must be in [{MAX_OBS_MIN}, {MAX_OBS_MAX}]")
    if max_total_observations < MAX_OBS_MIN or max_total_observations > MAX_OBS_MAX:
        raise ConfigError(f"max_total_observations must be in [{MAX_OBS_MIN}, {MAX_OBS_MAX}]")
    if queue_capacity < QUEUE_CAPACITY_MIN or queue_capacity > QUEUE_CAPACITY_MAX:
        raise ConfigError(f"queue_capacity must be in [{QUEUE_CAPACITY_MIN}, {QUEUE_CAPACITY_MAX}]")


def build_prospective_feed(
    *,
    venue: str,
    client: Any,
    now: Any,
    symbol: str,
    poll_interval: timedelta,
    duration: timedelta,
    max_polls: int,
    max_events: int,
    max_observations_per_token: int,
    max_total_observations: int,
    queue_capacity: int,
    sleep: Any = None,
) -> ProspectiveFeed:
    """Construct a prospective feed for a supported venue only (no silent replay fallback)."""
    normalized = venue.strip().lower()
    if normalized != "binance":
        raise ConfigError(
            f"unsupported prospective venue: {venue!r} "
            "(Phase 6.5 supports binance public Spot only; "
            "raydium/birdeye/geckoterminal are not prospective-ready)"
        )
    from newcoin_trader.research.prospective_binance import BinanceProspectiveFeed

    kwargs: dict[str, Any] = {
        "client": client,
        "now": now,
        "symbol": symbol,
        "poll_interval": poll_interval,
        "duration": duration,
        "max_polls": max_polls,
        "max_events": max_events,
        "max_observations_per_token": max_observations_per_token,
        "max_total_observations": max_total_observations,
        "queue_capacity": queue_capacity,
    }
    if sleep is not None:
        kwargs["sleep"] = sleep
    return BinanceProspectiveFeed(**kwargs)


def summarize_feed_result(result: ProspectiveFeedResult) -> dict[str, Any]:
    return {
        "status": result.status.value,
        "poll_count": result.poll_count,
        "overflow_count": result.overflow_count,
        "rejected_count": result.rejected_count,
        "duplicate_suppressed_count": result.duplicate_suppressed_count,
        "source_errors": list(result.source_errors),
        "event_count": len(result.events),
        "observations_emitted": result.observations_emitted,
    }


__all__ = [
    "ProspectiveFeed",
    "ProspectiveFeedResult",
    "ProspectiveFeedStatus",
    "build_prospective_feed",
    "summarize_feed_result",
    "validate_prospective_feed_bounds",
]
