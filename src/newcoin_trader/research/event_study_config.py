"""Phase 3 event-study configuration: delays, holdings, and CLI bounds."""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import datetime, timedelta

from newcoin_trader.domain.event_study import TokenListingEvent
from newcoin_trader.domain.types import require_utc
from newcoin_trader.errors import ConfigError

DEFAULT_ENTRY_DELAYS: tuple[timedelta, ...] = (
    timedelta(seconds=10),
    timedelta(seconds=30),
    timedelta(minutes=1),
    timedelta(minutes=2),
    timedelta(minutes=5),
    timedelta(minutes=10),
    timedelta(minutes=15),
    timedelta(minutes=30),
)

DEFAULT_HOLDING_PERIODS: tuple[timedelta, ...] = (
    timedelta(minutes=1),
    timedelta(minutes=5),
    timedelta(minutes=15),
    timedelta(minutes=30),
    timedelta(hours=1),
    timedelta(hours=2),
    timedelta(hours=4),
    timedelta(hours=24),
)

# Bounded CLI: no analyze-everything default; require explicit max_events.
MAX_EVENTS_MIN = 1
MAX_EVENTS_MAX = 10_000
MAX_GRID_ITEMS = 32
# Per-run observation read budget (deterministic; exceeding raises, never truncates).
DEFAULT_MAX_OBSERVATIONS = 250_000
MAX_OBSERVATIONS_MIN = 1
MAX_OBSERVATIONS_MAX = 5_000_000

_DURATION_RE = re.compile(r"^(\d+)(s|m|h|d)$")

ELIGIBILITY_RULES: tuple[str, ...] = (
    "entry_time_must_be_at_or_after_decision_available_time",
    "entry_and_exit_require_exact_timestamp_observation",
    "no_forward_fill_for_missing_exit",
    "subminute_entry_delays_require_point_resolution_observations",
    "venues_never_pooled",
    "nonpositive_prices_are_invalid_market_data",
    "future_observations_must_not_affect_entry_eligibility",
    "right_censored_when_exit_beyond_data_horizon",
    "path_observations_require_finite_positive_prices",
)


def parse_duration(spec: str) -> timedelta:
    """Parse a duration like ``10s``, ``1m``, ``2h``, ``1d``."""
    text = spec.strip().lower()
    match = _DURATION_RE.fullmatch(text)
    if match is None:
        raise ConfigError(f"unsupported duration: {spec!r} (expected e.g. 10s, 1m, 2h)")
    amount = int(match.group(1))
    if amount <= 0:
        raise ConfigError(f"duration must be positive: {spec!r}")
    unit = match.group(2)
    if unit == "s":
        return timedelta(seconds=amount)
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    return timedelta(days=amount)


def format_duration(delta: timedelta) -> str:
    seconds = int(delta.total_seconds())
    if seconds <= 0:
        raise ConfigError(f"duration must be positive seconds: {delta!r}")
    if seconds % 86400 == 0:
        return f"{seconds // 86400}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def parse_duration_list(
    specs: list[str] | tuple[str, ...] | None,
    *,
    default: tuple[timedelta, ...],
) -> tuple[timedelta, ...]:
    if specs is None or len(specs) == 0:
        return default
    if len(specs) > MAX_GRID_ITEMS:
        raise ConfigError(f"at most {MAX_GRID_ITEMS} durations allowed")
    parsed = tuple(parse_duration(item) for item in specs)
    # Deterministic unique preserve order
    seen: set[timedelta] = set()
    ordered: list[timedelta] = []
    for item in parsed:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return tuple(ordered)


def validate_event_study_bounds(
    *,
    start: datetime,
    end: datetime,
    max_events: int,
    entry_delays: tuple[timedelta, ...],
    holding_periods: tuple[timedelta, ...],
    max_observations: int = DEFAULT_MAX_OBSERVATIONS,
) -> None:
    start_utc = require_utc(start)
    end_utc = require_utc(end)
    if end_utc <= start_utc:
        raise ConfigError("event-study end must be after start")
    if max_events < MAX_EVENTS_MIN or max_events > MAX_EVENTS_MAX:
        raise ConfigError(f"max_events must be in [{MAX_EVENTS_MIN}, {MAX_EVENTS_MAX}]")
    if max_observations < MAX_OBSERVATIONS_MIN or max_observations > MAX_OBSERVATIONS_MAX:
        raise ConfigError(f"max_observations must be in [{MAX_OBSERVATIONS_MIN}, {MAX_OBSERVATIONS_MAX}]")
    if not entry_delays:
        raise ConfigError("entry_delays must be non-empty")
    if not holding_periods:
        raise ConfigError("holding_periods must be non-empty")
    if len(entry_delays) > MAX_GRID_ITEMS:
        raise ConfigError(f"at most {MAX_GRID_ITEMS} entry_delays allowed")
    if len(holding_periods) > MAX_GRID_ITEMS:
        raise ConfigError(f"at most {MAX_GRID_ITEMS} holding_periods allowed")
    for delay in entry_delays:
        if delay.total_seconds() <= 0:
            raise ConfigError("entry delays must be positive")
    for holding in holding_periods:
        if holding.total_seconds() <= 0:
            raise ConfigError("holding periods must be positive")


def observation_snapshot_bounds(
    events: Sequence[TokenListingEvent],
    entry_delays: Sequence[timedelta],
    holding_periods: Sequence[timedelta],
) -> tuple[datetime, datetime]:
    """Derive inclusive snapshot window from selected events and the delay/holding grid.

    Lower bound is the earliest ``source_event_time`` (earliest entry need is at or after
    that clock). Upper bound is the latest ``source_event_time`` plus max entry delay plus
    max holding period.
    """
    if not events:
        raise ConfigError("observation_snapshot_bounds requires at least one event")
    if not entry_delays or not holding_periods:
        raise ConfigError("observation_snapshot_bounds requires non-empty delay and holding grids")
    earliest = min(event.source_event_time for event in events)
    latest = max(event.source_event_time for event in events)
    return earliest, latest + max(entry_delays) + max(holding_periods)
