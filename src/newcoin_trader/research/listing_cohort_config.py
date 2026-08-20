"""Phase 8.1 listing-cohort research bounds. Delay/holding grids reuse Phase 3 exactly."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from newcoin_trader.domain.types import require_utc
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.event_study_config import (
    DEFAULT_ENTRY_DELAYS as DEFAULT_ENTRY_DELAYS,
)
from newcoin_trader.research.event_study_config import (
    DEFAULT_HOLDING_PERIODS as DEFAULT_HOLDING_PERIODS,
)
from newcoin_trader.research.event_study_config import (
    format_duration,
)
from newcoin_trader.research.feature_research_config import DEFAULT_SPLIT_RATIOS

PHASE81_SPLIT_RATIOS: tuple[Decimal, Decimal, Decimal] = DEFAULT_SPLIT_RATIOS

TARGET_VALID_CRYPTO_LISTINGS = 50
PILOT_LOOKBACK = timedelta(days=1095)

MAX_PROBE_DAYS_CAP = 31
MAX_LOOKBACK_BEFORE_DAYS = 3


def format_listing_duration(delta: timedelta) -> str:
    """Format grid durations with Phase 3 semantics (no invented 0m delay)."""
    return format_duration(delta)


def clamp_research_end(*, requested_end: datetime, now_utc: datetime) -> datetime:
    """Inclusive research end is never later than actual current UTC."""
    return min(require_utc(requested_end), require_utc(now_utc))


def listing_search_start(*, requested_start: datetime, effective_end: datetime) -> datetime:
    """Lower bound is the later of the requested start and a 3-year lookback."""
    start = require_utc(requested_start)
    end = require_utc(effective_end)
    return max(start, end - PILOT_LOOKBACK)


def validate_listing_cohort_bounds(
    *,
    start: datetime,
    end: datetime,
    max_probe_days: int,
    lookback_before_days: int,
) -> None:
    start_utc = require_utc(start)
    end_utc = require_utc(end)
    if end_utc <= start_utc:
        raise ConfigError("listing-cohort end must be after start")
    if max_probe_days < 1 or max_probe_days > MAX_PROBE_DAYS_CAP:
        raise ConfigError(f"max_probe_days must be in [1, {MAX_PROBE_DAYS_CAP}]")
    if lookback_before_days < 0 or lookback_before_days > MAX_LOOKBACK_BEFORE_DAYS:
        raise ConfigError(f"lookback_before_days must be in [0, {MAX_LOOKBACK_BEFORE_DAYS}]")
