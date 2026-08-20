"""Phase 8.1B reuses Phase 3 grids and clamps research end to current UTC."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from newcoin_trader.research import event_study_config, listing_cohort_config
from newcoin_trader.research.event_study_config import DEFAULT_ENTRY_DELAYS, DEFAULT_HOLDING_PERIODS, format_duration
from newcoin_trader.research.listing_cohort_config import (
    PILOT_LOOKBACK,
    TARGET_VALID_CRYPTO_LISTINGS,
    clamp_research_end,
    format_listing_duration,
    listing_search_start,
)


def test_phase81_does_not_invent_delay_or_holding_grids() -> None:
    assert not hasattr(listing_cohort_config, "PHASE81_ENTRY_DELAYS")
    assert not hasattr(listing_cohort_config, "PHASE81_HOLDING_PERIODS")
    assert listing_cohort_config.DEFAULT_ENTRY_DELAYS is event_study_config.DEFAULT_ENTRY_DELAYS
    assert listing_cohort_config.DEFAULT_HOLDING_PERIODS is event_study_config.DEFAULT_HOLDING_PERIODS


def test_listing_cohort_reuses_phase3_entry_and_holding_grids() -> None:
    assert tuple(DEFAULT_ENTRY_DELAYS) == (
        timedelta(seconds=10),
        timedelta(seconds=30),
        timedelta(minutes=1),
        timedelta(minutes=2),
        timedelta(minutes=5),
        timedelta(minutes=10),
        timedelta(minutes=15),
        timedelta(minutes=30),
    )
    assert tuple(DEFAULT_HOLDING_PERIODS) == (
        timedelta(minutes=1),
        timedelta(minutes=5),
        timedelta(minutes=15),
        timedelta(minutes=30),
        timedelta(hours=1),
        timedelta(hours=2),
        timedelta(hours=4),
        timedelta(hours=24),
    )
    assert timedelta(0) not in DEFAULT_ENTRY_DELAYS
    assert timedelta(minutes=3) not in DEFAULT_ENTRY_DELAYS
    assert [format_listing_duration(d) for d in DEFAULT_ENTRY_DELAYS] == [
        "10s",
        "30s",
        "1m",
        "2m",
        "5m",
        "10m",
        "15m",
        "30m",
    ]
    assert [format_listing_duration(h) for h in DEFAULT_HOLDING_PERIODS] == [
        format_duration(h) for h in DEFAULT_HOLDING_PERIODS
    ]
    assert "0m" not in [format_listing_duration(d) for d in DEFAULT_ENTRY_DELAYS]


def test_pilot_selection_bounds_are_fifty_listings_and_three_year_lookback() -> None:
    assert TARGET_VALID_CRYPTO_LISTINGS == 50
    assert PILOT_LOOKBACK == timedelta(days=1095)


def test_collect_classified_listings_docstring_targets_fifty_and_three_year_lookback() -> None:
    from newcoin_trader.research.listing_cohort_run import collect_classified_listings

    doc = collect_classified_listings.__doc__ or ""
    assert "50" in doc
    assert "20 valid" not in doc
    assert "12-month" not in doc


def test_clamp_research_end_uses_min_of_requested_and_now() -> None:
    now = datetime(2026, 8, 19, 4, 40, tzinfo=UTC)
    requested = datetime(2026, 12, 31, tzinfo=UTC)
    assert clamp_research_end(requested_end=requested, now_utc=now) == now
    past = datetime(2024, 2, 1, tzinfo=UTC)
    assert clamp_research_end(requested_end=past, now_utc=now) == past


def test_listing_search_start_caps_lookback_at_three_years() -> None:
    effective_end = datetime(2026, 8, 19, tzinfo=UTC)
    requested_start = datetime(2020, 1, 1, tzinfo=UTC)
    start = listing_search_start(requested_start=requested_start, effective_end=effective_end)
    assert start == effective_end - timedelta(days=1095)
    recent = datetime(2026, 7, 1, tzinfo=UTC)
    assert listing_search_start(requested_start=recent, effective_end=effective_end) == recent


def test_clamp_rejects_naive_datetimes() -> None:
    now = datetime(2026, 8, 19, tzinfo=UTC)
    with pytest.raises(ValueError):
        clamp_research_end(requested_end=datetime(2026, 12, 31), now_utc=now)
