"""Pure deterministic Phase 3 event-study engine (no I/O, no strategy PnL)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal, DecimalException

from newcoin_trader.domain.enums import Venue
from newcoin_trader.domain.event_study import (
    CellOutcomeStatus,
    EventStudyCellResult,
    MarketObservation,
    ObservationResolution,
    PathStats,
    TokenListingEvent,
)
from newcoin_trader.research.event_study_resolution import finest_resolution, supports_entry_delay


def _obs_sort_key(obs: MarketObservation) -> tuple[datetime, str, str, str, str]:
    return (
        obs.timestamp,
        obs.venue.value,
        obs.token_address,
        obs.source,
        obs.resolution.value,
    )


def _dedupe_key(obs: MarketObservation) -> tuple[Venue, str, str, datetime]:
    return (obs.venue, obs.chain, obs.token_address, obs.timestamp)


def _resolution_rank(resolution: ObservationResolution) -> int:
    # Prefer finer products when collapsing duplicate timestamps.
    if resolution is ObservationResolution.POINT:
        return 0
    if resolution is ObservationResolution.MINUTE:
        return 1
    return 2


def prepare_observations(observations: Sequence[MarketObservation]) -> tuple[MarketObservation, ...]:
    """Sort chronologically and deterministically de-duplicate exact timestamps."""
    ordered = sorted(observations, key=_obs_sort_key)
    chosen: dict[tuple[Venue, str, str, datetime], MarketObservation] = {}
    for obs in ordered:
        key = _dedupe_key(obs)
        existing = chosen.get(key)
        if existing is None:
            chosen[key] = obs
            continue
        # Prefer finer resolution, then lexicographically smaller source for stability.
        if (_resolution_rank(obs.resolution), obs.source) < (
            _resolution_rank(existing.resolution),
            existing.source,
        ):
            chosen[key] = obs
    return tuple(sorted(chosen.values(), key=_obs_sort_key))


def _filter_event_obs(
    observations: Sequence[MarketObservation],
    event: TokenListingEvent,
) -> tuple[MarketObservation, ...]:
    return tuple(
        obs
        for obs in observations
        if obs.venue == event.venue and obs.token_address == event.token_address and obs.chain == event.chain.value
    )


def _lookup_at(
    by_ts: dict[datetime, MarketObservation],
    timestamp: datetime,
) -> MarketObservation | None:
    return by_ts.get(timestamp)


def _is_finite_positive_price(price: Decimal) -> bool:
    return price.is_finite() and price > 0


def _compute_returns(entry: Decimal, exit_: Decimal) -> tuple[Decimal, Decimal] | None:
    """Return finite simple/log returns, or None when arithmetic cannot be safely finite."""
    if not _is_finite_positive_price(entry) or not _is_finite_positive_price(exit_):
        return None
    try:
        ratio = exit_ / entry
        if not ratio.is_finite() or ratio <= 0:
            return None
        simple = ratio - Decimal("1")
        if not simple.is_finite():
            return None
        log_ret = ratio.ln()
        if not log_ret.is_finite():
            return None
        return simple, log_ret
    except DecimalException:
        return None


def _path_stats(
    path: Sequence[MarketObservation],
    *,
    entry_time: datetime,
    entry_price: Decimal,
) -> PathStats | None:
    """Build path extrema, or None if any included observation has invalid price.

    Policy: any non-finite or non-positive price on an included path point yields
    ``invalid_market_data`` (never silently drop the point).
    """
    if any(not _is_finite_positive_price(obs.price) for obs in path):
        return None
    if len(path) < 2:
        return PathStats(path_observation_count=len(path), path_available=False)
    prices = [obs.price for obs in path]
    peak = max(prices)
    trough = min(prices)
    peak_obs = next(obs for obs in path if obs.price == peak)
    trough_obs = next(obs for obs in path if obs.price == trough)
    try:
        mfe = (peak / entry_price) - Decimal("1")
        mae = (trough / entry_price) - Decimal("1")
    except DecimalException:
        return None
    if not mfe.is_finite() or not mae.is_finite():
        return None
    return PathStats(
        mfe=mfe,
        mae=mae,
        peak_price=peak,
        trough_price=trough,
        time_to_peak=peak_obs.timestamp - entry_time,
        time_to_trough=trough_obs.timestamp - entry_time,
        path_observation_count=len(path),
        path_available=True,
    )


def _obs_provenance(obs: MarketObservation | None) -> dict[str, str] | None:
    if obs is None or obs.provenance is None:
        return None
    return dict(obs.provenance)


def _result(
    event: TokenListingEvent,
    *,
    entry_delay: timedelta,
    holding_period: timedelta,
    entry_time: datetime,
    exit_time: datetime,
    status: CellOutcomeStatus,
    entry_price: Decimal | None = None,
    exit_price: Decimal | None = None,
    simple_return: Decimal | None = None,
    log_return: Decimal | None = None,
    path: PathStats | None = None,
    entry_obs: MarketObservation | None = None,
    exit_obs: MarketObservation | None = None,
) -> EventStudyCellResult:
    return EventStudyCellResult(
        event_id=event.event_id,
        venue=event.venue,
        token_address=event.token_address,
        chain=event.chain,
        source_event_time=event.source_event_time,
        first_seen_time=event.first_seen_time,
        first_market_data_time=event.first_market_data_time,
        decision_available_time=event.decision_available_time,
        entry_delay=entry_delay,
        holding_period=holding_period,
        entry_time=entry_time,
        exit_time=exit_time,
        status=status,
        entry_price=entry_price,
        exit_price=exit_price,
        simple_return=simple_return,
        log_return=log_return,
        path=path if path is not None else PathStats(),
        event_source=event.source,
        event_provenance=dict(event.provenance),
        entry_source=entry_obs.source if entry_obs is not None else None,
        entry_provenance=_obs_provenance(entry_obs),
        exit_source=exit_obs.source if exit_obs is not None else None,
        exit_provenance=_obs_provenance(exit_obs),
    )


def evaluate_cell(
    event: TokenListingEvent,
    observations: Sequence[MarketObservation],
    *,
    entry_delay: timedelta,
    holding_period: timedelta,
    data_horizon_end: datetime | None = None,
) -> EventStudyCellResult:
    """Evaluate one event × delay × holding cell.

    Observations after ``entry_time`` never affect entry eligibility or entry price.
    """
    entry_time = event.source_event_time + entry_delay
    exit_time = entry_time + holding_period

    if entry_time < event.decision_available_time:
        return _result(
            event,
            entry_delay=entry_delay,
            holding_period=holding_period,
            entry_time=entry_time,
            exit_time=exit_time,
            status=CellOutcomeStatus.NOT_DECISION_AVAILABLE,
        )

    event_obs = prepare_observations(_filter_event_obs(observations, event))
    resolutions = {obs.resolution for obs in event_obs}
    stream_resolution = finest_resolution(resolutions)
    if stream_resolution is None or not supports_entry_delay(stream_resolution, entry_delay):
        return _result(
            event,
            entry_delay=entry_delay,
            holding_period=holding_period,
            entry_time=entry_time,
            exit_time=exit_time,
            status=CellOutcomeStatus.UNSUPPORTED_RESOLUTION,
        )

    # Entry eligibility uses only observations at-or-before entry_time (no lookahead).
    eligible = tuple(obs for obs in event_obs if obs.timestamp <= entry_time)
    if entry_delay < timedelta(minutes=1):
        # Sub-minute pricing may only use point observations.
        eligible = tuple(obs for obs in eligible if obs.resolution is ObservationResolution.POINT)
        if not eligible:
            return _result(
                event,
                entry_delay=entry_delay,
                holding_period=holding_period,
                entry_time=entry_time,
                exit_time=exit_time,
                status=CellOutcomeStatus.UNSUPPORTED_RESOLUTION,
            )

    by_ts = {obs.timestamp: obs for obs in eligible}
    entry_obs = _lookup_at(by_ts, entry_time)
    if entry_obs is None:
        return _result(
            event,
            entry_delay=entry_delay,
            holding_period=holding_period,
            entry_time=entry_time,
            exit_time=exit_time,
            status=CellOutcomeStatus.MISSING_ENTRY,
        )
    if not _is_finite_positive_price(entry_obs.price):
        return _result(
            event,
            entry_delay=entry_delay,
            holding_period=holding_period,
            entry_time=entry_time,
            exit_time=exit_time,
            status=CellOutcomeStatus.INVALID_MARKET_DATA,
            entry_price=entry_obs.price if entry_obs.price.is_finite() else None,
            entry_obs=entry_obs,
        )

    horizon = data_horizon_end
    if horizon is None and event_obs:
        horizon = max(obs.timestamp for obs in event_obs)
    if horizon is not None and exit_time > horizon:
        return _result(
            event,
            entry_delay=entry_delay,
            holding_period=holding_period,
            entry_time=entry_time,
            exit_time=exit_time,
            status=CellOutcomeStatus.RIGHT_CENSORED,
            entry_price=entry_obs.price,
            entry_obs=entry_obs,
        )

    # Exit lookup may use full event series but never forward-fills a missing stamp.
    full_by_ts = {obs.timestamp: obs for obs in event_obs}
    if entry_delay < timedelta(minutes=1):
        full_by_ts = {obs.timestamp: obs for obs in event_obs if obs.resolution is ObservationResolution.POINT}
    exit_obs = _lookup_at(full_by_ts, exit_time)
    if exit_obs is None:
        return _result(
            event,
            entry_delay=entry_delay,
            holding_period=holding_period,
            entry_time=entry_time,
            exit_time=exit_time,
            status=CellOutcomeStatus.MISSING_EXIT,
            entry_price=entry_obs.price,
            entry_obs=entry_obs,
        )
    if not _is_finite_positive_price(exit_obs.price):
        return _result(
            event,
            entry_delay=entry_delay,
            holding_period=holding_period,
            entry_time=entry_time,
            exit_time=exit_time,
            status=CellOutcomeStatus.INVALID_MARKET_DATA,
            entry_price=entry_obs.price,
            exit_price=exit_obs.price if exit_obs.price.is_finite() else None,
            entry_obs=entry_obs,
            exit_obs=exit_obs,
        )

    returns = _compute_returns(entry_obs.price, exit_obs.price)
    if returns is None:
        return _result(
            event,
            entry_delay=entry_delay,
            holding_period=holding_period,
            entry_time=entry_time,
            exit_time=exit_time,
            status=CellOutcomeStatus.INVALID_MARKET_DATA,
            entry_price=entry_obs.price,
            exit_price=exit_obs.price,
            entry_obs=entry_obs,
            exit_obs=exit_obs,
        )
    simple, log_ret = returns
    path = tuple(
        obs
        for obs in event_obs
        if entry_time <= obs.timestamp <= exit_time
        and (entry_delay >= timedelta(minutes=1) or obs.resolution is ObservationResolution.POINT)
    )
    path_stats = _path_stats(path, entry_time=entry_time, entry_price=entry_obs.price)
    if path_stats is None:
        return _result(
            event,
            entry_delay=entry_delay,
            holding_period=holding_period,
            entry_time=entry_time,
            exit_time=exit_time,
            status=CellOutcomeStatus.INVALID_MARKET_DATA,
            entry_price=entry_obs.price,
            exit_price=exit_obs.price,
            entry_obs=entry_obs,
            exit_obs=exit_obs,
        )
    return _result(
        event,
        entry_delay=entry_delay,
        holding_period=holding_period,
        entry_time=entry_time,
        exit_time=exit_time,
        status=CellOutcomeStatus.COMPLETE,
        entry_price=entry_obs.price,
        exit_price=exit_obs.price,
        simple_return=simple,
        log_return=log_ret,
        path=path_stats,
        entry_obs=entry_obs,
        exit_obs=exit_obs,
    )


def run_event_study(
    events: Sequence[TokenListingEvent],
    observations: Sequence[MarketObservation],
    *,
    entry_delays: Sequence[timedelta],
    holding_periods: Sequence[timedelta],
    data_horizon_end: datetime | None = None,
) -> tuple[EventStudyCellResult, ...]:
    """Evaluate the full delay × holding grid per event; venues are never pooled."""
    prepared = prepare_observations(observations)
    # Deterministic event order.
    ordered_events = sorted(
        events,
        key=lambda e: (e.source_event_time, e.venue.value, e.token_address, e.event_id),
    )
    results: list[EventStudyCellResult] = []
    for event in ordered_events:
        for delay in entry_delays:
            for holding in holding_periods:
                results.append(
                    evaluate_cell(
                        event,
                        prepared,
                        entry_delay=delay,
                        holding_period=holding,
                        data_horizon_end=data_horizon_end,
                    )
                )
    return tuple(results)
