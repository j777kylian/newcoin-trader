"""Point-in-time decision features with hard cutoff and no forward-fill."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from decimal import Decimal, DecimalException
from hashlib import sha256

from newcoin_trader.domain.enums import Venue
from newcoin_trader.domain.event_study import ObservationResolution, TokenListingEvent
from newcoin_trader.domain.feature_research import (
    AvailabilityLevel,
    DecisionFeatureRecord,
    FeatureMarketInput,
    FeatureTradeInput,
    FeatureValue,
    FeatureValueState,
    FutureLabel,
)
from newcoin_trader.domain.types import require_utc
from newcoin_trader.errors import ResearchError
from newcoin_trader.research.event_study_config import format_duration
from newcoin_trader.research.feature_research_availability import classify_family


def _input_sort_key(obs: FeatureMarketInput) -> tuple[datetime, str, str, str, str]:
    return (obs.timestamp, obs.venue.value, obs.token_address, obs.source, obs.resolution.value)


def _trade_sort_key(trade: FeatureTradeInput) -> tuple[datetime, str, str, str]:
    return (trade.timestamp, trade.venue.value, trade.token_address, trade.source)


def _dedupe_key(obs: FeatureMarketInput) -> tuple[Venue, str, str, datetime]:
    return (obs.venue, obs.chain, obs.token_address, obs.timestamp)


def _resolution_rank(resolution: ObservationResolution) -> int:
    if resolution is ObservationResolution.POINT:
        return 0
    if resolution is ObservationResolution.MINUTE:
        return 1
    return 2


def prepare_feature_inputs(observations: Sequence[FeatureMarketInput]) -> tuple[FeatureMarketInput, ...]:
    """Sort chronologically and deterministically de-duplicate exact timestamps."""
    ordered = sorted(observations, key=_input_sort_key)
    chosen: dict[tuple[Venue, str, str, datetime], FeatureMarketInput] = {}
    for obs in ordered:
        key = _dedupe_key(obs)
        existing = chosen.get(key)
        if existing is None:
            chosen[key] = obs
            continue
        if (_resolution_rank(obs.resolution), obs.source) < (
            _resolution_rank(existing.resolution),
            existing.source,
        ):
            chosen[key] = obs
    return tuple(sorted(chosen.values(), key=_input_sort_key))


def prepare_trades(trades: Sequence[FeatureTradeInput]) -> tuple[FeatureTradeInput, ...]:
    return tuple(sorted(trades, key=_trade_sort_key))


def _require_finite(value: Decimal, *, context: str) -> Decimal:
    if not value.is_finite():
        raise ResearchError(f"nonfinite decimal in {context}")
    return value


def _filter_event_inputs(
    observations: Sequence[FeatureMarketInput],
    event: TokenListingEvent,
) -> tuple[FeatureMarketInput, ...]:
    return tuple(
        obs
        for obs in observations
        if obs.venue == event.venue and obs.token_address == event.token_address and obs.chain == event.chain.value
    )


def _filter_event_trades(
    trades: Sequence[FeatureTradeInput],
    event: TokenListingEvent,
) -> tuple[FeatureTradeInput, ...]:
    return tuple(
        t
        for t in trades
        if t.venue == event.venue and t.token_address == event.token_address and t.chain == event.chain.value
    )


def _as_of(
    observations: Sequence[FeatureMarketInput],
    cutoff: datetime,
) -> FeatureMarketInput | None:
    eligible = [obs for obs in observations if obs.timestamp <= cutoff]
    if not eligible:
        return None
    return eligible[-1]


def _window_slice(
    observations: Sequence[FeatureMarketInput],
    *,
    start: datetime,
    end: datetime,
) -> tuple[FeatureMarketInput, ...]:
    return tuple(obs for obs in observations if start < obs.timestamp <= end)


def _trade_slice(
    trades: Sequence[FeatureTradeInput],
    *,
    start: datetime,
    end: datetime,
) -> tuple[FeatureTradeInput, ...]:
    return tuple(t for t in trades if start < t.timestamp <= end)


def _fv(
    name: str,
    family: str,
    *,
    state: FeatureValueState,
    value: Decimal | str | None = None,
    source: str | None = None,
    provenance: Mapping[str, str] | None = None,
    window: timedelta | None = None,
) -> FeatureValue:
    return FeatureValue(
        name=name,
        family=family,
        value=value,
        state=state,
        source=source,
        provenance=dict(provenance or {}),
        window=window,
    )


def _decimal_context_invalid(
    name: str,
    family: str,
    *,
    window: timedelta | None = None,
) -> FeatureValue:
    """Arithmetic context failure (e.g. Overflow) → INVALID, never raise DecimalException."""
    return _fv(
        name,
        family,
        state=FeatureValueState.INVALID,
        window=window,
        provenance={"reason": "decimal_context_failure"},
    )


def _compute_ages(event: TokenListingEvent, decision_time: datetime) -> list[FeatureValue]:
    features: list[FeatureValue] = [
        _fv(
            "age_source_event_seconds",
            "age",
            state=FeatureValueState.AVAILABLE,
            value=Decimal(str((decision_time - event.source_event_time).total_seconds())),
            provenance={"clock": "source_event_time"},
        ),
        _fv(
            "age_first_seen_seconds",
            "age",
            state=FeatureValueState.AVAILABLE,
            value=Decimal(str((decision_time - event.first_seen_time).total_seconds())),
            provenance={"clock": "first_seen_time"},
        ),
        _fv(
            "age_decision_available_seconds",
            "age",
            state=FeatureValueState.AVAILABLE,
            value=Decimal(str((decision_time - event.decision_available_time).total_seconds())),
            provenance={"clock": "decision_available_time"},
        ),
    ]
    if event.first_market_data_time is None:
        features.append(
            _fv(
                "age_first_market_seconds",
                "age",
                state=FeatureValueState.MISSING,
                provenance={"clock": "first_market_data_time"},
            )
        )
    else:
        features.append(
            _fv(
                "age_first_market_seconds",
                "age",
                state=FeatureValueState.AVAILABLE,
                value=Decimal(str((decision_time - event.first_market_data_time).total_seconds())),
                provenance={"clock": "first_market_data_time"},
            )
        )
    return features


def _price_return(
    observations: Sequence[FeatureMarketInput],
    *,
    decision_time: datetime,
    window: timedelta,
    family_level: AvailabilityLevel,
) -> FeatureValue:
    name = f"price_return_{format_duration(window)}"
    if family_level is AvailabilityLevel.UNSUPPORTED:
        return _fv(name, "price_momentum", state=FeatureValueState.UNSUPPORTED, window=window)
    current = _as_of(observations, decision_time)
    if current is None:
        return _fv(name, "price_momentum", state=FeatureValueState.MISSING, window=window)
    start_bound = decision_time - window
    start_obs = _as_of(observations, start_bound)
    if start_obs is None:
        return _fv(
            name,
            "price_momentum",
            state=FeatureValueState.INSUFFICIENT,
            window=window,
            provenance={"reason": "no_observation_at_or_before_window_start"},
        )
    # Distinct endpoints required; as-of is PIT-safe and never forward-fills missing stamps.
    if start_obs.timestamp == current.timestamp:
        return _fv(
            name,
            "price_momentum",
            state=FeatureValueState.INSUFFICIENT,
            window=window,
            provenance={"reason": "identical_endpoints"},
        )
    try:
        start_price = _require_finite(start_obs.price, context=name)
        end_price = _require_finite(current.price, context=name)
        if start_price <= 0 or end_price <= 0:
            return _fv(name, "price_momentum", state=FeatureValueState.INVALID, window=window)
        ret = (end_price / start_price) - Decimal("1")
        _require_finite(ret, context=name)
    except DecimalException:
        return _decimal_context_invalid(name, "price_momentum", window=window)
    return _fv(
        name,
        "price_momentum",
        state=FeatureValueState.AVAILABLE,
        value=ret,
        source=current.source,
        provenance={
            "start_ts": start_obs.timestamp.isoformat(),
            "end_ts": current.timestamp.isoformat(),
            "start_source": start_obs.source,
        },
        window=window,
    )


def _volatility(
    observations: Sequence[FeatureMarketInput],
    *,
    decision_time: datetime,
    window: timedelta,
    family_level: AvailabilityLevel,
) -> FeatureValue:
    name = f"volatility_{format_duration(window)}"
    if family_level is AvailabilityLevel.UNSUPPORTED:
        return _fv(name, "volatility", state=FeatureValueState.UNSUPPORTED, window=window)
    sliced = _window_slice(observations, start=decision_time - window, end=decision_time)
    if any(obs.resolution is ObservationResolution.COARSE for obs in sliced) and len(sliced) < 3:
        return _fv(
            name,
            "volatility",
            state=FeatureValueState.UNSUPPORTED,
            window=window,
            provenance={"reason": "coarse_resolution_insufficient"},
        )
    if len(sliced) < 3:
        return _fv(name, "volatility", state=FeatureValueState.INSUFFICIENT, window=window)
    try:
        prices = [_require_finite(obs.price, context=name) for obs in sliced]
        if any(p <= 0 for p in prices):
            return _fv(name, "volatility", state=FeatureValueState.INVALID, window=window)
        rets: list[Decimal] = []
        for i in range(1, len(prices)):
            r = (prices[i] / prices[i - 1]) - Decimal("1")
            _require_finite(r, context=name)
            rets.append(r)
        if len(rets) < 2:
            return _fv(name, "volatility", state=FeatureValueState.INSUFFICIENT, window=window)
        mean = sum(rets, start=Decimal("0")) / Decimal(len(rets))
        var = sum((r - mean) * (r - mean) for r in rets) / Decimal(len(rets))
        std = var.sqrt()
        _require_finite(std, context=name)
    except DecimalException:
        return _decimal_context_invalid(name, "volatility", window=window)
    return _fv(
        name,
        "volatility",
        state=FeatureValueState.AVAILABLE,
        value=std,
        source=sliced[-1].source,
        provenance={"obs_count": str(len(sliced))},
        window=window,
    )


def _range_feature(
    observations: Sequence[FeatureMarketInput],
    *,
    decision_time: datetime,
    window: timedelta,
    family_level: AvailabilityLevel,
) -> FeatureValue:
    name = f"price_range_{format_duration(window)}"
    if family_level is AvailabilityLevel.UNSUPPORTED:
        return _fv(name, "volatility", state=FeatureValueState.UNSUPPORTED, window=window)
    sliced = _window_slice(observations, start=decision_time - window, end=decision_time)
    if len(sliced) < 2:
        return _fv(name, "volatility", state=FeatureValueState.INSUFFICIENT, window=window)
    try:
        prices = [_require_finite(obs.price, context=name) for obs in sliced]
        if any(p <= 0 for p in prices):
            return _fv(name, "volatility", state=FeatureValueState.INVALID, window=window)
        hi = max(prices)
        lo = min(prices)
        rng = (hi - lo) / lo
        _require_finite(rng, context=name)
    except DecimalException:
        return _decimal_context_invalid(name, "volatility", window=window)
    return _fv(
        name,
        "volatility",
        state=FeatureValueState.AVAILABLE,
        value=rng,
        source=sliced[-1].source,
        window=window,
        provenance={"high": str(hi), "low": str(lo)},
    )


def _volume_sum(
    observations: Sequence[FeatureMarketInput],
    *,
    decision_time: datetime,
    window: timedelta,
    family_level: AvailabilityLevel,
) -> FeatureValue:
    name = f"volume_sum_{format_duration(window)}"
    if family_level is AvailabilityLevel.UNSUPPORTED:
        return _fv(name, "volume", state=FeatureValueState.UNSUPPORTED, window=window)
    sliced = _window_slice(observations, start=decision_time - window, end=decision_time)
    if not sliced:
        return _fv(name, "volume", state=FeatureValueState.INSUFFICIENT, window=window)
    volumes = [obs.volume for obs in sliced if obs.volume is not None]
    if not volumes:
        return _fv(name, "volume", state=FeatureValueState.MISSING, window=window)
    try:
        total = sum((_require_finite(v, context=name) for v in volumes), start=Decimal("0"))
        _require_finite(total, context=name)
    except DecimalException as exc:
        raise ResearchError(f"decimal failure in {name}") from exc
    return _fv(
        name,
        "volume",
        state=FeatureValueState.AVAILABLE,
        value=total,
        source=sliced[-1].source,
        window=window,
        provenance={"non_null_count": str(len(volumes))},
    )


def _liquidity_features(
    observations: Sequence[FeatureMarketInput],
    *,
    decision_time: datetime,
    window: timedelta,
    family_level: AvailabilityLevel,
) -> list[FeatureValue]:
    current_name = "liquidity_current"
    change_name = f"liquidity_change_{format_duration(window)}"
    if family_level is AvailabilityLevel.UNSUPPORTED:
        return [
            _fv(current_name, "liquidity", state=FeatureValueState.UNSUPPORTED),
            _fv(change_name, "liquidity", state=FeatureValueState.UNSUPPORTED, window=window),
        ]
    current = _as_of(observations, decision_time)
    if current is None:
        return [
            _fv(current_name, "liquidity", state=FeatureValueState.MISSING),
            _fv(change_name, "liquidity", state=FeatureValueState.INSUFFICIENT, window=window),
        ]
    if current.liquidity is None:
        current_fv = _fv(
            current_name,
            "liquidity",
            state=FeatureValueState.MISSING,
            source=current.source,
        )
    else:
        liq = _require_finite(current.liquidity, context=current_name)
        current_fv = _fv(
            current_name,
            "liquidity",
            state=FeatureValueState.AVAILABLE,
            value=liq,
            source=current.source,
        )
    start_obs = _as_of(observations, decision_time - window)
    if current.liquidity is None:
        change_fv = _fv(change_name, "liquidity", state=FeatureValueState.MISSING, window=window)
    elif start_obs is None or start_obs.liquidity is None or start_obs.timestamp == current.timestamp:
        change_fv = _fv(change_name, "liquidity", state=FeatureValueState.INSUFFICIENT, window=window)
    else:
        try:
            change = _require_finite(current.liquidity, context=change_name) - _require_finite(
                start_obs.liquidity, context=change_name
            )
            _require_finite(change, context=change_name)
            change_fv = _fv(
                change_name,
                "liquidity",
                state=FeatureValueState.AVAILABLE,
                value=change,
                source=current.source,
                window=window,
            )
        except DecimalException as exc:
            raise ResearchError(f"decimal failure in {change_name}") from exc
    return [current_fv, change_fv]


def _activity_features(
    trades: Sequence[FeatureTradeInput],
    *,
    decision_time: datetime,
    window: timedelta,
    family_level: AvailabilityLevel,
) -> list[FeatureValue]:
    count_name = f"trade_count_{format_duration(window)}"
    imb_name = f"buy_sell_imbalance_{format_duration(window)}"
    if family_level is AvailabilityLevel.UNSUPPORTED:
        return [
            _fv(count_name, "activity", state=FeatureValueState.UNSUPPORTED, window=window),
            _fv(imb_name, "buy_sell_imbalance", state=FeatureValueState.UNSUPPORTED, window=window),
        ]
    sliced = _trade_slice(trades, start=decision_time - window, end=decision_time)
    if not sliced:
        return [
            _fv(count_name, "activity", state=FeatureValueState.INSUFFICIENT, window=window),
            _fv(imb_name, "buy_sell_imbalance", state=FeatureValueState.INSUFFICIENT, window=window),
        ]
    buys = sum(1 for t in sliced if t.side.lower() == "buy")
    sells = sum(1 for t in sliced if t.side.lower() == "sell")
    total = buys + sells
    if total == 0:
        imb_state = FeatureValueState.MISSING
        imb_val: Decimal | None = None
    else:
        imb_state = FeatureValueState.AVAILABLE
        imb_val = (Decimal(buys) - Decimal(sells)) / Decimal(total)
    return [
        _fv(
            count_name,
            "activity",
            state=FeatureValueState.AVAILABLE,
            value=Decimal(len(sliced)),
            source=sliced[-1].source,
            window=window,
        ),
        _fv(
            imb_name,
            "buy_sell_imbalance",
            state=imb_state,
            value=imb_val,
            source=sliced[-1].source,
            window=window,
            provenance={"buys": str(buys), "sells": str(sells)},
        ),
    ]


def _computation_id(
    *,
    event_id: str,
    decision_time: datetime,
    windows: Sequence[timedelta],
    config_id: str,
) -> str:
    payload = {
        "event_id": event_id,
        "decision_time": decision_time.isoformat(),
        "windows": [format_duration(w) for w in windows],
        "config_id": config_id,
    }
    digest = sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return digest[:24]


def build_future_labels(raw_labels: Sequence[Mapping[str, object] | FutureLabel]) -> tuple[FutureLabel, ...]:
    labels: list[FutureLabel] = []
    for item in raw_labels:
        if isinstance(item, FutureLabel):
            labels.append(item)
            continue
        labels.append(
            FutureLabel(
                entry_delay=item["entry_delay"],  # type: ignore[arg-type]
                holding_period=item["holding_period"],  # type: ignore[arg-type]
                status=item["status"],  # type: ignore[arg-type]
                simple_return=item.get("simple_return"),  # type: ignore[arg-type]
                log_return=item.get("log_return"),  # type: ignore[arg-type]
                mfe=item.get("mfe"),  # type: ignore[arg-type]
                mae=item.get("mae"),  # type: ignore[arg-type]
                label_source=str(item.get("label_source", "phase3_cell")),
            )
        )
    return tuple(labels)


def build_decision_feature_record(
    event: TokenListingEvent,
    observations: Sequence[FeatureMarketInput],
    *,
    trades: Sequence[FeatureTradeInput] = (),
    decision_time: datetime,
    windows: Sequence[timedelta],
    labels: Sequence[Mapping[str, object] | FutureLabel] = (),
    config_id: str,
) -> DecisionFeatureRecord:
    """Build one decision-level record. Labels never enter feature arithmetic."""
    decision = require_utc(decision_time)
    if decision < event.decision_available_time:
        raise ResearchError("decision_time precedes decision_available_time")
    prepared = prepare_feature_inputs(observations)
    prepared_trades = prepare_trades(trades)

    # Hard invariant: only inputs at-or-before decision_time.
    pit_inputs = tuple(obs for obs in prepared if obs.timestamp <= decision)
    pit_trades = tuple(t for t in prepared_trades if t.timestamp <= decision)

    # Reject nonfinite prices in PIT set early.
    for obs in pit_inputs:
        _require_finite(obs.price, context="feature_input_price")
        if obs.volume is not None:
            _require_finite(obs.volume, context="feature_input_volume")
        if obs.liquidity is not None:
            _require_finite(obs.liquidity, context="feature_input_liquidity")

    event_inputs = _filter_event_inputs(pit_inputs, event)
    event_trades = _filter_event_trades(pit_trades, event)

    features: list[FeatureValue] = []
    features.extend(_compute_ages(event, decision))

    for window in windows:
        features.append(
            _price_return(
                event_inputs,
                decision_time=decision,
                window=window,
                family_level=classify_family(event.venue, "price_momentum"),
            )
        )
        features.append(
            _volatility(
                event_inputs,
                decision_time=decision,
                window=window,
                family_level=classify_family(event.venue, "volatility"),
            )
        )
        features.append(
            _range_feature(
                event_inputs,
                decision_time=decision,
                window=window,
                family_level=classify_family(event.venue, "volatility"),
            )
        )
        features.append(
            _volume_sum(
                event_inputs,
                decision_time=decision,
                window=window,
                family_level=classify_family(event.venue, "volume"),
            )
        )
        features.extend(
            _liquidity_features(
                event_inputs,
                decision_time=decision,
                window=window,
                family_level=classify_family(event.venue, "liquidity"),
            )
        )
        activity_level = classify_family(event.venue, "activity")
        imb_level = classify_family(event.venue, "buy_sell_imbalance")
        # Activity uses the stricter of activity/imbalance unsupported flags per field.
        act = _activity_features(
            event_trades,
            decision_time=decision,
            window=window,
            family_level=activity_level,
        )
        if imb_level is AvailabilityLevel.UNSUPPORTED:
            act = [
                act[0],
                _fv(
                    f"buy_sell_imbalance_{format_duration(window)}",
                    "buy_sell_imbalance",
                    state=FeatureValueState.UNSUPPORTED,
                    window=window,
                ),
            ]
        features.extend(act)

    # Identity once (not per window); data product from last PIT observation.
    last = event_inputs[-1] if event_inputs else None
    features.extend(
        [
            _fv(
                "venue_identity",
                "venue_chain_identity",
                state=FeatureValueState.AVAILABLE,
                value=event.venue.value,
            ),
            _fv(
                "chain_identity",
                "venue_chain_identity",
                state=FeatureValueState.AVAILABLE,
                value=event.chain.value,
            ),
            _fv(
                "data_product_identity",
                "venue_chain_identity",
                state=FeatureValueState.AVAILABLE,
                value=last.source if last is not None else event.source,
            ),
        ]
    )

    # Deduplicate liquidity_current (emitted once per window loop) — keep first AVAILABLE/MISSING.
    deduped: list[FeatureValue] = []
    seen_current = False
    for feat in features:
        if feat.name == "liquidity_current":
            if seen_current:
                continue
            seen_current = True
        deduped.append(feat)

    future_labels = build_future_labels(labels)
    return DecisionFeatureRecord(
        event_id=event.event_id,
        venue=event.venue,
        chain=event.chain,
        token_address=event.token_address,
        pair_address=event.pair_address,
        source_event_time=event.source_event_time,
        first_seen_time=event.first_seen_time,
        first_market_data_time=event.first_market_data_time,
        decision_available_time=event.decision_available_time,
        decision_time=decision,
        feature_cutoff=decision,
        features=tuple(deduped),
        labels=future_labels,
        config_id=config_id,
        computation_id=_computation_id(
            event_id=event.event_id,
            decision_time=decision,
            windows=windows,
            config_id=config_id,
        ),
        event_source=event.source,
        event_provenance=dict(event.provenance),
    )
