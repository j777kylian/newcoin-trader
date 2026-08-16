"""Pure Phase 5 historical execution simulator (no I/O, no PaperBroker, no orders)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal, DecimalException

from newcoin_trader.domain.enums import Side, Venue
from newcoin_trader.domain.event_study import ObservationResolution, TokenListingEvent
from newcoin_trader.domain.executable_backtest import (
    WARNING_MODELED,
    DepthLevel,
    ExecutableBacktestStatus,
    ExecutableTradeResult,
    ExecutionConfidence,
    ExecutionMarketObservation,
    ExecutionTradeTick,
    FrozenCandidateIdentity,
    HistoricalDepthBook,
    SimulatedFill,
    SimulatedFillMode,
)
from newcoin_trader.domain.feature_research import CandidateRule, DecisionFeatureRecord
from newcoin_trader.research.event_study_resolution import finest_resolution, supports_entry_delay
from newcoin_trader.research.executable_backtest_config import (
    DEFAULT_IMPACT_COEFFICIENT,
    FEE_BPS_MAX,
    FEE_BPS_MIN,
    MAX_PARTICIPATION_MAX,
    MAX_PARTICIPATION_MIN,
)
from newcoin_trader.research.feature_research_rules import evaluate_rule


def _finite_positive(value: Decimal) -> bool:
    return value.is_finite() and value > 0


def _finite_nonnegative(value: Decimal) -> bool:
    return value.is_finite() and value >= 0


def _finite_in_closed_range(value: Decimal, *, lo: Decimal, hi: Decimal) -> bool:
    return value.is_finite() and lo <= value <= hi


def _safe_fee(notional: Decimal, fee_bps: Decimal) -> Decimal | None:
    try:
        fee = notional * fee_bps / Decimal("10000")
    except DecimalException:
        return None
    if not fee.is_finite() or fee < 0:
        return None
    return fee


def _mid_from_book(book: HistoricalDepthBook) -> Decimal | None:
    best_bid = max((lvl.price for lvl in book.bids), default=None)
    best_ask = min((lvl.price for lvl in book.asks), default=None)
    if best_bid is None or best_ask is None:
        return None
    if not _finite_positive(best_bid) or not _finite_positive(best_ask):
        return None
    try:
        mid = (best_bid + best_ask) / Decimal("2")
    except DecimalException:
        return None
    return mid if mid.is_finite() else None


def _walk_levels(
    levels: Sequence[DepthLevel],
    *,
    requested_qty: Decimal,
) -> tuple[Decimal, Decimal, Decimal] | None:
    remaining = requested_qty
    filled = Decimal("0")
    notional = Decimal("0")
    try:
        for level in levels:
            if not _finite_positive(level.price) or not _finite_positive(level.quantity):
                return None
            take = min(remaining, level.quantity)
            notional += take * level.price
            filled += take
            remaining -= take
            if remaining <= 0:
                break
        if filled <= 0:
            return Decimal("0"), Decimal("0"), Decimal("0")
        vwap = notional / filled
        if not vwap.is_finite():
            return None
        return filled, notional, vwap
    except DecimalException:
        return None


def simulate_cex_depth_fill(
    *,
    book: HistoricalDepthBook,
    side: Side,
    requested_qty: Decimal,
    assumed_fee_bps: Decimal,
    request_time: datetime | None = None,
    fill_time: datetime | None = None,
) -> SimulatedFill | None:
    """Walk asks for buys / bids for sells against supplied historical L2 only."""
    if not requested_qty.is_finite() or requested_qty <= 0:
        return None
    ts = fill_time or book.timestamp
    req_ts = request_time or book.timestamp
    if side is Side.BUY:
        levels = tuple(sorted(book.asks, key=lambda lvl: (lvl.price, lvl.quantity)))
    else:
        levels = tuple(sorted(book.bids, key=lambda lvl: (-lvl.price, lvl.quantity)))

    if not levels:
        return SimulatedFill(
            side=side,
            status=ExecutableBacktestStatus.UNFILLED,
            mode=SimulatedFillMode.EXACT_DEPTH,
            confidence=ExecutionConfidence.EXACT_DEPTH,
            request_time=req_ts,
            fill_time=ts,
            requested_qty=requested_qty,
            fill_qty=Decimal("0"),
            fill_price=Decimal("0"),
            notional=Decimal("0"),
            fee_cost=Decimal("0"),
            spread_cost=Decimal("0"),
            slippage_cost=Decimal("0"),
            impact_cost=Decimal("0"),
            assumed_fee_bps=assumed_fee_bps,
            label="exact_depth_unfilled",
            source=book.source,
        )

    walked = _walk_levels(levels, requested_qty=requested_qty)
    if walked is None:
        return None
    filled, notional, vwap = walked
    if filled <= 0:
        status = ExecutableBacktestStatus.UNFILLED
    elif filled < requested_qty:
        status = ExecutableBacktestStatus.PARTIAL
    else:
        status = ExecutableBacktestStatus.FULLY_FILLED

    mid = _mid_from_book(book)
    top = levels[0].price
    try:
        half_spread = abs(top - mid) if mid is not None else Decimal("0")
        spread_cost = half_spread * filled if half_spread.is_finite() else Decimal("0")
        impact_per_unit = abs(vwap - top) if top.is_finite() else Decimal("0")
        impact_cost = impact_per_unit * filled if impact_per_unit.is_finite() else Decimal("0")
        slippage_cost = abs(vwap - (mid if mid is not None else top)) * filled
    except DecimalException:
        return None
    if not all(c.is_finite() for c in (spread_cost, impact_cost, slippage_cost)):
        return None
    fee = _safe_fee(notional, assumed_fee_bps)
    if fee is None:
        return None
    return SimulatedFill(
        side=side,
        status=status,
        mode=SimulatedFillMode.EXACT_DEPTH,
        confidence=ExecutionConfidence.EXACT_DEPTH,
        request_time=req_ts,
        fill_time=ts,
        requested_qty=requested_qty,
        fill_qty=filled,
        fill_price=vwap if filled > 0 else Decimal("0"),
        notional=notional,
        fee_cost=fee,
        spread_cost=spread_cost,
        slippage_cost=slippage_cost,
        impact_cost=impact_cost,
        assumed_fee_bps=assumed_fee_bps,
        label="exact_depth_walk",
        source=book.source,
    )


def simulate_modeled_price_fill(
    *,
    observation: ExecutionMarketObservation,
    side: Side,
    position_notional: Decimal,
    max_participation: Decimal,
    assumed_fee_bps: Decimal,
    request_time: datetime | None = None,
    fill_time: datetime | None = None,
) -> SimulatedFill | None:
    """Modeled CEX/price fallback using timestamped price (+ optional liquidity cap)."""
    if not _finite_positive(observation.price):
        return None
    if not position_notional.is_finite() or position_notional <= 0:
        return None
    ts = fill_time or observation.timestamp
    req_ts = request_time or observation.timestamp
    liquidity = observation.liquidity
    capped_notional = position_notional
    status = ExecutableBacktestStatus.FULLY_FILLED
    if liquidity is not None:
        if not liquidity.is_finite() or liquidity < 0:
            return None
        try:
            max_notional = liquidity * max_participation
        except DecimalException:
            return None
        if not max_notional.is_finite():
            return None
        if max_notional <= 0:
            return SimulatedFill(
                side=side,
                status=ExecutableBacktestStatus.INSUFFICIENT_LIQUIDITY,
                mode=SimulatedFillMode.MODELED_PRICE,
                confidence=ExecutionConfidence.MODELED_PRICE,
                request_time=req_ts,
                fill_time=ts,
                requested_qty=Decimal("0"),
                fill_qty=Decimal("0"),
                fill_price=observation.price,
                notional=Decimal("0"),
                fee_cost=Decimal("0"),
                spread_cost=Decimal("0"),
                slippage_cost=Decimal("0"),
                impact_cost=Decimal("0"),
                assumed_fee_bps=assumed_fee_bps,
                label=WARNING_MODELED,
                source=observation.source,
            )
        if position_notional > max_notional:
            capped_notional = max_notional
            status = ExecutableBacktestStatus.PARTIAL
    try:
        qty = capped_notional / observation.price
        requested_qty = position_notional / observation.price
    except DecimalException:
        return None
    if not qty.is_finite() or qty <= 0 or not requested_qty.is_finite():
        return None
    fee = _safe_fee(capped_notional, assumed_fee_bps)
    if fee is None:
        return None
    return SimulatedFill(
        side=side,
        status=status,
        mode=SimulatedFillMode.MODELED_PRICE,
        confidence=ExecutionConfidence.MODELED_PRICE,
        request_time=req_ts,
        fill_time=ts,
        requested_qty=requested_qty,
        fill_qty=qty,
        fill_price=observation.price,
        notional=capped_notional,
        fee_cost=fee,
        spread_cost=Decimal("0"),
        slippage_cost=Decimal("0"),
        impact_cost=Decimal("0"),
        assumed_fee_bps=assumed_fee_bps,
        label=WARNING_MODELED,
        source=observation.source,
    )


def simulate_dex_liquidity_fill(
    *,
    observation: ExecutionMarketObservation,
    side: Side,
    position_notional: Decimal,
    max_participation: Decimal,
    assumed_fee_bps: Decimal,
    impact_coefficient: Decimal = DEFAULT_IMPACT_COEFFICIENT,
    request_time: datetime | None = None,
    fill_time: datetime | None = None,
) -> SimulatedFill | None:
    """Deterministic liquidity-participation impact; modeled, never AMM-exact."""
    if not _finite_positive(position_notional):
        return None
    if not _finite_in_closed_range(
        max_participation,
        lo=MAX_PARTICIPATION_MIN,
        hi=MAX_PARTICIPATION_MAX,
    ):
        return None
    if not _finite_in_closed_range(assumed_fee_bps, lo=FEE_BPS_MIN, hi=FEE_BPS_MAX):
        return None
    if not _finite_nonnegative(impact_coefficient):
        return None
    if not _finite_positive(observation.price):
        return None
    ts = fill_time or observation.timestamp
    req_ts = request_time or observation.timestamp
    liquidity = observation.liquidity
    if liquidity is None or not liquidity.is_finite() or liquidity <= 0:
        return SimulatedFill(
            side=side,
            status=ExecutableBacktestStatus.INSUFFICIENT_LIQUIDITY,
            mode=SimulatedFillMode.MODELED_LIQUIDITY,
            confidence=ExecutionConfidence.MODELED_LIQUIDITY_IMPACT,
            request_time=req_ts,
            fill_time=ts,
            requested_qty=Decimal("0"),
            fill_qty=Decimal("0"),
            fill_price=observation.price,
            notional=Decimal("0"),
            fee_cost=Decimal("0"),
            spread_cost=Decimal("0"),
            slippage_cost=Decimal("0"),
            impact_cost=Decimal("0"),
            assumed_fee_bps=assumed_fee_bps,
            label="modeled_liquidity_impact_not_reserve_exact",
            source=observation.source,
        )
    try:
        max_notional = liquidity * max_participation
    except DecimalException:
        return None
    if not max_notional.is_finite() or max_notional <= 0:
        return None
    status = ExecutableBacktestStatus.FULLY_FILLED
    capped = position_notional
    if position_notional > max_notional:
        capped = max_notional
        status = ExecutableBacktestStatus.PARTIAL
    try:
        participation = capped / liquidity
        impact_frac = impact_coefficient * participation
        if side is Side.BUY:
            fill_price = observation.price * (Decimal("1") + impact_frac)
        else:
            fill_price = observation.price * (Decimal("1") - impact_frac)
        qty = capped / fill_price
        impact_cost = abs(fill_price - observation.price) * qty
        requested_qty = position_notional / observation.price
    except DecimalException:
        return None
    if not all(v.is_finite() for v in (fill_price, qty, impact_cost, requested_qty)):
        return None
    if fill_price <= 0 or qty <= 0:
        return None
    fee = _safe_fee(capped, assumed_fee_bps)
    if fee is None:
        return None
    return SimulatedFill(
        side=side,
        status=status,
        mode=SimulatedFillMode.MODELED_LIQUIDITY,
        confidence=ExecutionConfidence.MODELED_LIQUIDITY_IMPACT,
        request_time=req_ts,
        fill_time=ts,
        requested_qty=requested_qty,
        fill_qty=qty,
        fill_price=fill_price,
        notional=capped,
        fee_cost=fee,
        spread_cost=Decimal("0"),
        slippage_cost=Decimal("0"),
        impact_cost=impact_cost,
        assumed_fee_bps=assumed_fee_bps,
        label="modeled_liquidity_impact_not_reserve_exact",
        source=observation.source,
    )


def _filter_obs(
    observations: Sequence[ExecutionMarketObservation],
    *,
    venue: Venue,
    token: str,
    chain: str,
) -> tuple[ExecutionMarketObservation, ...]:
    return tuple(
        obs for obs in observations if obs.venue == venue and obs.token_address == token and obs.chain == chain
    )


def _obs_at_or_before(
    observations: Sequence[ExecutionMarketObservation],
    timestamp: datetime,
    *,
    require_point: bool,
    after: datetime | None = None,
) -> ExecutionMarketObservation | None:
    eligible = [
        obs
        for obs in observations
        if obs.timestamp <= timestamp
        and (after is None or obs.timestamp > after)
        and (not require_point or obs.resolution is ObservationResolution.POINT)
    ]
    if not eligible:
        return None
    return max(eligible, key=lambda o: (o.timestamp, o.source, o.resolution.value))


def _obs_exact(
    observations: Sequence[ExecutionMarketObservation],
    timestamp: datetime,
    *,
    require_point: bool,
) -> ExecutionMarketObservation | None:
    matches = [
        obs
        for obs in observations
        if obs.timestamp == timestamp and (not require_point or obs.resolution is ObservationResolution.POINT)
    ]
    if not matches:
        return None
    return sorted(matches, key=lambda o: (o.source, o.resolution.value))[0]


def _trade_exact(
    trades: Sequence[ExecutionTradeTick],
    timestamp: datetime,
    *,
    venue: Venue,
    token: str,
) -> ExecutionTradeTick | None:
    matches = [t for t in trades if t.timestamp == timestamp and t.venue == venue and t.token_address == token]
    if not matches:
        return None
    return sorted(matches, key=lambda t: (t.source, t.price, t.amount))[0]


def _depth_exact(
    books: Sequence[HistoricalDepthBook],
    timestamp: datetime,
    *,
    venue: Venue,
    token: str,
) -> HistoricalDepthBook | None:
    matches = [b for b in books if b.timestamp == timestamp and b.venue == venue and b.token_address == token]
    if not matches:
        return None
    return sorted(matches, key=lambda b: b.source)[0]


def _rule_matches(identity: FrozenCandidateIdentity, record: DecisionFeatureRecord) -> bool:
    rule = CandidateRule(
        rule_id=identity.rule_id,
        conditions=identity.conditions,
        human_readable=identity.human_readable,
        selected=True,
    )
    return len(evaluate_rule(rule, (record,))) == 1


def _phase4_gross(record: DecisionFeatureRecord) -> Decimal | None:
    for label in record.labels:
        if label.simple_return is not None and label.simple_return.is_finite():
            return label.simple_return
    return None


def _fill_leg(
    *,
    venue: Venue,
    side: Side,
    position_notional: Decimal,
    max_participation: Decimal,
    assumed_fee_bps: Decimal,
    impact_coefficient: Decimal,
    request_time: datetime,
    fill_time: datetime,
    observations: Sequence[ExecutionMarketObservation],
    trades: Sequence[ExecutionTradeTick],
    depth_books: Sequence[HistoricalDepthBook],
    token: str,
    chain: str,
    require_point: bool,
    requested_qty: Decimal | None = None,
    missing_status: ExecutableBacktestStatus = ExecutableBacktestStatus.NO_ENTRY,
    observation_after: datetime | None = None,
) -> tuple[SimulatedFill | None, ExecutableBacktestStatus | None]:
    book = _depth_exact(depth_books, fill_time, venue=venue, token=token)
    if book is not None:
        qty = requested_qty
        if qty is None:
            ref = _mid_from_book(book)
            if ref is None:
                levels = book.asks if side is Side.BUY else book.bids
                ref = levels[0].price if levels else None
            if ref is None or not _finite_positive(ref):
                return None, ExecutableBacktestStatus.UNFILLED
            try:
                qty = position_notional / ref
            except DecimalException:
                return None, ExecutableBacktestStatus.INVALID_MARKET_DATA
        fill = simulate_cex_depth_fill(
            book=book,
            side=side,
            requested_qty=qty,
            assumed_fee_bps=assumed_fee_bps,
            request_time=request_time,
            fill_time=fill_time,
        )
        if fill is None:
            return None, ExecutableBacktestStatus.INVALID_MARKET_DATA
        return fill, None

    tick = _trade_exact(trades, fill_time, venue=venue, token=token)
    if tick is not None:
        if not _finite_positive(tick.price):
            return None, ExecutableBacktestStatus.INVALID_MARKET_DATA
        try:
            qty = requested_qty if requested_qty is not None else position_notional / tick.price
            notional = qty * tick.price
        except DecimalException:
            return None, ExecutableBacktestStatus.INVALID_MARKET_DATA
        if not qty.is_finite() or not notional.is_finite():
            return None, ExecutableBacktestStatus.INVALID_MARKET_DATA
        fee = _safe_fee(notional, assumed_fee_bps)
        if fee is None:
            return None, ExecutableBacktestStatus.INVALID_MARKET_DATA
        return (
            SimulatedFill(
                side=side,
                status=ExecutableBacktestStatus.FULLY_FILLED,
                mode=SimulatedFillMode.EXACT_TRADE,
                confidence=ExecutionConfidence.EXACT_TRADE,
                request_time=request_time,
                fill_time=fill_time,
                requested_qty=qty,
                fill_qty=qty,
                fill_price=tick.price,
                notional=notional,
                fee_cost=fee,
                spread_cost=Decimal("0"),
                slippage_cost=Decimal("0"),
                impact_cost=Decimal("0"),
                assumed_fee_bps=assumed_fee_bps,
                label="exact_trade_at_timestamp",
                source=tick.source,
            ),
            None,
        )

    obs = _obs_exact(observations, fill_time, require_point=require_point)
    if obs is not None and observation_after is not None and obs.timestamp <= observation_after:
        obs = None
    if obs is None:
        obs = _obs_at_or_before(
            observations,
            fill_time,
            require_point=require_point,
            after=observation_after,
        )
    if obs is None:
        return None, missing_status
    if not _finite_positive(obs.price):
        return None, ExecutableBacktestStatus.INVALID_MARKET_DATA

    notional_for_fill = position_notional
    if requested_qty is not None:
        try:
            notional_for_fill = requested_qty * obs.price
        except DecimalException:
            return None, ExecutableBacktestStatus.INVALID_MARKET_DATA
        if not notional_for_fill.is_finite():
            return None, ExecutableBacktestStatus.INVALID_MARKET_DATA

    if venue in {Venue.RAYDIUM, Venue.GECKO}:
        fill = simulate_dex_liquidity_fill(
            observation=obs,
            side=side,
            position_notional=notional_for_fill,
            max_participation=max_participation,
            assumed_fee_bps=assumed_fee_bps,
            impact_coefficient=impact_coefficient,
            request_time=request_time,
            fill_time=fill_time,
        )
    else:
        fill = simulate_modeled_price_fill(
            observation=obs,
            side=side,
            position_notional=notional_for_fill,
            max_participation=max_participation,
            assumed_fee_bps=assumed_fee_bps,
            request_time=request_time,
            fill_time=fill_time,
        )
    if fill is None:
        return None, ExecutableBacktestStatus.INVALID_MARKET_DATA
    return fill, None


def _compute_returns(
    entry: SimulatedFill,
    exit_: SimulatedFill,
) -> tuple[Decimal, Decimal] | None:
    if entry.fill_qty <= 0 or exit_.fill_qty <= 0:
        return None
    if not _finite_positive(entry.fill_price) or not _finite_positive(exit_.fill_price):
        return None
    try:
        gross = (exit_.fill_price / entry.fill_price) - Decimal("1")
        entry_cost_frac = (
            entry.fee_cost + entry.spread_cost + entry.slippage_cost + entry.impact_cost
        ) / entry.notional
        exit_cost_frac = (exit_.fee_cost + exit_.spread_cost + exit_.slippage_cost + exit_.impact_cost) / exit_.notional
        net = gross - entry_cost_frac - exit_cost_frac
    except DecimalException:
        return None
    if not gross.is_finite() or not net.is_finite():
        return None
    return gross, net


def edge_retention_pair(gross: Decimal, net: Decimal) -> tuple[Decimal | None, str]:
    if not gross.is_finite() or not net.is_finite():
        return None, "invalid"
    if gross > 0:
        try:
            ratio = net / gross
        except DecimalException:
            return None, "invalid"
        if not ratio.is_finite():
            return None, "invalid"
        return ratio, "positive_gross"
    if gross == 0:
        return None, "zero_gross"
    return None, "negative_gross"


def evaluate_executable_trade(
    *,
    event: TokenListingEvent,
    record: DecisionFeatureRecord,
    identity: FrozenCandidateIdentity,
    observations: Sequence[ExecutionMarketObservation],
    side: Side = Side.BUY,
    position_notional: Decimal,
    holding_period: timedelta,
    entry_request_latency: timedelta,
    entry_fill_latency: timedelta,
    exit_request_latency: timedelta,
    exit_fill_latency: timedelta,
    assumed_fee_bps: Decimal,
    max_participation: Decimal,
    depth_books: Sequence[HistoricalDepthBook] = (),
    trades: Sequence[ExecutionTradeTick] = (),
    impact_coefficient: Decimal = DEFAULT_IMPACT_COEFFICIENT,
) -> ExecutableTradeResult:
    """Simulate one entry/exit under frozen Phase 4 identity and venue execution rules."""
    configured_decision = record.decision_time
    signal_time = configured_decision
    request_time = signal_time + entry_request_latency
    fill_time = request_time + entry_fill_latency
    exit_signal_time = fill_time + holding_period
    exit_request_time = exit_signal_time + exit_request_latency
    exit_fill_time = exit_request_time + exit_fill_latency
    phase4_gross = _phase4_gross(record)

    def _base(
        status: ExecutableBacktestStatus,
        *,
        entry_fill: SimulatedFill | None = None,
        exit_fill: SimulatedFill | None = None,
        gross: Decimal | None = None,
        net: Decimal | None = None,
        confidence: ExecutionConfidence | None = None,
    ) -> ExecutableTradeResult:
        fee = spread = slip = impact = None
        if entry_fill is not None:
            fee = entry_fill.fee_cost
            spread = entry_fill.spread_cost
            slip = entry_fill.slippage_cost
            impact = entry_fill.impact_cost
        if exit_fill is not None:
            fee = (fee or Decimal("0")) + exit_fill.fee_cost
            spread = (spread or Decimal("0")) + exit_fill.spread_cost
            slip = (slip or Decimal("0")) + exit_fill.slippage_cost
            impact = (impact or Decimal("0")) + exit_fill.impact_cost
        retention = None
        semantics = None
        if gross is not None and net is not None:
            retention, semantics = edge_retention_pair(gross, net)
        return ExecutableTradeResult(
            event_id=event.event_id,
            venue=event.venue,
            token_address=event.token_address,
            chain=event.chain.value,
            frozen_rule_id=identity.rule_id,
            phase4_config_id=identity.phase4_config_id,
            split_label=identity.split_label,
            fold_index=identity.fold_index,
            source_event_time=event.source_event_time,
            first_seen_time=event.first_seen_time,
            decision_available_time=event.decision_available_time,
            configured_decision_time=configured_decision,
            signal_time=signal_time,
            request_time=request_time,
            fill_time=fill_time,
            exit_signal_time=exit_signal_time,
            exit_request_time=exit_request_time,
            exit_fill_time=exit_fill_time,
            status=status,
            side=side,
            position_notional=position_notional,
            holding_period=holding_period,
            entry_fill=entry_fill,
            exit_fill=exit_fill,
            phase4_gross_return=phase4_gross,
            gross_return=gross,
            net_return=net,
            total_fee_cost=fee,
            total_spread_cost=spread,
            total_slippage_cost=slip,
            total_impact_cost=impact,
            edge_retention=retention,
            edge_retention_semantics=semantics,
            confidence=confidence
            or (entry_fill.confidence if entry_fill is not None else ExecutionConfidence.UNSUPPORTED),
        )

    if fill_time < event.decision_available_time or signal_time < event.decision_available_time:
        return _base(ExecutableBacktestStatus.NOT_DECISION_AVAILABLE)
    if fill_time < signal_time or request_time < signal_time:
        return _base(ExecutableBacktestStatus.INVALID_MARKET_DATA)

    if not _rule_matches(identity, record):
        return _base(ExecutableBacktestStatus.RULE_NOT_MATCHED)

    event_obs = _filter_obs(
        observations,
        venue=event.venue,
        token=event.token_address,
        chain=event.chain.value,
    )
    total_entry_latency = entry_request_latency + entry_fill_latency
    require_point = total_entry_latency < timedelta(minutes=1)
    if require_point:
        stream_resolution = finest_resolution({obs.resolution for obs in event_obs})
        point_obs = [o for o in event_obs if o.resolution is ObservationResolution.POINT]
        if (
            not point_obs
            or stream_resolution is None
            or not supports_entry_delay(stream_resolution, total_entry_latency)
        ):
            # Minute-only streams cannot support sub-minute latency.
            if not point_obs:
                return _base(ExecutableBacktestStatus.UNSUPPORTED_RESOLUTION)

    entry_fill, hard = _fill_leg(
        venue=event.venue,
        side=side,
        position_notional=position_notional,
        max_participation=max_participation,
        assumed_fee_bps=assumed_fee_bps,
        impact_coefficient=impact_coefficient,
        request_time=request_time,
        fill_time=fill_time,
        observations=event_obs,
        trades=trades,
        depth_books=depth_books,
        token=event.token_address,
        chain=event.chain.value,
        require_point=require_point,
        missing_status=ExecutableBacktestStatus.NO_ENTRY,
    )
    if hard is not None and entry_fill is None:
        return _base(hard)
    if entry_fill is None:
        return _base(ExecutableBacktestStatus.NO_ENTRY)
    if entry_fill.status in {
        ExecutableBacktestStatus.UNFILLED,
        ExecutableBacktestStatus.INSUFFICIENT_LIQUIDITY,
        ExecutableBacktestStatus.INVALID_MARKET_DATA,
    }:
        return _base(entry_fill.status, entry_fill=entry_fill, confidence=entry_fill.confidence)
    if entry_fill.fill_qty <= 0:
        return _base(ExecutableBacktestStatus.UNFILLED, entry_fill=entry_fill, confidence=entry_fill.confidence)

    exit_side = Side.SELL if side is Side.BUY else Side.BUY
    exit_fill, exit_hard = _fill_leg(
        venue=event.venue,
        side=exit_side,
        position_notional=entry_fill.notional,
        max_participation=max_participation,
        assumed_fee_bps=assumed_fee_bps,
        impact_coefficient=impact_coefficient,
        request_time=exit_request_time,
        fill_time=exit_fill_time,
        observations=event_obs,
        trades=trades,
        depth_books=depth_books,
        token=event.token_address,
        chain=event.chain.value,
        require_point=require_point,
        requested_qty=entry_fill.fill_qty,
        missing_status=ExecutableBacktestStatus.NO_EXIT,
        observation_after=fill_time,
    )
    if exit_hard is not None and exit_fill is None:
        return _base(exit_hard, entry_fill=entry_fill, confidence=entry_fill.confidence)
    if exit_fill is None:
        return _base(
            ExecutableBacktestStatus.NO_EXIT,
            entry_fill=entry_fill,
            confidence=entry_fill.confidence,
        )
    if (
        exit_fill.status
        in {
            ExecutableBacktestStatus.UNFILLED,
            ExecutableBacktestStatus.INSUFFICIENT_LIQUIDITY,
        }
        or exit_fill.fill_qty <= 0
    ):
        return _base(
            ExecutableBacktestStatus.NO_EXIT,
            entry_fill=entry_fill,
            exit_fill=exit_fill,
            confidence=entry_fill.confidence,
        )

    returns = _compute_returns(entry_fill, exit_fill)
    if returns is None:
        return _base(
            ExecutableBacktestStatus.INVALID_MARKET_DATA,
            entry_fill=entry_fill,
            exit_fill=exit_fill,
            confidence=entry_fill.confidence,
        )
    gross, net = returns
    status = ExecutableBacktestStatus.FULLY_FILLED
    if entry_fill.status is ExecutableBacktestStatus.PARTIAL or exit_fill.status is ExecutableBacktestStatus.PARTIAL:
        status = ExecutableBacktestStatus.PARTIAL
    return _base(
        status,
        entry_fill=entry_fill,
        exit_fill=exit_fill,
        gross=gross,
        net=net,
        confidence=entry_fill.confidence,
    )


def run_executable_backtest(
    *,
    events: Sequence[TokenListingEvent],
    records: Sequence[DecisionFeatureRecord],
    identities: Sequence[FrozenCandidateIdentity],
    observations: Sequence[ExecutionMarketObservation],
    latencies: Sequence[timedelta],
    holding_periods: Sequence[timedelta],
    position_notionals: Sequence[Decimal],
    assumed_fee_bps: Decimal,
    max_participation: Decimal,
    depth_books: Sequence[HistoricalDepthBook] = (),
    trades: Sequence[ExecutionTradeTick] = (),
) -> tuple[ExecutableTradeResult, ...]:
    by_event = {r.event_id: r for r in records}
    ordered_events = sorted(
        events,
        key=lambda e: (e.source_event_time, e.venue.value, e.token_address, e.event_id),
    )
    ordered_ids = sorted(identities, key=lambda i: (i.rule_id, i.split_label, i.fold_index or -1))
    results: list[ExecutableTradeResult] = []
    for event in ordered_events:
        record = by_event.get(event.event_id)
        if record is None:
            continue
        for identity in ordered_ids:
            for latency in latencies:
                for holding in holding_periods:
                    for notional in position_notionals:
                        results.append(
                            evaluate_executable_trade(
                                event=event,
                                record=record,
                                identity=identity,
                                observations=observations,
                                position_notional=notional,
                                holding_period=holding,
                                entry_request_latency=timedelta(0),
                                entry_fill_latency=latency,
                                exit_request_latency=timedelta(0),
                                exit_fill_latency=timedelta(0),
                                assumed_fee_bps=assumed_fee_bps,
                                max_participation=max_participation,
                                depth_books=depth_books,
                                trades=trades,
                            )
                        )
    return tuple(results)
