"""Phase 5 executable-backtest aggregates: gross/net, costs, coverage, edge retention."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from decimal import Decimal, DecimalException
from typing import Any

from newcoin_trader.domain.executable_backtest import (
    ExecutableBacktestStatus,
    ExecutableTradeResult,
)
from newcoin_trader.research.executable_backtest_engine import edge_retention_pair


def edge_retention(*, gross: Decimal, net: Decimal) -> Decimal | dict[str, str]:
    """Public helper: ratio for positive gross; controlled semantics otherwise."""
    ratio, semantics = edge_retention_pair(gross, net)
    if semantics == "positive_gross" and ratio is not None:
        return ratio
    return {"semantics": semantics}


def _mean(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    try:
        total = sum(values, start=Decimal("0"))
        mean = total / Decimal(len(values))
    except DecimalException:
        return None
    return mean if mean.is_finite() else None


def _segment(trades: Sequence[ExecutableTradeResult]) -> dict[str, Any]:
    completed = [
        t
        for t in trades
        if t.status in {ExecutableBacktestStatus.FULLY_FILLED, ExecutableBacktestStatus.PARTIAL}
        and t.gross_return is not None
        and t.net_return is not None
    ]
    grosses = [t.gross_return for t in completed if t.gross_return is not None]
    nets = [t.net_return for t in completed if t.net_return is not None]
    phase4 = [t.phase4_gross_return for t in trades if t.phase4_gross_return is not None]
    fees = [t.total_fee_cost for t in trades if t.total_fee_cost is not None]
    spreads = [t.total_spread_cost for t in trades if t.total_spread_cost is not None]
    slips = [t.total_slippage_cost for t in trades if t.total_slippage_cost is not None]
    impacts = [t.total_impact_cost for t in trades if t.total_impact_cost is not None]
    filled = sum(
        1 for t in trades if t.status in {ExecutableBacktestStatus.FULLY_FILLED, ExecutableBacktestStatus.PARTIAL}
    )
    attempted = len(trades)
    coverage = None
    if attempted > 0:
        try:
            coverage = Decimal(filled) / Decimal(attempted)
        except DecimalException:
            coverage = None
        if coverage is not None and not coverage.is_finite():
            coverage = None

    retentions: list[Decimal] = []
    zero_gross = 0
    negative_gross = 0
    positive_gross = 0
    for t in completed:
        assert t.gross_return is not None and t.net_return is not None
        ratio, semantics = edge_retention_pair(t.gross_return, t.net_return)
        if semantics == "positive_gross":
            positive_gross += 1
            if ratio is not None:
                retentions.append(ratio)
        elif semantics == "zero_gross":
            zero_gross += 1
        elif semantics == "negative_gross":
            negative_gross += 1

    status_counts: dict[str, int] = defaultdict(int)
    for t in trades:
        status_counts[t.status.value] += 1

    return {
        "samples": attempted,
        "completed_count": len(completed),
        "fill_coverage": coverage,
        "mean_phase4_gross_return": _mean(phase4),
        "mean_gross_return": _mean(grosses),
        "mean_net_return": _mean(nets),
        "mean_fee_cost": _mean(fees),
        "mean_spread_cost": _mean(spreads),
        "mean_slippage_cost": _mean(slips),
        "mean_impact_cost": _mean(impacts),
        "mean_edge_retention": _mean(retentions),
        "edge_retention_positive_gross_count": positive_gross,
        "edge_retention_zero_gross_count": zero_gross,
        "edge_retention_negative_gross_count": negative_gross,
        "status_counts": dict(sorted(status_counts.items())),
    }


def aggregate_trades(trades: Sequence[ExecutableTradeResult]) -> dict[str, Any]:
    overall = _segment(trades)
    by_venue: dict[str, dict[str, Any]] = {}
    by_rule: dict[str, dict[str, Any]] = {}
    by_fold: dict[str, dict[str, Any]] = {}
    by_confidence: dict[str, dict[str, Any]] = {}

    venue_groups: dict[str, list[ExecutableTradeResult]] = defaultdict(list)
    rule_groups: dict[str, list[ExecutableTradeResult]] = defaultdict(list)
    fold_groups: dict[str, list[ExecutableTradeResult]] = defaultdict(list)
    conf_groups: dict[str, list[ExecutableTradeResult]] = defaultdict(list)
    for trade in trades:
        venue_groups[trade.venue.value].append(trade)
        rule_groups[trade.frozen_rule_id].append(trade)
        fold_key = str(trade.fold_index) if trade.fold_index is not None else "none"
        fold_groups[fold_key].append(trade)
        conf_key = trade.confidence.value if trade.confidence is not None else "unknown"
        conf_groups[conf_key].append(trade)

    for key, group in sorted(venue_groups.items()):
        by_venue[key] = _segment(group)
    for key, group in sorted(rule_groups.items()):
        by_rule[key] = _segment(group)
    for key, group in sorted(fold_groups.items()):
        by_fold[key] = _segment(group)
    for key, group in sorted(conf_groups.items()):
        by_confidence[key] = _segment(group)

    return {
        "phase4_gross": {"mean_phase4_gross_return": overall["mean_phase4_gross_return"]},
        "mean_phase4_gross_return": overall["mean_phase4_gross_return"],
        "mean_gross_return": overall["mean_gross_return"],
        "mean_net_return": overall["mean_net_return"],
        "fill_coverage": overall["fill_coverage"],
        "mean_fee_cost": overall["mean_fee_cost"],
        "mean_spread_cost": overall["mean_spread_cost"],
        "mean_slippage_cost": overall["mean_slippage_cost"],
        "mean_impact_cost": overall["mean_impact_cost"],
        "mean_edge_retention": overall["mean_edge_retention"],
        "edge_retention_positive_gross_count": overall["edge_retention_positive_gross_count"],
        "edge_retention_zero_gross_count": overall["edge_retention_zero_gross_count"],
        "edge_retention_negative_gross_count": overall["edge_retention_negative_gross_count"],
        "status_counts": overall["status_counts"],
        "by_venue": by_venue,
        "by_rule": by_rule,
        "by_fold": by_fold,
        "by_confidence": by_confidence,
    }
