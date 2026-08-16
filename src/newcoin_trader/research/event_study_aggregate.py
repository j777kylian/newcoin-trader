"""Deterministic venue × delay × holding aggregation for Phase 3."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from datetime import timedelta
from decimal import Decimal, DecimalException

from newcoin_trader.domain.enums import Venue
from newcoin_trader.domain.event_study import (
    CellAggregate,
    CellOutcomeStatus,
    EventStudyCellResult,
)
from newcoin_trader.errors import ResearchError
from newcoin_trader.research.event_study_config import format_duration

# Type-7 (R/Hyndman-Fan) quantile: linear interpolation on sorted ascending sample.
_QUANTILE_METHOD = "type-7"


def deterministic_quantile(sorted_values: Sequence[Decimal], q: Decimal | float | int) -> Decimal | None:
    """Type-7 style quantile on a pre-sorted ascending Decimal sequence (deterministic).

    Retains Hyndman-Fan type-7 / R default linear interpolation: position
    ``(n - 1) * q`` with weights between adjacent order statistics.
    """
    if not sorted_values:
        return None
    q_dec = q if isinstance(q, Decimal) else Decimal(str(q))
    try:
        if q_dec <= 0:
            return sorted_values[0]
        if q_dec >= 1:
            return sorted_values[-1]
        n = len(sorted_values)
        if n == 1:
            return sorted_values[0]
        pos = (Decimal(n - 1)) * q_dec
        low = int(pos)
        high = min(low + 1, n - 1)
        weight = pos - Decimal(low)
        return sorted_values[low] * (Decimal("1") - weight) + sorted_values[high] * weight
    except DecimalException as exc:
        raise ResearchError(f"decimal aggregation overflow in {_QUANTILE_METHOD} quantile") from exc


def _population_std(values: Sequence[Decimal], mean: Decimal) -> Decimal:
    n = len(values)
    if n < 2:
        return Decimal("0")
    acc = Decimal("0")
    for value in values:
        delta = value - mean
        acc += delta * delta
    variance = acc / Decimal(n)
    return variance.sqrt()


def _decimal_stats(values: Sequence[Decimal]) -> dict[str, Decimal | None]:
    if not values:
        return {
            "mean": None,
            "median": None,
            "std": None,
            "p10": None,
            "p25": None,
            "p75": None,
            "p90": None,
            "min": None,
            "max": None,
            "win_rate": None,
        }
    try:
        for sample in values:
            if not sample.is_finite():
                raise ResearchError("aggregate input return is not finite")
        ordered = sorted(values)
        n = Decimal(len(values))
        total = sum(values, start=Decimal("0"))
        mean = total / n
        wins = sum(1 for v in values if v > 0)
        std = _population_std(values, mean)
        median = deterministic_quantile(ordered, Decimal("0.5"))
        if median is None:
            raise ResearchError("median undefined for nonempty aggregate sample")
        stats = {
            "mean": mean,
            "median": median,
            "std": std,
            "p10": deterministic_quantile(ordered, Decimal("0.10")),
            "p25": deterministic_quantile(ordered, Decimal("0.25")),
            "p75": deterministic_quantile(ordered, Decimal("0.75")),
            "p90": deterministic_quantile(ordered, Decimal("0.90")),
            "min": ordered[0],
            "max": ordered[-1],
            "win_rate": Decimal(wins) / n,
        }
        for key, value in stats.items():
            if value is not None and not value.is_finite():
                raise ResearchError(f"aggregate {key} is not finite")
        return stats
    except DecimalException as exc:
        raise ResearchError("decimal aggregation overflow or invalid operation") from exc


def aggregate_results(results: Sequence[EventStudyCellResult]) -> tuple[CellAggregate, ...]:
    buckets: dict[tuple[Venue, timedelta, timedelta], list[EventStudyCellResult]] = defaultdict(list)
    for row in results:
        buckets[(row.venue, row.entry_delay, row.holding_period)].append(row)

    aggregates: list[CellAggregate] = []
    for venue, delay, holding in sorted(
        buckets.keys(),
        key=lambda k: (k[0].value, k[1].total_seconds(), k[2].total_seconds()),
    ):
        rows = buckets[(venue, delay, holding)]
        status_counts: dict[str, int] = defaultdict(int)
        returns: list[Decimal] = []
        mfes: list[Decimal] = []
        maes: list[Decimal] = []
        complete = 0
        censored = 0
        for row in rows:
            status_counts[row.status.value] += 1
            if row.status is CellOutcomeStatus.COMPLETE:
                complete += 1
            if row.status is CellOutcomeStatus.RIGHT_CENSORED:
                censored += 1
            if row.simple_return is not None and row.status is CellOutcomeStatus.COMPLETE:
                returns.append(row.simple_return)
            if row.path.path_available and row.path.mfe is not None:
                mfes.append(row.path.mfe)
            if row.path.path_available and row.path.mae is not None:
                maes.append(row.path.mae)

        stats = _decimal_stats(returns)
        mfe_stats = _decimal_stats(mfes)
        mae_stats = _decimal_stats(maes)
        aggregates.append(
            CellAggregate(
                venue=venue,
                entry_delay=delay,
                holding_period=holding,
                samples=len(rows),
                complete_count=complete,
                valid_return_count=len(returns),
                censored_count=censored,
                status_counts=dict(sorted(status_counts.items())),
                mean_simple_return=stats["mean"],
                median_simple_return=stats["median"],
                std_simple_return=stats["std"],
                win_rate=stats["win_rate"],
                p10=stats["p10"],
                p25=stats["p25"],
                p75=stats["p75"],
                p90=stats["p90"],
                min_simple_return=stats["min"],
                max_simple_return=stats["max"],
                mean_mfe=mfe_stats["mean"],
                mean_mae=mae_stats["mean"],
                median_mfe=mfe_stats["median"],
                median_mae=mae_stats["median"],
                mfe_available_count=len(mfes),
                mae_available_count=len(maes),
            )
        )
    return tuple(aggregates)


def aggregate_row_dict(agg: CellAggregate) -> dict[str, object]:
    return {
        "venue": agg.venue.value,
        "entry_delay": format_duration(agg.entry_delay),
        "holding_period": format_duration(agg.holding_period),
        "samples": agg.samples,
        "complete_count": agg.complete_count,
        "valid_return_count": agg.valid_return_count,
        "censored_count": agg.censored_count,
        "status_counts": agg.status_counts,
        "mean_simple_return": agg.mean_simple_return,
        "median_simple_return": agg.median_simple_return,
        "std_simple_return": agg.std_simple_return,
        "win_rate": agg.win_rate,
        "p10": agg.p10,
        "p25": agg.p25,
        "p75": agg.p75,
        "p90": agg.p90,
        "min_simple_return": agg.min_simple_return,
        "max_simple_return": agg.max_simple_return,
        "mean_mfe": agg.mean_mfe,
        "mean_mae": agg.mean_mae,
        "median_mfe": agg.median_mfe,
        "median_mae": agg.median_mae,
        "mfe_available_count": agg.mfe_available_count,
        "mae_available_count": agg.mae_available_count,
        "label": agg.label,
        "warning": agg.warning,
    }
