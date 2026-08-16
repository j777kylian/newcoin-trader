"""Deterministic per-venue univariate stats, chronological split, walk-forward."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from decimal import ROUND_DOWN, Decimal, DecimalException

from newcoin_trader.domain.enums import Venue
from newcoin_trader.domain.event_study import CellOutcomeStatus
from newcoin_trader.domain.feature_research import (
    ChronologicalSplit,
    DecisionFeatureRecord,
    FeatureBinStats,
    FeatureValueState,
    WalkForwardFold,
)
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.event_study_aggregate import _decimal_stats, deterministic_quantile


def chronological_split(
    records: Sequence[DecisionFeatureRecord],
    *,
    ratios: tuple[Decimal, Decimal, Decimal],
) -> ChronologicalSplit:
    ordered = tuple(
        sorted(
            records,
            key=lambda r: (r.decision_time, r.venue.value, r.token_address, r.event_id),
        )
    )
    n = len(ordered)
    if n == 0:
        return ChronologicalSplit(train=(), validation=(), test=(), ratios=ratios)
    train_r, val_r, _test_r = ratios
    if train_r + val_r + _test_r != Decimal("1"):
        raise ConfigError("split ratios must sum to 1")
    # Exact boundary indices via floor on cumulative ratios (deterministic).
    train_end = int((Decimal(n) * train_r).to_integral_value(rounding=ROUND_DOWN))
    val_end = int((Decimal(n) * (train_r + val_r)).to_integral_value(rounding=ROUND_DOWN))
    # Guarantee non-empty remainder assignment when n is small but positive ratios.
    if n >= 3:
        train_end = max(1, min(train_end, n - 2))
        val_end = max(train_end + 1, min(val_end, n - 1))
    elif n == 2:
        train_end = 1
        val_end = 1
    else:
        train_end = 1
        val_end = 1
    return ChronologicalSplit(
        train=ordered[:train_end],
        validation=ordered[train_end:val_end],
        test=ordered[val_end:],
        ratios=ratios,
    )


def walk_forward_folds(
    records: Sequence[DecisionFeatureRecord],
    *,
    n_folds: int,
    min_train: int = 3,
    min_test: int = 2,
) -> tuple[WalkForwardFold, ...]:
    ordered = tuple(
        sorted(
            records,
            key=lambda r: (r.decision_time, r.venue.value, r.token_address, r.event_id),
        )
    )
    n = len(ordered)
    if n_folds < 1 or n < min_train + min_test:
        return ()
    folds: list[WalkForwardFold] = []
    # Rolling expanding train; test is the next contiguous block.
    test_size = max(min_test, n // (n_folds + 1))
    for fold_index in range(n_folds):
        test_start = min_train + fold_index * test_size
        test_end = min(test_start + test_size, n)
        if test_start >= n or test_end - test_start < min_test:
            break
        train = ordered[:test_start]
        test = ordered[test_start:test_end]
        if len(train) < min_train:
            continue
        folds.append(WalkForwardFold(fold_index=fold_index, train=train, test=test))
    return tuple(folds)


def _primary_label(
    record: DecisionFeatureRecord,
) -> tuple[CellOutcomeStatus | None, Decimal | None, Decimal | None, Decimal | None]:
    if not record.labels:
        return None, None, None, None
    label = record.labels[0]
    return label.status, label.simple_return, label.mfe, label.mae


def _feature_numeric(record: DecisionFeatureRecord, feature_name: str) -> Decimal | None:
    for feat in record.features:
        if feat.name != feature_name:
            continue
        if feat.state is not FeatureValueState.AVAILABLE:
            return None
        if isinstance(feat.value, Decimal):
            return feat.value
        if isinstance(feat.value, str):
            try:
                return Decimal(feat.value)
            except DecimalException:
                return None
        return None
    return None


def _bin_label(value: Decimal, edges: Sequence[Decimal]) -> str:
    """Deterministic quantile-edge bins: (-inf,e0], (e0,e1], ..."""
    for idx, edge in enumerate(edges):
        if value <= edge:
            if idx == 0:
                return f"(-inf,{edge}]"
            return f"({edges[idx - 1]},{edge}]"
    return f"({edges[-1]},+inf)" if edges else "all"


def compute_univariate_stats(
    records: Sequence[DecisionFeatureRecord],
    *,
    min_sample: int,
    feature_names: Sequence[str] | None = None,
    n_bins: int = 4,
) -> tuple[FeatureBinStats, ...]:
    by_venue: dict[Venue, list[DecisionFeatureRecord]] = defaultdict(list)
    for record in records:
        by_venue[record.venue].append(record)

    # Discover numeric feature names if not provided.
    names: list[str]
    if feature_names is None:
        discovered: set[str] = set()
        for record in records:
            for feat in record.features:
                if isinstance(feat.value, Decimal) and feat.state is FeatureValueState.AVAILABLE:
                    discovered.add(feat.name)
        names = sorted(discovered)
    else:
        names = list(feature_names)

    results: list[FeatureBinStats] = []
    for venue in sorted(by_venue.keys(), key=lambda v: v.value):
        venue_rows = by_venue[venue]
        for feature_name in names:
            values: list[tuple[DecisionFeatureRecord, Decimal]] = []
            for record in venue_rows:
                num = _feature_numeric(record, feature_name)
                if num is not None:
                    values.append((record, num))
            if len(values) < min_sample:
                results.append(
                    FeatureBinStats(
                        venue=venue,
                        feature_name=feature_name,
                        bin_label="all",
                        samples=len(values),
                        complete_count=0,
                        censored_count=0,
                        valid_return_count=0,
                        insufficient_sample=True,
                    )
                )
                continue
            ordered_vals = sorted(v for _, v in values)
            # Quantile edges for bins (deterministic).
            edges: list[Decimal] = []
            for q in (Decimal("0.25"), Decimal("0.5"), Decimal("0.75")):
                edge = deterministic_quantile(ordered_vals, q)
                if edge is not None and (not edges or edge != edges[-1]):
                    edges.append(edge)
            # Always emit an "all" bucket plus per-bin buckets.
            buckets: dict[str, list[DecisionFeatureRecord]] = defaultdict(list)
            for record, num in values:
                buckets["all"].append(record)
                if edges and n_bins > 1:
                    buckets[_bin_label(num, edges)].append(record)
            for bin_label in sorted(buckets.keys()):
                rows = buckets[bin_label]
                returns: list[Decimal] = []
                mfes: list[Decimal] = []
                maes: list[Decimal] = []
                complete = 0
                censored = 0
                for record in rows:
                    status, simple, mfe, mae = _primary_label(record)
                    if status is CellOutcomeStatus.COMPLETE:
                        complete += 1
                    if status is CellOutcomeStatus.RIGHT_CENSORED:
                        censored += 1
                    if simple is not None and status is CellOutcomeStatus.COMPLETE:
                        returns.append(simple)
                    if mfe is not None:
                        mfes.append(mfe)
                    if mae is not None:
                        maes.append(mae)
                stats = _decimal_stats(returns)
                mfe_stats = _decimal_stats(mfes)
                mae_stats = _decimal_stats(maes)
                results.append(
                    FeatureBinStats(
                        venue=venue,
                        feature_name=feature_name,
                        bin_label=bin_label,
                        samples=len(rows),
                        complete_count=complete,
                        censored_count=censored,
                        valid_return_count=len(returns),
                        mean_simple_return=stats["mean"],
                        median_simple_return=stats["median"],
                        win_rate=stats["win_rate"],
                        p10=stats["p10"],
                        p25=stats["p25"],
                        p75=stats["p75"],
                        p90=stats["p90"],
                        std_simple_return=stats["std"],
                        mean_mfe=mfe_stats["mean"],
                        mean_mae=mae_stats["mean"],
                        insufficient_sample=len(rows) < min_sample,
                    )
                )
    return tuple(results)
