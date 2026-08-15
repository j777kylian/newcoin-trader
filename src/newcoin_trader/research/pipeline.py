"""Research analysis orchestration (pure, no I/O)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta

from newcoin_trader.domain.market import PriceSnapshot
from newcoin_trader.domain.research import ListingAnalysis, WindowStats
from newcoin_trader.research.features import infer_candidate_windows
from newcoin_trader.research.metrics import compute_window_metrics
from newcoin_trader.research.windows import (
    as_of_frame,
    listing_window_slice,
    resolve_windows,
    since_listing_frame,
    snapshots_to_frame,
    without_lookahead,
)


def analyze_listing(
    snapshots: Sequence[PriceSnapshot],
    *,
    listing_time: datetime,
    token_address: str,
    evaluation_time: datetime | None = None,
    windows: Mapping[str, timedelta] | None = None,
) -> ListingAnalysis:
    visible = without_lookahead(snapshots, evaluation_time) if evaluation_time is not None else tuple(snapshots)
    frame = snapshots_to_frame(visible)
    if evaluation_time is not None:
        frame = as_of_frame(frame, evaluation_time)
    frame = since_listing_frame(frame, listing_time)
    window_map = resolve_windows(windows)
    stats: list[WindowStats] = []
    for name, delta in window_map.items():
        sliced = listing_window_slice(frame, listing_time=listing_time, window=delta)
        metrics = compute_window_metrics(sliced)
        n_observations = metrics["n_observations"]
        if not isinstance(n_observations, int):
            n_observations = int(n_observations or 0)
        stats.append(
            WindowStats(
                window=name,
                window_delta=delta,
                simple_return=metrics["simple_return"],  # type: ignore[arg-type]
                volatility=metrics["volatility"],  # type: ignore[arg-type]
                max_drawdown=metrics["max_drawdown"],  # type: ignore[arg-type]
                mean_liquidity=metrics["mean_liquidity"],  # type: ignore[arg-type]
                mean_volume=metrics["mean_volume"],  # type: ignore[arg-type]
                n_observations=n_observations,
            )
        )
    candidates = infer_candidate_windows(frame, listing_time=listing_time)
    return ListingAnalysis(
        token_address=token_address,
        listing_time=listing_time,
        windows=tuple(stats),
        candidates=candidates,
    )
