"""Deterministic candidate entry/exit windows. Research output only."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pandas as pd

from newcoin_trader.domain.research import CandidateWindow
from newcoin_trader.research.metrics import simple_return
from newcoin_trader.research.windows import since_listing_frame


def _row_timestamp(frame: pd.DataFrame, index: int) -> datetime:
    value = frame.loc[index, "timestamp"]
    if isinstance(value, datetime):
        return value
    ts = pd.Timestamp(str(value))
    result = ts.to_pydatetime()
    if isinstance(result, datetime):
        return result
    raise TypeError(f"unsupported timestamp: {value!r}")


def infer_candidate_windows(
    frame: pd.DataFrame,
    *,
    listing_time: datetime,
) -> tuple[CandidateWindow, ...]:
    """Infer candidate windows from historical returns.

    This is research output, not trading advice.
    Pre-listing rows are excluded before cumulative-return calculations.
    """
    work = since_listing_frame(frame, listing_time)
    if work.empty or len(work) < 2:
        return ()
    work = work.sort_values("timestamp").reset_index(drop=True)
    prices = work["price"]
    cum_returns = prices / prices.iloc[0] - 1.0
    peak_idx = int(cum_returns.idxmax())
    peak_ts = _row_timestamp(work, peak_idx)
    peak_value = Decimal(str(cum_returns.iloc[peak_idx]))

    entry: CandidateWindow | None = None
    for idx, value in enumerate(cum_returns):
        if float(value) > 0:
            entry = CandidateWindow(
                kind="entry",
                start=listing_time,
                end=_row_timestamp(work, idx),
                metric="first_positive_cumulative_return",
                value=Decimal(str(value)),
            )
            break

    exit_window = CandidateWindow(
        kind="exit",
        start=peak_ts,
        end=peak_ts,
        metric="max_cumulative_return",
        value=peak_value,
    )
    overall = simple_return(prices)
    candidates: list[CandidateWindow] = []
    if entry is not None:
        candidates.append(entry)
    candidates.append(exit_window)
    if overall is not None:
        candidates.append(
            CandidateWindow(
                kind="holding",
                start=listing_time,
                end=_row_timestamp(work, len(work) - 1),
                metric="full_sample_simple_return",
                value=overall,
            )
        )
    return tuple(candidates)
