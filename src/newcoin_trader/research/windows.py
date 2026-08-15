"""Listing-relative window helpers. Prevent look-ahead by as-of filtering."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from decimal import Decimal

import pandas as pd

from newcoin_trader.domain.market import PriceSnapshot
from newcoin_trader.errors import ConfigError

DEFAULT_WINDOWS: dict[str, timedelta] = {
    "5m": timedelta(minutes=5),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
}


def parse_window(spec: str) -> timedelta:
    if spec in DEFAULT_WINDOWS:
        return DEFAULT_WINDOWS[spec]
    raise ConfigError(f"unsupported window: {spec}")


def snapshots_to_frame(snapshots: Sequence[PriceSnapshot]) -> pd.DataFrame:
    rows = [
        {
            "timestamp": s.timestamp,
            "price": float(s.price),
            "volume": float(s.volume) if s.volume is not None else float("nan"),
            "liquidity": float(s.liquidity) if s.liquidity is not None else float("nan"),
            "token_address": s.token_address,
        }
        for s in snapshots
    ]
    if not rows:
        return pd.DataFrame(columns=["timestamp", "price", "volume", "liquidity", "token_address"])
    frame = pd.DataFrame(rows)
    return frame.sort_values("timestamp").reset_index(drop=True)


def without_lookahead(
    snapshots: Sequence[PriceSnapshot],
    evaluation_time: datetime,
) -> tuple[PriceSnapshot, ...]:
    return tuple(s for s in snapshots if s.timestamp <= evaluation_time)


def as_of_frame(frame: pd.DataFrame, evaluation_time: datetime) -> pd.DataFrame:
    if frame.empty:
        return frame
    return frame.loc[frame["timestamp"] <= evaluation_time].reset_index(drop=True)


def listing_window_slice(
    frame: pd.DataFrame,
    *,
    listing_time: datetime,
    window: timedelta,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    end = listing_time + window
    mask = (frame["timestamp"] >= listing_time) & (frame["timestamp"] <= end)
    return frame.loc[mask].reset_index(drop=True)


def since_listing_frame(frame: pd.DataFrame, listing_time: datetime) -> pd.DataFrame:
    if frame.empty:
        return frame
    return frame.loc[frame["timestamp"] >= listing_time].reset_index(drop=True)


def resolve_windows(window_specs: Mapping[str, timedelta] | None) -> dict[str, timedelta]:
    return dict(window_specs) if window_specs is not None else dict(DEFAULT_WINDOWS)


def decimal_or_none(value: float | None) -> Decimal | None:
    if value is None or value != value:  # NaN
        return None
    return Decimal(str(value))
