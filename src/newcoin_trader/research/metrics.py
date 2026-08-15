"""Pure listing-relative market metrics."""

from __future__ import annotations

from decimal import Decimal

import pandas as pd

from newcoin_trader.research.windows import decimal_or_none


def simple_return(prices: pd.Series) -> Decimal | None:
    if len(prices) < 2:
        return None
    first = Decimal(str(prices.iloc[0]))
    last = Decimal(str(prices.iloc[-1]))
    if first == 0:
        return None
    return (last - first) / first


def volatility(prices: pd.Series) -> Decimal | None:
    if len(prices) < 3:
        return None
    rets = prices.pct_change().dropna()
    if rets.empty:
        return None
    return decimal_or_none(float(rets.std(ddof=1)))


def max_drawdown(prices: pd.Series) -> Decimal | None:
    if len(prices) < 2:
        return None
    cummax = prices.cummax()
    drawdowns = (prices - cummax) / cummax
    return decimal_or_none(float(drawdowns.min()))


def mean_series(values: pd.Series) -> Decimal | None:
    clean = values.dropna()
    if clean.empty:
        return None
    return decimal_or_none(float(clean.mean()))


def compute_window_metrics(frame: pd.DataFrame) -> dict[str, Decimal | None | int]:
    if frame.empty:
        return {
            "simple_return": None,
            "volatility": None,
            "max_drawdown": None,
            "mean_liquidity": None,
            "mean_volume": None,
            "n_observations": 0,
        }
    prices = frame["price"]
    return {
        "simple_return": simple_return(prices),
        "volatility": volatility(prices),
        "max_drawdown": max_drawdown(prices),
        "mean_liquidity": mean_series(frame["liquidity"]),
        "mean_volume": mean_series(frame["volume"]),
        "n_observations": int(len(frame)),
    }
