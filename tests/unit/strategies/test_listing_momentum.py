"""Deterministic listing-momentum strategy tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from newcoin_trader.domain.enums import SignalKind
from newcoin_trader.domain.market import PriceSnapshot
from newcoin_trader.domain.strategy import StrategyContext
from newcoin_trader.strategies.listing_momentum import ListingMomentumStrategy


def _snap(ts: datetime, price: str) -> PriceSnapshot:
    return PriceSnapshot(
        token_address="TOKEN",
        chain="solana",
        timestamp=ts,
        price=Decimal(price),
        volume=Decimal("100"),
        liquidity=Decimal("10000"),
        source="test",
    )


def test_strategy_is_deterministic_and_ignores_future() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    eval_ts = t0 + timedelta(minutes=5)
    snaps = (
        _snap(t0, "1.00"),
        _snap(eval_ts, "1.20"),
        _snap(t0 + timedelta(hours=1), "5.00"),
    )
    ctx = StrategyContext(
        token_address="TOKEN",
        listing_time=t0,
        evaluation_time=eval_ts,
        snapshots=snaps,
        parameters={"momentum_threshold": "0.05", "qty": "2"},
    )
    strategy = ListingMomentumStrategy()
    a = strategy.generate(ctx)
    b = strategy.generate(ctx)
    assert a == b
    assert len(a) == 1
    assert a[0].kind is SignalKind.BUY
    assert a[0].price == Decimal("1.20")


def test_strategy_no_buy_when_only_future_meets_threshold() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    eval_ts = t0 + timedelta(minutes=5)
    snaps = (
        _snap(t0, "1.00"),
        _snap(eval_ts, "1.01"),
        _snap(t0 + timedelta(hours=1), "2.00"),
    )
    ctx = StrategyContext(
        token_address="TOKEN",
        listing_time=t0,
        evaluation_time=eval_ts,
        snapshots=snaps,
        parameters={"momentum_threshold": "0.50", "qty": "1"},
    )
    signals = ListingMomentumStrategy().generate(ctx)
    assert signals == []
