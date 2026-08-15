"""Research metrics, windows, and no-look-ahead guarantees."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from newcoin_trader.domain.market import PriceSnapshot
from newcoin_trader.research.metrics import max_drawdown, simple_return, volatility
from newcoin_trader.research.pipeline import analyze_listing
from newcoin_trader.research.windows import snapshots_to_frame, without_lookahead


def _snap(ts: datetime, price: str, volume: str = "100", liquidity: str = "1000") -> PriceSnapshot:
    return PriceSnapshot(
        token_address="TOKEN",
        chain="solana",
        timestamp=ts,
        price=Decimal(price),
        volume=Decimal(volume),
        liquidity=Decimal(liquidity),
        source="test",
    )


def test_simple_return_volatility_and_drawdown() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    snaps = [
        _snap(t0, "100"),
        _snap(t0 + timedelta(minutes=5), "110"),
        _snap(t0 + timedelta(hours=1), "105"),
        _snap(t0 + timedelta(hours=4), "120"),
    ]
    frame = snapshots_to_frame(snaps)
    assert simple_return(frame["price"]) == Decimal("0.2")
    assert volatility(frame["price"]) is not None
    assert max_drawdown(frame["price"]) is not None
    assert max_drawdown(frame["price"]) < 0


def test_listing_relative_windows() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    snaps = [
        _snap(t0, "1.00"),
        _snap(t0 + timedelta(minutes=5), "1.10"),
        _snap(t0 + timedelta(hours=1), "1.05"),
        _snap(t0 + timedelta(hours=4), "1.20"),
        _snap(t0 + timedelta(days=1), "0.90"),
    ]
    analysis = analyze_listing(snaps, listing_time=t0, token_address="TOKEN")
    by_name = {w.window: w for w in analysis.windows}
    assert by_name["5m"].simple_return == Decimal("0.1")
    assert by_name["1h"].simple_return == Decimal("0.05")
    assert by_name["4h"].simple_return == Decimal("0.2")
    assert analysis.candidates
    assert all(c.label == "research_output_not_trading_advice" for c in analysis.candidates)


def test_no_lookahead_in_analysis() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    eval_ts = t0 + timedelta(minutes=5)
    snaps = [
        _snap(t0, "1.00"),
        _snap(eval_ts, "1.05"),
        _snap(t0 + timedelta(hours=4), "10.00"),  # future spike must be ignored
    ]
    analysis = analyze_listing(
        snaps,
        listing_time=t0,
        token_address="TOKEN",
        evaluation_time=eval_ts,
    )
    by_name = {w.window: w for w in analysis.windows}
    assert by_name["4h"].simple_return == Decimal("0.05")
    assert by_name["4h"].n_observations == 2


def test_without_lookahead_filters_future_bars() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    snaps = (_snap(t0, "1"), _snap(t0 + timedelta(hours=1), "2"))
    visible = without_lookahead(snaps, t0)
    assert len(visible) == 1
    assert visible[0].price == Decimal("1")
