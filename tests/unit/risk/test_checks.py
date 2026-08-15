"""Risk reject reason coverage."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from newcoin_trader.domain.enums import RejectReason, Side
from newcoin_trader.domain.execution import PaperOrder, PortfolioState
from newcoin_trader.risk.checks import evaluate
from newcoin_trader.risk.limits import RiskLimits


def _order(qty: str = "1", price: str = "10") -> PaperOrder:
    return PaperOrder(
        token_address="TOKEN",
        chain="solana",
        side=Side.BUY,
        requested_qty=Decimal(qty),
        limit_price=Decimal(price),
        signal_ts=datetime(2024, 1, 1, tzinfo=UTC),
    )


def _portfolio(**overrides: object) -> PortfolioState:
    base = {
        "open_positions": 0,
        "gross_notional": Decimal("0"),
        "position_size": Decimal("0"),
        "drawdown": Decimal("0"),
        "observed_liquidity": Decimal("10000"),
    }
    base.update(overrides)
    return PortfolioState(**base)  # type: ignore[arg-type]


def test_risk_rejects() -> None:
    limits = RiskLimits(
        max_notional=Decimal("100"),
        max_position_size=Decimal("50"),
        max_open_positions=1,
        max_drawdown=Decimal("0.1"),
        min_liquidity=Decimal("5000"),
    )
    assert evaluate(_order("10", "10"), _portfolio(), limits).reason is RejectReason.MAX_POSITION_SIZE
    assert (
        evaluate(_order("1", "10"), _portfolio(gross_notional=Decimal("95")), limits).reason
        is RejectReason.MAX_NOTIONAL
    )
    assert evaluate(_order("1", "10"), _portfolio(open_positions=1), limits).reason is RejectReason.MAX_OPEN_POSITIONS
    assert evaluate(_order("1", "10"), _portfolio(drawdown=Decimal("0.2")), limits).reason is RejectReason.MAX_DRAWDOWN
    assert (
        evaluate(_order("1", "10"), _portfolio(observed_liquidity=Decimal("100")), limits).reason
        is RejectReason.INSUFFICIENT_LIQUIDITY
    )


def test_risk_accepts_valid_order() -> None:
    decision = evaluate(_order("1", "10"), _portfolio(), RiskLimits())
    assert decision.accepted
    assert decision.reason is None
