"""Pre-trade risk checks with explicit reject reasons."""

from __future__ import annotations

from dataclasses import dataclass

from newcoin_trader.domain.enums import RejectReason, Side
from newcoin_trader.domain.execution import PaperOrder, PortfolioState
from newcoin_trader.risk.limits import RiskLimits


@dataclass(frozen=True)
class RiskDecision:
    accepted: bool
    reason: RejectReason | None = None
    detail: str | None = None


def evaluate(order: PaperOrder, portfolio: PortfolioState, limits: RiskLimits) -> RiskDecision:
    order_notional = order.requested_qty * order.limit_price
    if order.requested_qty <= 0 or order.limit_price <= 0:
        return RiskDecision(False, RejectReason.INVALID_ORDER, "qty/price must be positive")

    if order.side is Side.SELL:
        if order_notional > portfolio.position_size:
            return RiskDecision(
                False,
                RejectReason.SELL_EXCEEDS_POSITION,
                "sell notional exceeds current position size",
            )
        if portfolio.observed_liquidity < limits.min_liquidity:
            return RiskDecision(
                False,
                RejectReason.INSUFFICIENT_LIQUIDITY,
                "observed liquidity below minimum",
            )
        # Risk-reducing sells bypass max-open and max-drawdown gates.
        return RiskDecision(True)

    resulting_position = portfolio.position_size + order_notional
    if resulting_position > limits.max_position_size:
        return RiskDecision(False, RejectReason.MAX_POSITION_SIZE, "resulting position exceeds max")
    if portfolio.gross_notional + order_notional > limits.max_notional:
        return RiskDecision(False, RejectReason.MAX_NOTIONAL, "portfolio notional limit")
    if portfolio.open_positions >= limits.max_open_positions:
        return RiskDecision(False, RejectReason.MAX_OPEN_POSITIONS, "too many open positions")
    if portfolio.drawdown > limits.max_drawdown:
        return RiskDecision(False, RejectReason.MAX_DRAWDOWN, "drawdown limit breached")
    if portfolio.observed_liquidity < limits.min_liquidity:
        return RiskDecision(
            False,
            RejectReason.INSUFFICIENT_LIQUIDITY,
            "observed liquidity below minimum",
        )
    return RiskDecision(True)
