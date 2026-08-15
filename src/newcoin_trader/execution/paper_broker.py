"""Deterministic paper broker: market-price fills with limit enforcement."""

from __future__ import annotations

from decimal import Decimal

from newcoin_trader.domain.enums import PaperStatus, RejectReason, Side
from newcoin_trader.domain.execution import PaperFill, PaperOrder, RejectedOrder
from newcoin_trader.domain.market import PriceSnapshot
from newcoin_trader.domain.numeric import require_finite_decimal
from newcoin_trader.errors import ConfigError


class PaperBroker:
    def __init__(
        self,
        *,
        fee_bps: Decimal = Decimal("10"),
        slippage_bps: Decimal = Decimal("25"),
        max_fill_liquidity_fraction: Decimal = Decimal("0.10"),
    ) -> None:
        fee_bps = require_finite_decimal(fee_bps, name="fee_bps")
        slippage_bps = require_finite_decimal(slippage_bps, name="slippage_bps")
        max_fill_liquidity_fraction = require_finite_decimal(
            max_fill_liquidity_fraction,
            name="max_fill_liquidity_fraction",
        )
        if fee_bps < 0:
            raise ConfigError("fee_bps must be >= 0")
        if slippage_bps < 0:
            raise ConfigError("slippage_bps must be >= 0")
        if max_fill_liquidity_fraction <= 0 or max_fill_liquidity_fraction > 1:
            raise ConfigError("max_fill_liquidity_fraction must be in (0, 1]")
        self._fee_bps = fee_bps
        self._slippage_bps = slippage_bps
        self._max_fill_liquidity_fraction = max_fill_liquidity_fraction

    def fill(
        self,
        order: PaperOrder,
        *,
        market: PriceSnapshot | None = None,
    ) -> PaperFill | RejectedOrder:
        if market is None:
            return RejectedOrder(
                order=order,
                reason=RejectReason.INSUFFICIENT_LIQUIDITY,
                detail="market snapshot required for paper fill",
            )
        if market.timestamp > order.signal_ts:
            return RejectedOrder(
                order=order,
                reason=RejectReason.LOOKAHEAD_FORBIDDEN,
                detail="market snapshot timestamp is after order.signal_ts",
            )
        if market.token_address != order.token_address or market.chain != order.chain:
            return RejectedOrder(
                order=order,
                reason=RejectReason.MARKET_MISMATCH,
                detail="market token_address/chain does not match order",
            )

        slip = self._slippage_bps / Decimal("10000")
        if order.side is Side.BUY:
            fill_price = market.price * (Decimal("1") + slip)
            if fill_price > order.limit_price:
                return RejectedOrder(
                    order=order,
                    reason=RejectReason.LIMIT_NOT_MET,
                    detail="buy fill after slippage exceeds limit price",
                )
        else:
            fill_price = market.price * (Decimal("1") - slip)
            if fill_price < order.limit_price:
                return RejectedOrder(
                    order=order,
                    reason=RejectReason.LIMIT_NOT_MET,
                    detail="sell fill after slippage is below limit price",
                )
        if fill_price <= 0:
            return RejectedOrder(order=order, reason=RejectReason.INVALID_ORDER, detail="bad fill price")

        liquidity = market.liquidity if market.liquidity is not None else Decimal("0")
        max_notional = liquidity * self._max_fill_liquidity_fraction
        max_qty = max_notional / fill_price if fill_price else Decimal("0")

        if max_qty <= 0:
            return RejectedOrder(
                order=order,
                reason=RejectReason.INSUFFICIENT_LIQUIDITY,
                detail="no observable liquidity for paper fill",
            )

        fill_qty = min(order.requested_qty, max_qty)
        status = PaperStatus.FILLED if fill_qty >= order.requested_qty else PaperStatus.PARTIAL
        fee = fill_qty * fill_price * self._fee_bps / Decimal("10000")
        return PaperFill(
            order=order,
            status=status,
            fill_qty=fill_qty,
            fill_price=fill_price,
            fee=fee,
            slippage_bps=self._slippage_bps,
            mode="paper",
        )
