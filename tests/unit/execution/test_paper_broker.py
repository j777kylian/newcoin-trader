"""Paper broker fee/slippage/liquidity behavior."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from newcoin_trader.domain.enums import PaperStatus, RejectReason, Side
from newcoin_trader.domain.execution import PaperOrder
from newcoin_trader.domain.market import PriceSnapshot
from newcoin_trader.execution.paper_broker import PaperBroker


def _order(qty: str = "10", price: str = "1.00", *, side: Side = Side.BUY) -> PaperOrder:
    return PaperOrder(
        token_address="TOKEN",
        chain="solana",
        side=side,
        requested_qty=Decimal(qty),
        limit_price=Decimal(price),
        signal_ts=datetime(2024, 1, 1, tzinfo=UTC),
    )


def _market(liquidity: str, *, price: str = "1.00") -> PriceSnapshot:
    return PriceSnapshot(
        token_address="TOKEN",
        chain="solana",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        price=Decimal(price),
        liquidity=Decimal(liquidity),
        source="test",
    )


def test_paper_fill_applies_fee_and_slippage_from_market_price() -> None:
    broker = PaperBroker(
        fee_bps=Decimal("10"),
        slippage_bps=Decimal("100"),
        max_fill_liquidity_fraction=Decimal("1"),
    )
    # market 1.00 + 100bps = 1.01; limit 2 allows fill
    result = broker.fill(_order("1", "2"), market=_market("100000"))
    assert result.status is PaperStatus.FILLED  # type: ignore[union-attr]
    assert result.fill_price == Decimal("1.01")  # type: ignore[union-attr]
    assert result.fee == Decimal("0.00101")  # type: ignore[union-attr]


def test_partial_fill_when_liquidity_capped() -> None:
    broker = PaperBroker(
        fee_bps=Decimal("0"),
        slippage_bps=Decimal("0"),
        max_fill_liquidity_fraction=Decimal("0.1"),
    )
    # liquidity 100, fraction 0.1 => max notional 10 => max qty 10 at price 1
    result = broker.fill(_order("50", "1"), market=_market("100"))
    assert result.status is PaperStatus.PARTIAL  # type: ignore[union-attr]
    assert result.fill_qty == Decimal("10")  # type: ignore[union-attr]


def test_reject_when_no_liquidity() -> None:
    broker = PaperBroker(slippage_bps=Decimal("0"))
    result = broker.fill(_order(), market=_market("0"))
    assert result.reason is RejectReason.INSUFFICIENT_LIQUIDITY  # type: ignore[union-attr]
