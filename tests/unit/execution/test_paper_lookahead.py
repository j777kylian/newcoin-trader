"""Paper broker must reject look-ahead and market identity mismatches."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from newcoin_trader.domain.enums import RejectReason, Side
from newcoin_trader.domain.execution import PaperOrder, RejectedOrder
from newcoin_trader.domain.market import PriceSnapshot
from newcoin_trader.execution.paper_broker import PaperBroker


def _order(*, signal_ts: datetime) -> PaperOrder:
    return PaperOrder(
        token_address="TOKEN",
        chain="solana",
        side=Side.BUY,
        requested_qty=Decimal("1"),
        limit_price=Decimal("1.00"),
        signal_ts=signal_ts,
    )


def _market(
    *,
    timestamp: datetime,
    token_address: str = "TOKEN",
    chain: str = "solana",
    liquidity: str = "100000",
) -> PriceSnapshot:
    return PriceSnapshot(
        token_address=token_address,
        chain=chain,
        timestamp=timestamp,
        price=Decimal("1.00"),
        liquidity=Decimal(liquidity),
        source="test",
    )


def test_rejects_market_snapshot_after_signal_ts() -> None:
    signal_ts = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    future = signal_ts + timedelta(seconds=1)
    broker = PaperBroker(max_fill_liquidity_fraction=Decimal("1"))
    result = broker.fill(_order(signal_ts=signal_ts), market=_market(timestamp=future))
    assert isinstance(result, RejectedOrder)
    assert result.reason is RejectReason.LOOKAHEAD_FORBIDDEN


def test_rejects_token_address_mismatch() -> None:
    signal_ts = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    broker = PaperBroker(max_fill_liquidity_fraction=Decimal("1"))
    result = broker.fill(
        _order(signal_ts=signal_ts),
        market=_market(timestamp=signal_ts, token_address="OTHER"),
    )
    assert isinstance(result, RejectedOrder)
    assert result.reason is RejectReason.MARKET_MISMATCH


def test_rejects_chain_mismatch() -> None:
    signal_ts = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    broker = PaperBroker(max_fill_liquidity_fraction=Decimal("1"))
    result = broker.fill(
        _order(signal_ts=signal_ts),
        market=_market(timestamp=signal_ts, chain="binance"),
    )
    assert isinstance(result, RejectedOrder)
    assert result.reason is RejectReason.MARKET_MISMATCH


def test_accepts_snapshot_at_or_before_signal_ts() -> None:
    signal_ts = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    broker = PaperBroker(
        fee_bps=Decimal("0"),
        slippage_bps=Decimal("0"),
        max_fill_liquidity_fraction=Decimal("1"),
    )
    result = broker.fill(_order(signal_ts=signal_ts), market=_market(timestamp=signal_ts))
    assert not isinstance(result, RejectedOrder)
