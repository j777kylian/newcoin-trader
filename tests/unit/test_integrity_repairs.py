"""Failing-first coverage for adversarial integrity repairs."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError
from sqlalchemy.dialects import postgresql

from newcoin_trader.database.models import PaperTrade, StrategyResult, Trade
from newcoin_trader.database.repositories.market import trade_upsert_statement
from newcoin_trader.database.repositories.paper import paper_trade_upsert_statement
from newcoin_trader.database.repositories.strategy import strategy_result_upsert_statement
from newcoin_trader.domain.enums import ExecMode, RejectReason, Side, SignalKind
from newcoin_trader.domain.execution import PaperOrder, PortfolioState, RejectedOrder
from newcoin_trader.domain.market import PriceSnapshot
from newcoin_trader.domain.strategy import StrategyContext
from newcoin_trader.errors import ConfigError, LiveExecutionForbiddenError
from newcoin_trader.execution.gateway import ExecutionGateway
from newcoin_trader.execution.paper_broker import PaperBroker
from newcoin_trader.execution.safety import ensure_paper_mode
from newcoin_trader.research.pipeline import analyze_listing
from newcoin_trader.risk.checks import evaluate
from newcoin_trader.risk.limits import RiskLimits
from newcoin_trader.strategies.listing_momentum import ListingMomentumStrategy


def _index_names(table: object) -> set[str]:
    return {i.name for i in table.indexes if i.name}  # type: ignore[attr-defined]


def _constraint_names(table: object) -> set[str]:
    return {c.name for c in table.constraints if c.name}  # type: ignore[attr-defined]


def test_trades_external_id_unique_includes_token_id() -> None:
    names = _index_names(Trade.__table__) | _constraint_names(Trade.__table__)
    assert "uq_trades_token_source_external_id" in names
    assert "uq_trades_composite_fallback" in names
    stmt = trade_upsert_statement(
        token_id=1,
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        side="buy",
        amount=Decimal("1"),
        price=Decimal("1"),
        source="binance",
        external_trade_id="abc",
    )
    sql = str(stmt.compile(dialect=postgresql.dialect())).lower()
    assert "token_id" in sql
    assert "on conflict" in sql


def test_ensure_paper_mode_rejects_arbitrary_strings() -> None:
    with pytest.raises(LiveExecutionForbiddenError):
        ensure_paper_mode("prod")
    with pytest.raises(LiveExecutionForbiddenError):
        ensure_paper_mode("PAPERISH")
    assert ensure_paper_mode("paper") is ExecMode.PAPER
    assert ensure_paper_mode(ExecMode.PAPER) is ExecMode.PAPER


def test_gateway_invalid_mode_never_calls_broker() -> None:
    class Counting:
        def __init__(self) -> None:
            self.n = 0

        def fill(self, order: PaperOrder, *, market: PriceSnapshot | None = None) -> None:
            self.n += 1

    broker = Counting()
    gateway = ExecutionGateway(broker=broker)
    order = PaperOrder(
        token_address="T",
        chain="solana",
        side=Side.BUY,
        requested_qty=Decimal("1"),
        limit_price=Decimal("1"),
        signal_ts=datetime(2024, 1, 1, tzinfo=UTC),
    )
    with pytest.raises(LiveExecutionForbiddenError):
        gateway.submit(order, mode="not-a-mode")
    assert broker.n == 0


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


def test_strategy_sorts_and_ignores_prelisting() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    eval_ts = t0 + timedelta(minutes=10)
    # Unsorted + contaminated pre-listing spike that would invert returns if used first/last.
    snaps = (
        _snap(eval_ts, "1.20"),
        _snap(t0 - timedelta(hours=1), "100.00"),
        _snap(t0, "1.00"),
        _snap(eval_ts + timedelta(hours=1), "9.00"),
    )
    ctx = StrategyContext(
        token_address="TOKEN",
        listing_time=t0,
        evaluation_time=eval_ts,
        snapshots=snaps,
        parameters={"momentum_threshold": "0.05", "qty": "1"},
    )
    signals = ListingMomentumStrategy().generate(ctx)
    assert len(signals) == 1
    assert signals[0].kind is SignalKind.BUY
    assert signals[0].price == Decimal("1.20")


def test_analyze_listing_excludes_prelisting_contamination() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    snaps = [
        _snap(t0 - timedelta(hours=1), "0.01"),
        _snap(t0, "1.00"),
        _snap(t0 + timedelta(minutes=5), "1.10"),
    ]
    analysis = analyze_listing(snaps, listing_time=t0, token_address="TOKEN")
    by_name = {w.window: w for w in analysis.windows}
    assert by_name["5m"].simple_return == Decimal("0.1")
    holding = next(c for c in analysis.candidates if c.kind == "holding")
    assert holding.value == Decimal("0.1")


def test_paper_order_requires_positive_qty_and_price() -> None:
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    with pytest.raises(ValidationError):
        PaperOrder(
            token_address="T",
            chain="solana",
            side=Side.BUY,
            requested_qty=Decimal("0"),
            limit_price=Decimal("1"),
            signal_ts=ts,
        )
    with pytest.raises(ValidationError):
        PaperOrder(
            token_address="T",
            chain="solana",
            side=Side.BUY,
            requested_qty=Decimal("1"),
            limit_price=Decimal("-1"),
            signal_ts=ts,
        )


def test_paper_broker_rejects_invalid_config() -> None:
    with pytest.raises(ConfigError):
        PaperBroker(fee_bps=Decimal("-1"))
    with pytest.raises(ConfigError):
        PaperBroker(slippage_bps=Decimal("-1"))
    with pytest.raises(ConfigError):
        PaperBroker(max_fill_liquidity_fraction=Decimal("0"))
    with pytest.raises(ConfigError):
        PaperBroker(max_fill_liquidity_fraction=Decimal("1.5"))


def test_paper_fill_uses_market_price_and_enforces_buy_limit() -> None:
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    broker = PaperBroker(
        fee_bps=Decimal("0"),
        slippage_bps=Decimal("0"),
        max_fill_liquidity_fraction=Decimal("1"),
    )
    order = PaperOrder(
        token_address="TOKEN",
        chain="solana",
        side=Side.BUY,
        requested_qty=Decimal("1"),
        limit_price=Decimal("90"),
        signal_ts=ts,
    )
    market = PriceSnapshot(
        token_address="TOKEN",
        chain="solana",
        timestamp=ts,
        price=Decimal("100"),
        liquidity=Decimal("100000"),
        source="test",
    )
    result = broker.fill(order, market=market)
    assert isinstance(result, RejectedOrder)
    assert result.reason is RejectReason.LIMIT_NOT_MET


def test_paper_fill_uses_market_price_and_enforces_sell_limit() -> None:
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    broker = PaperBroker(
        fee_bps=Decimal("0"),
        slippage_bps=Decimal("0"),
        max_fill_liquidity_fraction=Decimal("1"),
    )
    order = PaperOrder(
        token_address="TOKEN",
        chain="solana",
        side=Side.SELL,
        requested_qty=Decimal("1"),
        limit_price=Decimal("110"),
        signal_ts=ts,
    )
    market = PriceSnapshot(
        token_address="TOKEN",
        chain="solana",
        timestamp=ts,
        price=Decimal("100"),
        liquidity=Decimal("100000"),
        source="test",
    )
    result = broker.fill(order, market=market)
    assert isinstance(result, RejectedOrder)
    assert result.reason is RejectReason.LIMIT_NOT_MET


def test_paper_fill_buy_applies_slippage_from_market_price() -> None:
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    broker = PaperBroker(
        fee_bps=Decimal("10"),
        slippage_bps=Decimal("100"),
        max_fill_liquidity_fraction=Decimal("1"),
    )
    order = PaperOrder(
        token_address="TOKEN",
        chain="solana",
        side=Side.BUY,
        requested_qty=Decimal("1"),
        limit_price=Decimal("2"),
        signal_ts=ts,
    )
    market = PriceSnapshot(
        token_address="TOKEN",
        chain="solana",
        timestamp=ts,
        price=Decimal("1.00"),
        liquidity=Decimal("100000"),
        source="test",
    )
    result = broker.fill(order, market=market)
    assert not isinstance(result, RejectedOrder)
    assert result.fill_price == Decimal("1.01")
    assert result.fee == Decimal("0.00101")


def test_risk_buy_uses_resulting_position_size() -> None:
    limits = RiskLimits(max_position_size=Decimal("100"), max_notional=Decimal("1000"))
    order = PaperOrder(
        token_address="T",
        chain="solana",
        side=Side.BUY,
        requested_qty=Decimal("2"),
        limit_price=Decimal("30"),
        signal_ts=datetime(2024, 1, 1, tzinfo=UTC),
    )
    portfolio = PortfolioState(
        open_positions=0,
        gross_notional=Decimal("50"),
        position_size=Decimal("50"),
        drawdown=Decimal("0"),
        observed_liquidity=Decimal("10000"),
    )
    # resulting position 50+60=110 > 100
    assert evaluate(order, portfolio, limits).reason is RejectReason.MAX_POSITION_SIZE


def test_risk_allows_risk_reducing_sell_despite_open_and_drawdown() -> None:
    limits = RiskLimits(
        max_open_positions=1,
        max_drawdown=Decimal("0.05"),
        max_position_size=Decimal("1000"),
        max_notional=Decimal("1000"),
    )
    order = PaperOrder(
        token_address="T",
        chain="solana",
        side=Side.SELL,
        requested_qty=Decimal("1"),
        limit_price=Decimal("10"),
        signal_ts=datetime(2024, 1, 1, tzinfo=UTC),
    )
    portfolio = PortfolioState(
        open_positions=3,
        gross_notional=Decimal("100"),
        position_size=Decimal("100"),
        drawdown=Decimal("0.5"),
        observed_liquidity=Decimal("10000"),
    )
    decision = evaluate(order, portfolio, limits)
    assert decision.accepted


def test_risk_rejects_sell_larger_than_position() -> None:
    limits = RiskLimits()
    order = PaperOrder(
        token_address="T",
        chain="solana",
        side=Side.SELL,
        requested_qty=Decimal("5"),
        limit_price=Decimal("10"),
        signal_ts=datetime(2024, 1, 1, tzinfo=UTC),
    )
    portfolio = PortfolioState(
        open_positions=1,
        gross_notional=Decimal("20"),
        position_size=Decimal("20"),
        drawdown=Decimal("0"),
        observed_liquidity=Decimal("10000"),
    )
    decision = evaluate(order, portfolio, limits)
    assert decision.reason is RejectReason.SELL_EXCEEDS_POSITION


def test_strategy_and_paper_idempotency_constraints_and_sql() -> None:
    assert "uq_strategy_results_run_strategy_token_window" in _constraint_names(StrategyResult.__table__)
    assert "uq_paper_trades_run_order" in _constraint_names(PaperTrade.__table__)
    s_sql = str(
        strategy_result_upsert_statement(
            run_id="00000000-0000-0000-0000-000000000001",
            strategy_name="listing_momentum",
            strategy_version="1.0.0",
            token_id=1,
            params={},
            metrics={},
            signals=None,
            window_start=datetime(2024, 1, 1, tzinfo=UTC),
            window_end=datetime(2024, 1, 2, tzinfo=UTC),
        ).compile(dialect=postgresql.dialect())
    ).lower()
    p_sql = str(
        paper_trade_upsert_statement(
            run_id="00000000-0000-0000-0000-000000000001",
            token_id=1,
            signal_ts=datetime(2024, 1, 1, tzinfo=UTC),
            side="buy",
            requested_qty=Decimal("1"),
            requested_price=Decimal("1.01"),
            fill_price=Decimal("1"),
            fill_qty=Decimal("1"),
            fee=Decimal("0"),
            slippage_bps=Decimal("0"),
            status="filled",
            reject_reason=None,
        ).compile(dialect=postgresql.dialect())
    ).lower()
    assert "on conflict" in s_sql
    assert "on conflict" in p_sql
    assert "requested_price" in p_sql
