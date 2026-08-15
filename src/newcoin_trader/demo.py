"""Offline fixture-driven smoke path. No network and no database."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from importlib.resources import files
from pathlib import Path
from uuid import UUID

from newcoin_trader.domain.enums import ExecMode, Side, SignalKind
from newcoin_trader.domain.execution import PaperOrder, PortfolioState
from newcoin_trader.domain.market import PriceSnapshot
from newcoin_trader.domain.strategy import StrategyContext
from newcoin_trader.execution.gateway import ExecutionGateway
from newcoin_trader.execution.paper_broker import PaperBroker
from newcoin_trader.reports.writers import write_csv, write_json
from newcoin_trader.research.pipeline import analyze_listing
from newcoin_trader.risk.checks import evaluate
from newcoin_trader.risk.limits import RiskLimits
from newcoin_trader.strategies.listing_momentum import ListingMomentumStrategy


def _resolve_fixture_texts(*, fixtures_dir: Path | None) -> tuple[str, str]:
    if fixtures_dir is not None:
        meta_text = (fixtures_dir / "meta.json").read_text(encoding="utf-8")
        snapshots_text = (fixtures_dir / "snapshots.json").read_text(encoding="utf-8")
        return meta_text, snapshots_text
    root = files("newcoin_trader.resources.demo_run")
    meta_text = root.joinpath("meta.json").read_text(encoding="utf-8")
    snapshots_text = root.joinpath("snapshots.json").read_text(encoding="utf-8")
    return meta_text, snapshots_text


def _stable_run_id(*, meta_text: str, snapshots_text: str, run_id: str | None) -> str:
    if run_id is not None:
        return str(UUID(run_id))
    digest = hashlib.sha256(f"{meta_text}\n{snapshots_text}".encode()).hexdigest()
    return str(UUID(digest[:32]))


def _load_snapshots(text: str) -> list[PriceSnapshot]:
    payload = json.loads(text)
    return [PriceSnapshot.model_validate(item) for item in payload]


def run_offline_smoke(
    *,
    output_dir: Path | None = None,
    fixtures_dir: Path | None = None,
    run_id: str | None = None,
) -> int:
    out = output_dir or Path("artifacts")
    out.mkdir(parents=True, exist_ok=True)

    meta_text, snapshots_text = _resolve_fixture_texts(fixtures_dir=fixtures_dir)
    snapshots = _load_snapshots(snapshots_text)
    meta = json.loads(meta_text)
    listing_time = snapshots[0].timestamp
    token_address = str(meta["token_address"])
    evaluation_time = snapshots[3].timestamp
    resolved_run_id = _stable_run_id(
        meta_text=meta_text,
        snapshots_text=snapshots_text,
        run_id=run_id,
    )

    analysis = analyze_listing(
        snapshots,
        listing_time=listing_time,
        token_address=token_address,
        evaluation_time=evaluation_time,
    )

    strategy = ListingMomentumStrategy()
    ctx = StrategyContext(
        token_address=token_address,
        listing_time=listing_time,
        evaluation_time=evaluation_time,
        snapshots=tuple(snapshots),
        parameters={
            "momentum_threshold": "0.05",
            "exit_threshold": "-0.20",
            "qty": "10",
        },
    )
    signals = strategy.generate(ctx)

    broker = PaperBroker(
        fee_bps=Decimal("10"),
        slippage_bps=Decimal("25"),
        max_fill_liquidity_fraction=Decimal("0.10"),
    )
    gateway = ExecutionGateway(broker=broker)
    limits = RiskLimits(
        max_notional=Decimal("10000"),
        max_position_size=Decimal("5000"),
        max_open_positions=3,
        max_drawdown=Decimal("0.50"),
        min_liquidity=Decimal("1000"),
    )
    portfolio = PortfolioState(
        open_positions=0,
        gross_notional=Decimal("0"),
        position_size=Decimal("0"),
        drawdown=Decimal("0"),
        observed_liquidity=Decimal(str(meta.get("observed_liquidity", "20000"))),
    )

    fills: list[dict[str, object]] = []
    market = snapshots[3]
    for signal in signals:
        if signal.kind is SignalKind.HOLD:
            continue
        # Limit allows configured paper slippage against observed market.price.
        slip = Decimal("1") + Decimal("25") / Decimal("10000")
        if signal.kind is SignalKind.BUY:
            limit_price = signal.price * slip
            side = Side.BUY
        else:
            limit_price = signal.price / slip
            side = Side.SELL
        order = PaperOrder(
            token_address=token_address,
            chain="solana",
            side=side,
            requested_qty=signal.qty,
            limit_price=limit_price,
            signal_ts=signal.timestamp,
            run_id=resolved_run_id,
        )
        decision = evaluate(order, portfolio, limits)
        if not decision.accepted:
            fills.append(
                {
                    "status": "rejected",
                    "reason": str(decision.reason),
                    "detail": decision.detail,
                }
            )
            continue
        result = gateway.submit(order, mode=ExecMode.PAPER, market=market)
        fills.append(result.model_dump(mode="python"))

    write_json(
        out / "analysis.json",
        analysis.model_dump(mode="python"),
    )
    write_json(
        out / "paper_run.json",
        {
            "run_id": resolved_run_id,
            "strategy": strategy.name,
            "version": strategy.version,
            "signals": [s.model_dump(mode="python") for s in signals],
            "fills": fills,
            "disclaimer": "research_output_not_trading_advice",
        },
    )
    write_csv(
        out / "window_stats.csv",
        [
            {
                "window": w.window,
                "simple_return": w.simple_return,
                "volatility": w.volatility,
                "max_drawdown": w.max_drawdown,
                "mean_liquidity": w.mean_liquidity,
                "mean_volume": w.mean_volume,
                "n_observations": w.n_observations,
            }
            for w in analysis.windows
        ],
        fieldnames=[
            "window",
            "simple_return",
            "volatility",
            "max_drawdown",
            "mean_liquidity",
            "mean_volume",
            "n_observations",
        ],
    )
    return 0
