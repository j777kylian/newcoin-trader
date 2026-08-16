"""Reproducible Phase 5 executable-backtest run identity and artifact emission."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

from newcoin_trader.domain.executable_backtest import (
    ExecutableBacktestReport,
    ExecutableBacktestRunMeta,
    ExecutableTradeResult,
    FrozenCandidateIdentity,
)
from newcoin_trader.reports.schemas import to_jsonable
from newcoin_trader.reports.writers import write_csv, write_json
from newcoin_trader.research.event_study_run import git_identity
from newcoin_trader.research.executable_backtest_aggregate import aggregate_trades
from newcoin_trader.research.executable_backtest_capabilities import capability_matrix_str
from newcoin_trader.research.executable_backtest_config import ELIGIBILITY_RULES, format_duration


def _format_delay(delay: timedelta) -> str:
    if delay.total_seconds() == 0:
        return "0s"
    return format_duration(delay)


def build_config_id(
    *,
    venue: str,
    start: datetime,
    end: datetime,
    max_events: int,
    max_trades: int,
    max_execution_inputs: int,
    latencies: Sequence[timedelta],
    holding_periods: Sequence[timedelta],
    position_notionals: Sequence[Decimal],
    max_participation: Decimal,
    assumed_fee_bps: Decimal,
    frozen_rule_ids: Sequence[str],
) -> str:
    payload = {
        "venue": venue,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "max_events": max_events,
        "max_trades": max_trades,
        "max_execution_inputs": max_execution_inputs,
        "latencies": [_format_delay(d) for d in latencies],
        "holding_periods": [format_duration(h) for h in holding_periods],
        "position_notionals": [str(n) for n in position_notionals],
        "max_participation": str(max_participation),
        "assumed_fee_bps": str(assumed_fee_bps),
        "frozen_rule_ids": list(frozen_rule_ids),
        "eligibility_rules": list(ELIGIBILITY_RULES),
        "phase": "phase_5_executable_backtest",
    }
    digest = sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return digest[:16]


def build_run_id(*, config_id: str, trade_count: int, git_sha: str | None) -> str:
    material = f"{config_id}|trades={trade_count}|git={git_sha or 'unknown'}"
    return sha256(material.encode()).hexdigest()[:32]


TRADE_CSV_COLUMNS = [
    "event_id",
    "venue",
    "token_address",
    "frozen_rule_id",
    "phase4_config_id",
    "split_label",
    "fold_index",
    "source_event_time",
    "first_seen_time",
    "decision_available_time",
    "configured_decision_time",
    "signal_time",
    "request_time",
    "fill_time",
    "exit_signal_time",
    "exit_request_time",
    "exit_fill_time",
    "status",
    "side",
    "position_notional",
    "holding_period",
    "phase4_gross_return",
    "gross_return",
    "net_return",
    "total_fee_cost",
    "total_spread_cost",
    "total_slippage_cost",
    "total_impact_cost",
    "edge_retention",
    "edge_retention_semantics",
    "confidence",
    "entry_mode",
    "exit_mode",
]


def trade_row_dict(trade: ExecutableTradeResult) -> dict[str, object]:
    return {
        "event_id": trade.event_id,
        "venue": trade.venue.value,
        "token_address": trade.token_address,
        "frozen_rule_id": trade.frozen_rule_id,
        "phase4_config_id": trade.phase4_config_id,
        "split_label": trade.split_label,
        "fold_index": trade.fold_index,
        "source_event_time": trade.source_event_time.isoformat(),
        "first_seen_time": trade.first_seen_time.isoformat(),
        "decision_available_time": trade.decision_available_time.isoformat(),
        "configured_decision_time": trade.configured_decision_time.isoformat(),
        "signal_time": trade.signal_time.isoformat(),
        "request_time": trade.request_time.isoformat(),
        "fill_time": trade.fill_time.isoformat(),
        "exit_signal_time": trade.exit_signal_time.isoformat(),
        "exit_request_time": trade.exit_request_time.isoformat(),
        "exit_fill_time": trade.exit_fill_time.isoformat(),
        "status": trade.status.value,
        "side": trade.side.value,
        "position_notional": trade.position_notional,
        "holding_period": _format_delay(trade.holding_period),
        "phase4_gross_return": trade.phase4_gross_return,
        "gross_return": trade.gross_return,
        "net_return": trade.net_return,
        "total_fee_cost": trade.total_fee_cost,
        "total_spread_cost": trade.total_spread_cost,
        "total_slippage_cost": trade.total_slippage_cost,
        "total_impact_cost": trade.total_impact_cost,
        "edge_retention": trade.edge_retention,
        "edge_retention_semantics": trade.edge_retention_semantics,
        "confidence": trade.confidence.value if trade.confidence else None,
        "entry_mode": trade.entry_fill.mode.value if trade.entry_fill else None,
        "exit_mode": trade.exit_fill.mode.value if trade.exit_fill else None,
    }


def build_executable_backtest_report(
    *,
    trades: Sequence[ExecutableTradeResult],
    venue: str,
    start: datetime,
    end: datetime,
    max_events: int,
    max_trades: int,
    max_execution_inputs: int,
    latencies: Sequence[timedelta],
    holding_periods: Sequence[timedelta],
    position_notionals: Sequence[Decimal],
    max_participation: Decimal,
    assumed_fee_bps: Decimal,
    frozen_identities: Sequence[FrozenCandidateIdentity],
    event_count: int | None = None,
) -> ExecutableBacktestReport:
    ordered = tuple(
        sorted(
            trades,
            key=lambda t: (
                t.fill_time,
                t.venue.value,
                t.frozen_rule_id,
                t.event_id,
                str(t.position_notional),
                _format_delay(t.holding_period),
            ),
        )
    )
    ordered_ids = tuple(sorted(frozen_identities, key=lambda i: (i.rule_id, i.split_label, i.fold_index or -1)))
    rule_ids = tuple(i.rule_id for i in ordered_ids)
    git_sha = git_identity()
    config_id = build_config_id(
        venue=venue,
        start=start,
        end=end,
        max_events=max_events,
        max_trades=max_trades,
        max_execution_inputs=max_execution_inputs,
        latencies=latencies,
        holding_periods=holding_periods,
        position_notionals=position_notionals,
        max_participation=max_participation,
        assumed_fee_bps=assumed_fee_bps,
        frozen_rule_ids=rule_ids,
    )
    run_id = build_run_id(config_id=config_id, trade_count=len(ordered), git_sha=git_sha)
    unique_events = {t.event_id for t in ordered}
    meta = ExecutableBacktestRunMeta(
        run_id=run_id,
        config_id=config_id,
        venue=venue,
        start=start,
        end=end,
        max_events=max_events,
        max_trades=max_trades,
        max_execution_inputs=max_execution_inputs,
        latencies=tuple(latencies),
        holding_periods=tuple(holding_periods),
        position_notionals=tuple(position_notionals),
        max_participation=max_participation,
        assumed_fee_bps=assumed_fee_bps,
        event_count=event_count if event_count is not None else len(unique_events),
        trade_count=len(ordered),
        frozen_rule_ids=rule_ids,
        git_identity=git_sha,
    )
    return ExecutableBacktestReport(
        meta=meta,
        capabilities=capability_matrix_str(),
        trades=ordered,
        aggregates=aggregate_trades(ordered),
        frozen_identities=ordered_ids,
    )


def _markdown(report: ExecutableBacktestReport) -> str:
    agg = report.aggregates
    lines = [
        "# Phase 5 Executable Historical Backtest",
        "",
        f"- run_id: `{report.meta.run_id}`",
        f"- config_id: `{report.meta.config_id}`",
        f"- venue: `{report.meta.venue}`",
        f"- events: {report.meta.event_count}",
        f"- simulated trades: {report.meta.trade_count}",
        f"- assumed_fee_bps: {report.meta.assumed_fee_bps}",
        f"- max_participation: {report.meta.max_participation}",
        "",
        "## Warnings",
        "",
    ]
    for warning in report.meta.warnings:
        lines.append(f"- {warning}")
    lines.extend(
        [
            "",
            "## Gross vs net (Phase 4 gross comparison)",
            "",
            f"- mean Phase4 gross: {agg.get('mean_phase4_gross_return')}",
            f"- mean executable gross: {agg.get('mean_gross_return')}",
            f"- mean executable net: {agg.get('mean_net_return')}",
            f"- fill coverage: {agg.get('fill_coverage')}",
            f"- mean fee / spread / slippage / impact: "
            f"{agg.get('mean_fee_cost')} / {agg.get('mean_spread_cost')} / "
            f"{agg.get('mean_slippage_cost')} / {agg.get('mean_impact_cost')}",
            f"- mean edge retention (positive gross only): {agg.get('mean_edge_retention')}",
            f"- edge retention zero-gross count: {agg.get('edge_retention_zero_gross_count')}",
            f"- edge retention negative-gross count: {agg.get('edge_retention_negative_gross_count')}",
            "",
            "## Capabilities (modeled vs exact)",
            "",
            "Historical depth is unsupported in the stored database; CEX depth walking "
            "requires supplied historical L2. DEX uses modeled liquidity participation "
            "impact (not AMM-exact). Fees are assumed when historical fees are unavailable.",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def emit_executable_backtest_artifacts(
    report: ExecutableBacktestReport,
    output_dir: Path,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = to_jsonable(report.model_dump(mode="python"))
    json_path = write_json(output_dir / "executable_backtest_summary.json", payload)
    rows = [trade_row_dict(t) for t in report.trades]
    csv_path = write_csv(
        output_dir / "executable_backtest_trades.csv",
        rows,
        fieldnames=TRADE_CSV_COLUMNS,
    )
    md_path = output_dir / "executable_backtest_summary.md"
    md_path.write_text(_markdown(report), encoding="utf-8")
    caps_path = write_json(output_dir / "executable_backtest_capabilities.json", report.capabilities)
    return {"json": json_path, "csv": csv_path, "markdown": md_path, "capabilities": caps_path}
