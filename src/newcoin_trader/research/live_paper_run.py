"""Reproducible Phase 6 live-paper artifact emission."""

from __future__ import annotations

from pathlib import Path

from newcoin_trader.domain.live_paper import LivePaperReport, PaperFillRecord, PaperSignalRecord
from newcoin_trader.reports.schemas import to_jsonable
from newcoin_trader.reports.writers import write_csv, write_json
from newcoin_trader.research.event_study_config import format_duration

SIGNAL_CSV_COLUMNS = [
    "signal_id",
    "session_id",
    "event_id",
    "rule_id",
    "phase4_config_id",
    "split_label",
    "fold_index",
    "decision_time",
    "status",
    "reason",
    "source_timestamp",
    "received_timestamp",
]

FILL_CSV_COLUMNS = [
    "fill_id",
    "session_id",
    "signal_id",
    "position_id",
    "side",
    "status",
    "mode",
    "confidence",
    "request_time",
    "fill_time",
    "requested_qty",
    "fill_qty",
    "fill_price",
    "notional",
    "fee_cost",
    "spread_cost",
    "slippage_cost",
    "impact_cost",
    "label",
    "source",
]


def _signal_row(signal: PaperSignalRecord) -> dict[str, object]:
    return {
        "signal_id": signal.signal_id,
        "session_id": signal.session_id,
        "event_id": signal.event_id,
        "rule_id": signal.rule_id,
        "phase4_config_id": signal.phase4_config_id,
        "split_label": signal.split_label,
        "fold_index": signal.fold_index,
        "decision_time": signal.decision_time.isoformat(),
        "status": signal.status.value,
        "reason": signal.reason.value if signal.reason else None,
        "source_timestamp": signal.source_timestamp.isoformat() if signal.source_timestamp else None,
        "received_timestamp": signal.received_timestamp.isoformat() if signal.received_timestamp else None,
    }


def _fill_row(fill: PaperFillRecord) -> dict[str, object]:
    return {
        "fill_id": fill.fill_id,
        "session_id": fill.session_id,
        "signal_id": fill.signal_id,
        "position_id": fill.position_id,
        "side": fill.side.value,
        "status": fill.status.value,
        "mode": fill.mode.value,
        "confidence": fill.confidence.value,
        "request_time": fill.request_time.isoformat(),
        "fill_time": fill.fill_time.isoformat(),
        "requested_qty": fill.requested_qty,
        "fill_qty": fill.fill_qty,
        "fill_price": fill.fill_price,
        "notional": fill.notional,
        "fee_cost": fill.fee_cost,
        "spread_cost": fill.spread_cost,
        "slippage_cost": fill.slippage_cost,
        "impact_cost": fill.impact_cost,
        "label": fill.label,
        "source": fill.source,
    }


def _markdown(report: LivePaperReport) -> str:
    meta = report.meta
    port = report.portfolio
    lines = [
        "# Phase 6 Bounded Live-Paper Session",
        "",
        f"- session_id: `{meta.session_id}`",
        f"- config_id: `{meta.config_id}`",
        f"- venue: `{meta.venue}`",
        f"- duration: {format_duration(meta.duration)}",
        f"- events admitted/supplied: {meta.admitted_event_count}/{meta.supplied_event_count}",
        f"- max_events rejected: {meta.max_events_rejected_count}",
        f"- accepted signals: {meta.signal_count}",
        f"- paper positions (trades): {meta.trade_count}",
        f"- fills: {meta.fill_count}",
        f"- queue overflow: {meta.overflow_count}",
        f"- halted: {meta.halted}",
        "",
        "## Warnings",
        "",
    ]
    for warning in meta.warnings:
        lines.append(f"- {warning}")
    lines.extend(
        [
            "",
            "## Portfolio",
            "",
            f"- cash: {port.cash}",
            f"- equity: {port.equity}",
            f"- realized_pnl: {port.realized_pnl}",
            f"- unrealized_pnl: {port.unrealized_pnl}",
            f"- drawdown: {port.drawdown}",
            f"- open_positions: {port.open_positions}",
            f"- failed_positions: {port.failed_positions}",
            "",
            "## Data quality",
            "",
        ]
    )
    for key, value in sorted(report.data_quality.items()):
        lines.append(f"- {key}: {value}")
    if report.comparison:
        lines.extend(["", "## Comparison (Phase4 gross / Phase5 net / Phase6 paper)", ""])
        for key, value in sorted(report.comparison.items()):
            lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def emit_live_paper_artifacts(report: LivePaperReport, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = to_jsonable(report.model_dump(mode="python"))
    json_path = write_json(output_dir / "live_paper_summary.json", payload)
    signal_rows = [_signal_row(s) for s in report.signals]
    csv_path = write_csv(
        output_dir / "live_paper_signals.csv",
        signal_rows,
        fieldnames=SIGNAL_CSV_COLUMNS,
    )
    fill_rows = [_fill_row(f) for f in report.fills]
    fills_csv = write_csv(
        output_dir / "live_paper_fills.csv",
        fill_rows,
        fieldnames=FILL_CSV_COLUMNS,
    )
    md_path = output_dir / "live_paper_summary.md"
    md_path.write_text(_markdown(report), encoding="utf-8")
    return {"json": json_path, "csv": csv_path, "fills_csv": fills_csv, "markdown": md_path}
