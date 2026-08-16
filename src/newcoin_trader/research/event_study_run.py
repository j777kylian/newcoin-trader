"""Reproducible Phase 3 event-study run identity and report emission."""

from __future__ import annotations

import hashlib
import json
import subprocess
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path

from newcoin_trader.domain.event_study import (
    DISCLAIMER,
    WARNING_NO_PNL,
    EventStudyCellResult,
    EventStudyReport,
    EventStudyRunMeta,
    MarketObservation,
    TokenListingEvent,
)
from newcoin_trader.reports.schemas import to_jsonable
from newcoin_trader.reports.writers import write_csv, write_json
from newcoin_trader.research.event_study_aggregate import aggregate_results, aggregate_row_dict
from newcoin_trader.research.event_study_config import (
    ELIGIBILITY_RULES,
    format_duration,
)
from newcoin_trader.research.event_study_engine import run_event_study


def git_identity() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    sha = result.stdout.strip()
    return sha or None


def build_config_id(
    *,
    venue: str,
    start: datetime,
    end: datetime,
    max_events: int,
    entry_delays: Sequence[timedelta],
    holding_periods: Sequence[timedelta],
    seed: int | None,
) -> str:
    payload = {
        "venue": venue,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "max_events": max_events,
        "entry_delays": [format_duration(d) for d in entry_delays],
        "holding_periods": [format_duration(h) for h in holding_periods],
        "seed": seed,
        "eligibility_rules": list(ELIGIBILITY_RULES),
        "phase": "phase_3_event_study",
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return digest[:16]


def build_run_id(*, config_id: str, event_count: int, observation_count: int, git_sha: str | None) -> str:
    material = f"{config_id}|events={event_count}|obs={observation_count}|git={git_sha or 'unknown'}"
    return hashlib.sha256(material.encode()).hexdigest()[:32]


def cell_row_dict(cell: EventStudyCellResult) -> dict[str, object]:
    return {
        "event_id": cell.event_id,
        "venue": cell.venue.value,
        "token_address": cell.token_address,
        "chain": cell.chain.value,
        "source_event_time": cell.source_event_time.isoformat(),
        "first_seen_time": cell.first_seen_time.isoformat(),
        "first_market_data_time": (
            cell.first_market_data_time.isoformat() if cell.first_market_data_time is not None else None
        ),
        "decision_available_time": cell.decision_available_time.isoformat(),
        "entry_delay": format_duration(cell.entry_delay),
        "holding_period": format_duration(cell.holding_period),
        "entry_time": cell.entry_time.isoformat(),
        "exit_time": cell.exit_time.isoformat(),
        "status": cell.status.value,
        "entry_price": cell.entry_price,
        "exit_price": cell.exit_price,
        "simple_return": cell.simple_return,
        "log_return": cell.log_return,
        "mfe": cell.path.mfe,
        "mae": cell.path.mae,
        "peak_price": cell.path.peak_price,
        "trough_price": cell.path.trough_price,
        "time_to_peak_seconds": (
            cell.path.time_to_peak.total_seconds() if cell.path.time_to_peak is not None else None
        ),
        "time_to_trough_seconds": (
            cell.path.time_to_trough.total_seconds() if cell.path.time_to_trough is not None else None
        ),
        "path_available": cell.path.path_available,
        "path_observation_count": cell.path.path_observation_count,
        "event_source": cell.event_source,
        "event_provenance": dict(cell.event_provenance),
        "entry_source": cell.entry_source,
        "entry_provenance": dict(cell.entry_provenance) if cell.entry_provenance is not None else None,
        "exit_source": cell.exit_source,
        "exit_provenance": dict(cell.exit_provenance) if cell.exit_provenance is not None else None,
        "label": cell.label,
        "warning": cell.warning,
    }


def build_report(
    *,
    events: Sequence[TokenListingEvent],
    observations: Sequence[MarketObservation],
    venue: str,
    start: datetime,
    end: datetime,
    max_events: int,
    entry_delays: Sequence[timedelta],
    holding_periods: Sequence[timedelta],
    seed: int | None = None,
    include_cell_results: bool = True,
    data_horizon_end: datetime | None = None,
) -> EventStudyReport:
    # ``end`` is event-selection cohort bound only; never reuse it as market-data horizon.
    horizon = data_horizon_end
    if horizon is None:
        horizon = max((obs.timestamp for obs in observations), default=None)
    cells = run_event_study(
        events,
        observations,
        entry_delays=entry_delays,
        holding_periods=holding_periods,
        data_horizon_end=horizon,
    )
    aggregates = aggregate_results(cells)
    git_sha = git_identity()
    config_id = build_config_id(
        venue=venue,
        start=start,
        end=end,
        max_events=max_events,
        entry_delays=entry_delays,
        holding_periods=holding_periods,
        seed=seed,
    )
    run_id = build_run_id(
        config_id=config_id,
        event_count=len(events),
        observation_count=len(observations),
        git_sha=git_sha,
    )
    meta = EventStudyRunMeta(
        run_id=run_id,
        config_id=config_id,
        venue=venue,
        start=start,
        end=end,
        max_events=max_events,
        entry_delays=tuple(entry_delays),
        holding_periods=tuple(holding_periods),
        eligibility_rules=ELIGIBILITY_RULES,
        seed=seed,
        git_identity=git_sha,
        event_count=len(events),
        observation_count=len(observations),
        data_horizon_end=horizon,
    )
    return EventStudyReport(
        meta=meta,
        aggregates=aggregates,
        cell_results=cells if include_cell_results else (),
        extras={
            "disclaimer": DISCLAIMER,
            "warning": WARNING_NO_PNL,
        },
    )


def write_markdown_summary(path: Path, report: EventStudyReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = [
        "# Phase 3 event-study summary",
        "",
        f"- study_kind: `{report.meta.study_kind}`",
        f"- run_id: `{report.meta.run_id}`",
        f"- config_id: `{report.meta.config_id}`",
        f"- venue: `{report.meta.venue}`",
        f"- start: `{report.meta.start.isoformat()}`",
        f"- end: `{report.meta.end.isoformat()}`",
        f"- max_events: `{report.meta.max_events}`",
        f"- event_count: `{report.meta.event_count}`",
        f"- observation_count: `{report.meta.observation_count}`",
        f"- git_identity: `{report.meta.git_identity or 'unknown'}`",
        "",
        f"**{DISCLAIMER}**",
        "",
        f"Warning: `{WARNING_NO_PNL}`",
        "",
        "## Aggregates (venue × entry_delay × holding)",
        "",
        "| venue | entry_delay | holding | samples | complete | valid | censored | mean | median | win_rate |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for agg in report.aggregates:
        row = (
            f"| {agg.venue.value} | {format_duration(agg.entry_delay)} | "
            f"{format_duration(agg.holding_period)} | {agg.samples} | "
            f"{agg.complete_count} | {agg.valid_return_count} | {agg.censored_count} | "
            f"{agg.mean_simple_return if agg.mean_simple_return is not None else ''} | "
            f"{agg.median_simple_return if agg.median_simple_return is not None else ''} | "
            f"{agg.win_rate if agg.win_rate is not None else ''} |"
        )
        lines.append(row)
    if not report.aggregates:
        lines.append("| _(none)_ |  |  | 0 | 0 | 0 | 0 |  |  |  |")
    lines.extend(
        [
            "",
            "## Eligibility rules",
            "",
            *[f"- `{rule}`" for rule in report.meta.eligibility_rules],
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


EVENT_CSV_FIELDS = [
    "event_id",
    "venue",
    "token_address",
    "chain",
    "source_event_time",
    "first_seen_time",
    "first_market_data_time",
    "decision_available_time",
    "entry_delay",
    "holding_period",
    "entry_time",
    "exit_time",
    "status",
    "entry_price",
    "exit_price",
    "simple_return",
    "log_return",
    "mfe",
    "mae",
    "peak_price",
    "trough_price",
    "time_to_peak_seconds",
    "time_to_trough_seconds",
    "path_available",
    "path_observation_count",
    "event_source",
    "event_provenance",
    "entry_source",
    "entry_provenance",
    "exit_source",
    "exit_provenance",
    "label",
    "warning",
]


def _csv_cell_row(cell: EventStudyCellResult) -> dict[str, object]:
    row = cell_row_dict(cell)
    for key in ("event_provenance", "entry_provenance", "exit_provenance"):
        value = row.get(key)
        if value is not None:
            row[key] = json.dumps(to_jsonable(value), sort_keys=True, separators=(",", ":"))
    return row


def emit_event_study_artifacts(report: EventStudyReport, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    cell_rows = [cell_row_dict(c) for c in report.cell_results]
    summary_payload = {
        "meta": report.meta.model_dump(mode="python"),
        "aggregates": [aggregate_row_dict(a) for a in report.aggregates],
        "cell_results": cell_rows,
        "extras": report.extras,
        "cell_result_count": len(report.cell_results),
    }
    json_path = write_json(output_dir / "event_study_summary.json", to_jsonable(summary_payload))
    csv_path = write_csv(
        output_dir / "event_study_cells.csv",
        [_csv_cell_row(c) for c in report.cell_results],
        fieldnames=EVENT_CSV_FIELDS,
    )
    md_path = write_markdown_summary(output_dir / "event_study_summary.md", report)
    return {"json": json_path, "csv": csv_path, "markdown": md_path}
