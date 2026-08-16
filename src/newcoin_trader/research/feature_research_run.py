"""Reproducible Phase 4 feature-research run identity and artifact emission."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

from newcoin_trader.domain.feature_research import (
    DISCLAIMER,
    WARNING_NO_EXECUTION,
    CandidateRule,
    DecisionAvailabilityExclusion,
    DecisionFeatureRecord,
    FeatureBinStats,
    FeatureResearchReport,
    FeatureResearchRunMeta,
    FutureLabel,
)
from newcoin_trader.reports.schemas import to_jsonable
from newcoin_trader.reports.writers import write_csv, write_json
from newcoin_trader.research.event_study_run import git_identity
from newcoin_trader.research.feature_research_analysis import (
    chronological_split,
    compute_univariate_stats,
)
from newcoin_trader.research.feature_research_analysis import (
    walk_forward_folds as build_walk_forward_folds,
)
from newcoin_trader.research.feature_research_availability import availability_matrix_str
from newcoin_trader.research.feature_research_config import ELIGIBILITY_RULES, format_duration
from newcoin_trader.research.feature_research_rules import select_and_test_rules


def _format_delay(delay: timedelta) -> str:
    if delay.total_seconds() <= 0:
        return "0s"
    return format_duration(delay)


def build_config_id(
    *,
    venue: str,
    start: datetime,
    end: datetime,
    max_events: int,
    decision_delay: timedelta,
    windows: Sequence[timedelta],
    min_sample: int,
    split_ratios: tuple[Decimal, Decimal, Decimal],
    walk_forward_folds_n: int,
    max_rules: int,
    max_rule_conditions: int,
) -> str:
    payload = {
        "venue": venue,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "max_events": max_events,
        "decision_delay": _format_delay(decision_delay),
        "windows": [format_duration(w) for w in windows],
        "min_sample": min_sample,
        "split_ratios": [str(r) for r in split_ratios],
        "walk_forward_folds": walk_forward_folds_n,
        "max_rules": max_rules,
        "max_rule_conditions": max_rule_conditions,
        "eligibility_rules": list(ELIGIBILITY_RULES),
        "phase": "phase_4_feature_research",
    }
    digest = sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return digest[:16]


def build_run_id(*, config_id: str, record_count: int, git_sha: str | None) -> str:
    material = f"{config_id}|records={record_count}|git={git_sha or 'unknown'}"
    return sha256(material.encode()).hexdigest()[:32]


def _feature_csv_columns(records: Sequence[DecisionFeatureRecord]) -> list[str]:
    names: set[str] = set()
    for record in records:
        for feat in record.features:
            names.add(feat.name)
            names.add(f"{feat.name}__state")
    return sorted(names)


FEATURE_CSV_BASE = [
    "event_id",
    "venue",
    "chain",
    "token_address",
    "pair_address",
    "source_event_time",
    "first_seen_time",
    "first_market_data_time",
    "decision_available_time",
    "decision_time",
    "feature_cutoff",
    "config_id",
    "computation_id",
    "label_status",
    "label_simple_return",
    "label_mfe",
    "label_mae",
    "label_entry_delay",
    "label_holding_period",
]


def _record_csv_row(record: DecisionFeatureRecord, feature_cols: Sequence[str]) -> dict[str, object]:
    feat_map = {f.name: f for f in record.features}
    label: FutureLabel | None = record.labels[0] if record.labels else None
    row: dict[str, object] = {
        "event_id": record.event_id,
        "venue": record.venue.value,
        "chain": record.chain.value,
        "token_address": record.token_address,
        "pair_address": record.pair_address,
        "source_event_time": record.source_event_time.isoformat(),
        "first_seen_time": record.first_seen_time.isoformat(),
        "first_market_data_time": (
            record.first_market_data_time.isoformat() if record.first_market_data_time else None
        ),
        "decision_available_time": record.decision_available_time.isoformat(),
        "decision_time": record.decision_time.isoformat(),
        "feature_cutoff": record.feature_cutoff.isoformat(),
        "config_id": record.config_id,
        "computation_id": record.computation_id,
        "label_status": label.status.value if label else None,
        "label_simple_return": label.simple_return if label else None,
        "label_mfe": label.mfe if label else None,
        "label_mae": label.mae if label else None,
        "label_entry_delay": format_duration(label.entry_delay) if label else None,
        "label_holding_period": format_duration(label.holding_period) if label else None,
    }
    for col in feature_cols:
        if col.endswith("__state"):
            name = col[: -len("__state")]
            feat = feat_map.get(name)
            row[col] = feat.state.value if feat else None
        else:
            feat = feat_map.get(col)
            row[col] = feat.value if feat else None
    return row


def _stats_dict(stat: FeatureBinStats) -> dict[str, object]:
    return {
        "venue": stat.venue.value,
        "feature_name": stat.feature_name,
        "bin_label": stat.bin_label,
        "samples": stat.samples,
        "complete_count": stat.complete_count,
        "censored_count": stat.censored_count,
        "valid_return_count": stat.valid_return_count,
        "mean_simple_return": stat.mean_simple_return,
        "median_simple_return": stat.median_simple_return,
        "win_rate": stat.win_rate,
        "p10": stat.p10,
        "p25": stat.p25,
        "p75": stat.p75,
        "p90": stat.p90,
        "std_simple_return": stat.std_simple_return,
        "mean_mfe": stat.mean_mfe,
        "mean_mae": stat.mean_mae,
        "insufficient_sample": stat.insufficient_sample,
    }


def _rule_dict(rule: CandidateRule) -> dict[str, object]:
    return {
        "rule_id": rule.rule_id,
        "human_readable": rule.human_readable,
        "conditions": [{"feature_name": c.feature_name, "op": c.op, "threshold": c.threshold} for c in rule.conditions],
        "train_sample_count": rule.train_sample_count,
        "train_mean_return": rule.train_mean_return,
        "validation_mean_return": rule.validation_mean_return,
        "test_mean_return": rule.test_mean_return,
        "selected": rule.selected,
        "train_event_ids": list(rule.train_event_ids),
    }


def build_feature_research_report(
    *,
    records: Sequence[DecisionFeatureRecord],
    venue: str,
    start: datetime,
    end: datetime,
    max_events: int,
    decision_delay: timedelta,
    windows: Sequence[timedelta],
    min_sample: int,
    split_ratios: tuple[Decimal, Decimal, Decimal],
    walk_forward_folds: int,
    max_rules: int,
    max_rule_conditions: int,
    exclusions: Sequence[str] = (),
    decision_exclusions: Sequence[DecisionAvailabilityExclusion] = (),
) -> FeatureResearchReport:
    ordered = tuple(
        sorted(
            records,
            key=lambda r: (r.decision_time, r.venue.value, r.token_address, r.event_id),
        )
    )
    ordered_exclusions = tuple(
        sorted(
            decision_exclusions,
            key=lambda e: (e.configured_decision_time, e.event_id, e.decision_available_time),
        )
    )
    split = chronological_split(ordered, ratios=split_ratios)
    univariate = compute_univariate_stats(ordered, min_sample=min_sample)
    rules = select_and_test_rules(
        train=split.train,
        validation=split.validation,
        test=split.test,
        max_rules=max_rules,
        max_conditions=max_rule_conditions,
        min_sample=max(1, min_sample // 4),
    )
    folds = build_walk_forward_folds(
        ordered,
        n_folds=walk_forward_folds,
        min_train=max(3, min_sample // 5),
        min_test=max(2, min_sample // 10),
    )
    git_sha = git_identity()
    config_id = build_config_id(
        venue=venue,
        start=start,
        end=end,
        max_events=max_events,
        decision_delay=decision_delay,
        windows=windows,
        min_sample=min_sample,
        split_ratios=split_ratios,
        walk_forward_folds_n=walk_forward_folds,
        max_rules=max_rules,
        max_rule_conditions=max_rule_conditions,
    )
    run_id = build_run_id(config_id=config_id, record_count=len(ordered), git_sha=git_sha)
    meta = FeatureResearchRunMeta(
        run_id=run_id,
        config_id=config_id,
        venue=venue,
        start=start,
        end=end,
        max_events=max_events,
        decision_delay=decision_delay,
        windows=tuple(windows),
        min_sample=min_sample,
        split_ratios=split_ratios,
        walk_forward_folds=walk_forward_folds,
        max_rules=max_rules,
        max_rule_conditions=max_rule_conditions,
        event_count=len({r.event_id for r in ordered}),
        record_count=len(ordered),
        git_identity=git_sha,
    )
    return FeatureResearchReport(
        meta=meta,
        availability=availability_matrix_str(),
        records=ordered,
        univariate=univariate,
        split=split,
        rules=rules,
        folds=folds,
        exclusions=tuple(exclusions),
        decision_exclusions=ordered_exclusions,
        extras={
            "disclaimer": DISCLAIMER,
            "warning": WARNING_NO_EXECUTION,
            "eligibility_rules": list(ELIGIBILITY_RULES),
            "decision_exclusion_count": len(ordered_exclusions),
        },
    )


def write_markdown_summary(path: Path, report: FeatureResearchReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = [
        "# Phase 4 feature-research summary",
        "",
        f"- study_kind: `{report.meta.study_kind}`",
        f"- run_id: `{report.meta.run_id}`",
        f"- config_id: `{report.meta.config_id}`",
        f"- venue: `{report.meta.venue}`",
        f"- start: `{report.meta.start.isoformat()}`",
        f"- end: `{report.meta.end.isoformat()}`",
        f"- max_events: `{report.meta.max_events}`",
        f"- record_count: `{report.meta.record_count}`",
        f"- git_identity: `{report.meta.git_identity or 'unknown'}`",
        "",
        f"**{DISCLAIMER}**",
        "",
        f"Warning: `{WARNING_NO_EXECUTION}`",
        "",
        "## Availability matrix",
        "",
    ]
    for venue_key, families in sorted(report.availability.items()):
        lines.append(f"### {venue_key}")
        lines.append("")
        for family, level in sorted(families.items()):
            lines.append(f"- `{family}`: `{level}`")
        lines.append("")
    lines.extend(
        [
            "## Univariate scorecard (venue-first)",
            "",
            "| venue | feature | bin | samples | complete | censored | mean | median | win | insufficient |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for stat in report.univariate:
        lines.append(
            f"| {stat.venue.value} | {stat.feature_name} | {stat.bin_label} | {stat.samples} | "
            f"{stat.complete_count} | {stat.censored_count} | "
            f"{stat.mean_simple_return if stat.mean_simple_return is not None else ''} | "
            f"{stat.median_simple_return if stat.median_simple_return is not None else ''} | "
            f"{stat.win_rate if stat.win_rate is not None else ''} | "
            f"{stat.insufficient_sample} |"
        )
    if not report.univariate:
        lines.append("| _(none)_ |  |  | 0 | 0 | 0 |  |  |  | true |")
    lines.extend(["", "## Selected rules", ""])
    if report.rules and report.rules.selected:
        for rule in report.rules.selected:
            lines.append(
                f"- `{rule.human_readable}` "
                f"(train={rule.train_mean_return}, val={rule.validation_mean_return}, "
                f"test={rule.test_mean_return})"
            )
    else:
        lines.append("- _(none)_")
    exclusion_lines = [f"- `{e}`" for e in report.exclusions] if report.exclusions else ["- _(none)_"]
    decision_excl_lines = (
        [
            (
                f"- `{e.event_id}`: configured=`{e.configured_decision_time.isoformat()}` "
                f"available=`{e.decision_available_time.isoformat()}` reason=`{e.reason}`"
            )
            for e in report.decision_exclusions
        ]
        if report.decision_exclusions
        else ["- _(none)_"]
    )
    lines.extend(
        [
            "",
            "## Warnings",
            "",
            *[f"- `{w}`" for w in report.meta.warnings],
            "",
            "## Exclusions",
            "",
            *exclusion_lines,
            "",
            "## Decision availability exclusions",
            "",
            *decision_excl_lines,
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def emit_feature_research_artifacts(
    report: FeatureResearchReport,
    output_dir: Path,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    feature_cols = _feature_csv_columns(report.records)
    fieldnames = FEATURE_CSV_BASE + list(feature_cols)
    summary_payload = {
        "meta": report.meta.model_dump(mode="python"),
        "availability": report.availability,
        "splits": {
            "ratios": [str(r) for r in (report.split.ratios if report.split else ())],
            "train_count": len(report.split.train) if report.split else 0,
            "validation_count": len(report.split.validation) if report.split else 0,
            "test_count": len(report.split.test) if report.split else 0,
        },
        "univariate": [_stats_dict(s) for s in report.univariate],
        "rules": {
            "candidates": [_rule_dict(r) for r in (report.rules.candidates if report.rules else ())],
            "selected": [_rule_dict(r) for r in (report.rules.selected if report.rules else ())],
            "test_evaluated_once": report.rules.test_evaluated_once if report.rules else True,
        },
        "folds": [
            {
                "fold_index": f.fold_index,
                "train_count": len(f.train),
                "test_count": len(f.test),
                "train_end": f.train[-1].decision_time.isoformat() if f.train else None,
                "test_start": f.test[0].decision_time.isoformat() if f.test else None,
            }
            for f in report.folds
        ],
        "exclusions": list(report.exclusions),
        "decision_exclusions": [
            {
                "event_id": e.event_id,
                "configured_decision_time": e.configured_decision_time.isoformat(),
                "decision_available_time": e.decision_available_time.isoformat(),
                "reason": e.reason,
            }
            for e in report.decision_exclusions
        ],
        "extras": report.extras,
        "record_count": len(report.records),
    }
    json_path = write_json(output_dir / "feature_research_summary.json", to_jsonable(summary_payload))
    csv_path = write_csv(
        output_dir / "feature_research_records.csv",
        [_record_csv_row(r, feature_cols) for r in report.records],
        fieldnames=fieldnames,
    )
    md_path = write_markdown_summary(output_dir / "feature_research_summary.md", report)
    write_json(output_dir / "feature_research_availability.json", to_jsonable(report.availability))
    write_json(
        output_dir / "feature_research_stats.json",
        to_jsonable([_stats_dict(s) for s in report.univariate]),
    )
    return {"json": json_path, "csv": csv_path, "markdown": md_path}
