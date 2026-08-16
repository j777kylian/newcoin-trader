"""Phase 4 feature-research orchestration (DB reads → deterministic report)."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from newcoin_trader.database.repositories.feature_research import FeatureResearchRepository
from newcoin_trader.domain.feature_research import (
    REASON_CONFIGURED_DECISION_BEFORE_AVAILABLE,
    DecisionAvailabilityExclusion,
    DecisionFeatureRecord,
    FeatureResearchReport,
    FutureLabel,
)
from newcoin_trader.domain.types import require_utc
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.event_study_engine import evaluate_cell
from newcoin_trader.research.feature_research_availability import EXCLUDED_FAMILIES
from newcoin_trader.research.feature_research_config import (
    DEFAULT_DECISION_DELAY,
    DEFAULT_FEATURE_WINDOWS,
    DEFAULT_MAX_FEATURE_INPUTS,
    DEFAULT_MAX_RULE_CONDITIONS,
    DEFAULT_MAX_RULES,
    DEFAULT_MAX_TRADES,
    DEFAULT_MIN_SAMPLE,
    DEFAULT_SPLIT_RATIOS,
    DEFAULT_WALK_FORWARD_FOLDS,
    feature_input_bounds,
    label_observation_bounds,
    parse_duration,
    parse_duration_list,
    validate_feature_research_bounds,
)
from newcoin_trader.research.feature_research_features import build_decision_feature_record
from newcoin_trader.research.feature_research_run import (
    build_config_id,
    build_feature_research_report,
    emit_feature_research_artifacts,
)

# Default Phase-3-compatible label grid for attaching future outcomes (research only).
DEFAULT_LABEL_ENTRY_DELAYS: tuple[timedelta, ...] = (timedelta(minutes=1),)
DEFAULT_LABEL_HOLDINGS: tuple[timedelta, ...] = (timedelta(minutes=5),)


class FeatureResearchService:
    def __init__(self, session: AsyncSession) -> None:
        self._repo = FeatureResearchRepository(session)

    async def run(
        self,
        *,
        venue: str,
        start: datetime,
        end: datetime,
        max_events: int,
        output_dir: Path,
        decision_delay: timedelta = DEFAULT_DECISION_DELAY,
        window_specs: list[str] | None = None,
        min_sample: int = DEFAULT_MIN_SAMPLE,
        split_ratios: tuple[Decimal, Decimal, Decimal] = DEFAULT_SPLIT_RATIOS,
        walk_forward_folds: int = DEFAULT_WALK_FORWARD_FOLDS,
        max_rules: int = DEFAULT_MAX_RULES,
        max_rule_conditions: int = DEFAULT_MAX_RULE_CONDITIONS,
        max_feature_inputs: int = DEFAULT_MAX_FEATURE_INPUTS,
        max_trades: int = DEFAULT_MAX_TRADES,
        label_entry_delay_specs: list[str] | None = None,
        label_holding_specs: list[str] | None = None,
    ) -> tuple[FeatureResearchReport, dict[str, Path]]:
        start_utc = require_utc(start)
        end_utc = require_utc(end)
        windows = parse_duration_list(window_specs, default=DEFAULT_FEATURE_WINDOWS)
        validate_feature_research_bounds(
            start=start_utc,
            end=end_utc,
            max_events=max_events,
            decision_delay=decision_delay,
            windows=windows,
            min_sample=min_sample,
            split_ratios=split_ratios,
            walk_forward_folds=walk_forward_folds,
            max_rules=max_rules,
            max_rule_conditions=max_rule_conditions,
            max_feature_inputs=max_feature_inputs,
        )
        if not venue.strip():
            raise ConfigError("venue is required")

        label_entry_delays = parse_duration_list(label_entry_delay_specs, default=DEFAULT_LABEL_ENTRY_DELAYS)
        label_holdings = parse_duration_list(label_holding_specs, default=DEFAULT_LABEL_HOLDINGS)

        events = await self._repo.list_listing_events(
            venue=venue,
            start=start_utc,
            end=end_utc,
            max_events=max_events,
        )
        token_ids = [int(e.provenance["token_id"]) for e in events if "token_id" in e.provenance]
        config_id = build_config_id(
            venue=venue,
            start=start_utc,
            end=end_utc,
            max_events=max_events,
            decision_delay=decision_delay,
            windows=windows,
            min_sample=min_sample,
            split_ratios=split_ratios,
            walk_forward_folds_n=walk_forward_folds,
            max_rules=max_rules,
            max_rule_conditions=max_rule_conditions,
        )

        if not events:
            report = build_feature_research_report(
                records=(),
                venue=venue,
                start=start_utc,
                end=end_utc,
                max_events=max_events,
                decision_delay=decision_delay,
                windows=windows,
                min_sample=min_sample,
                split_ratios=split_ratios,
                walk_forward_folds=walk_forward_folds,
                max_rules=max_rules,
                max_rule_conditions=max_rule_conditions,
                exclusions=EXCLUDED_FAMILIES,
                decision_exclusions=(),
            )
            paths = emit_feature_research_artifacts(report, output_dir)
            return report, paths

        feat_start, feat_end = feature_input_bounds(events, decision_delay=decision_delay, windows=windows)
        feature_inputs = await self._repo.list_feature_inputs(
            token_ids=token_ids,
            venue=venue,
            start=feat_start,
            end=feat_end,
            max_feature_inputs=max_feature_inputs,
        )
        trades = await self._repo.list_feature_trades(
            token_ids=token_ids,
            venue=venue,
            start=feat_start,
            end=feat_end,
            max_trades=max_trades,
        )

        # Separate future-label observation window (Phase 3 engine); never fed to features.
        label_start, label_end = label_observation_bounds(
            events,
            decision_delay=decision_delay,
            entry_delays=label_entry_delays,
            holding_periods=label_holdings,
        )
        label_obs = await self._repo.list_label_observations(
            token_ids=token_ids,
            venue=venue,
            start=label_start,
            end=label_end,
            max_observations=max_feature_inputs,
        )
        data_horizon = max((obs.timestamp for obs in label_obs), default=label_end)

        records: list[DecisionFeatureRecord] = []
        decision_exclusions: list[DecisionAvailabilityExclusion] = []
        for event in events:
            # Configured clock only — never silently max with decision_available_time.
            decision_time = event.source_event_time + decision_delay
            if decision_time < event.decision_available_time:
                decision_exclusions.append(
                    DecisionAvailabilityExclusion(
                        event_id=event.event_id,
                        configured_decision_time=decision_time,
                        decision_available_time=event.decision_available_time,
                        reason=REASON_CONFIGURED_DECISION_BEFORE_AVAILABLE,
                    )
                )
                continue
            labels: list[FutureLabel] = []
            for entry_delay in label_entry_delays:
                for holding in label_holdings:
                    cell = evaluate_cell(
                        event,
                        label_obs,
                        entry_delay=entry_delay,
                        holding_period=holding,
                        data_horizon_end=data_horizon,
                    )
                    labels.append(
                        FutureLabel(
                            entry_delay=entry_delay,
                            holding_period=holding,
                            status=cell.status,
                            simple_return=cell.simple_return,
                            log_return=cell.log_return,
                            mfe=cell.path.mfe,
                            mae=cell.path.mae,
                            label_source="phase3_cell",
                        )
                    )
            records.append(
                build_decision_feature_record(
                    event,
                    feature_inputs,
                    trades=trades,
                    decision_time=decision_time,
                    windows=windows,
                    labels=labels,
                    config_id=config_id,
                )
            )

        report = build_feature_research_report(
            records=records,
            venue=venue,
            start=start_utc,
            end=end_utc,
            max_events=max_events,
            decision_delay=decision_delay,
            windows=windows,
            min_sample=min_sample,
            split_ratios=split_ratios,
            walk_forward_folds=walk_forward_folds,
            max_rules=max_rules,
            max_rule_conditions=max_rule_conditions,
            exclusions=EXCLUDED_FAMILIES,
            decision_exclusions=decision_exclusions,
        )
        paths = emit_feature_research_artifacts(report, output_dir)
        return report, paths


def resolve_decision_delay(spec: str | None) -> timedelta:
    if spec is None or spec.strip() == "":
        return DEFAULT_DECISION_DELAY
    return parse_duration(spec)
