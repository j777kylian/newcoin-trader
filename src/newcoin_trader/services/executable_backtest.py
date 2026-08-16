"""Phase 5 executable historical-backtest orchestration (DB reads → report; no writes)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from newcoin_trader.database.repositories.executable_backtest import ExecutableBacktestRepository
from newcoin_trader.domain.executable_backtest import (
    ExecutableBacktestReport,
    ExecutableTradeResult,
    FrozenCandidateIdentity,
)
from newcoin_trader.domain.feature_research import DecisionFeatureRecord
from newcoin_trader.domain.types import require_utc
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.event_study_normalize import parse_venue
from newcoin_trader.research.executable_backtest_config import (
    DEFAULT_HOLDING_PERIODS,
    DEFAULT_LATENCIES,
    DEFAULT_MAX_PARTICIPATION,
    DEFAULT_POSITION_NOTIONALS,
    assumed_fee_for_venue,
    execution_observation_bounds,
    parse_decimal_list,
    parse_duration_list,
    parse_latency_list,
    validate_executable_backtest_bounds,
)
from newcoin_trader.research.executable_backtest_engine import run_executable_backtest
from newcoin_trader.research.executable_backtest_run import (
    build_executable_backtest_report,
    emit_executable_backtest_artifacts,
)


def load_phase4_decision_records(path: Path) -> tuple[DecisionFeatureRecord, ...]:
    """Load frozen Phase 4 decision records from a feature-research summary JSON."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"unable to read Phase 4 records JSON: {path}") from exc
    raw_records = payload.get("records") if isinstance(payload, dict) else None
    if not isinstance(raw_records, list):
        raise ConfigError("Phase 4 records JSON must contain a 'records' list")
    try:
        return tuple(DecisionFeatureRecord.model_validate(item) for item in raw_records)
    except Exception as exc:
        raise ConfigError(f"invalid Phase 4 DecisionFeatureRecord payload in {path}") from exc


class ExecutableBacktestService:
    def __init__(self, session: AsyncSession) -> None:
        self._repo = ExecutableBacktestRepository(session)

    def validate_run_args(
        self,
        *,
        venue: str,
        start: datetime,
        end: datetime,
        max_events: int,
        max_trades: int,
        max_execution_inputs: int,
        output_dir: Path,
        frozen_rules: tuple[FrozenCandidateIdentity, ...],
        latencies: tuple[timedelta, ...] = DEFAULT_LATENCIES,
        holding_periods: tuple[timedelta, ...] = DEFAULT_HOLDING_PERIODS,
        position_notionals: tuple[Decimal, ...] = DEFAULT_POSITION_NOTIONALS,
        max_participation: Decimal = DEFAULT_MAX_PARTICIPATION,
        assumed_fee_bps: Decimal | None = None,
    ) -> None:
        _ = output_dir
        if not venue.strip():
            raise ConfigError("venue is required")
        if not frozen_rules:
            raise ConfigError("frozen Phase 4 rule identity is required (no rediscovery)")
        fee = assumed_fee_for_venue(venue, assumed_fee_bps)
        validate_executable_backtest_bounds(
            start=start,
            end=end,
            max_events=max_events,
            max_trades=max_trades,
            max_execution_inputs=max_execution_inputs,
            latencies=latencies,
            holding_periods=holding_periods,
            position_notionals=position_notionals,
            max_participation=max_participation,
            assumed_fee_bps=fee,
        )

    async def run(
        self,
        *,
        venue: str,
        start: datetime,
        end: datetime,
        max_events: int,
        max_trades: int,
        max_execution_inputs: int,
        output_dir: Path,
        frozen_rules: tuple[FrozenCandidateIdentity, ...],
        latency_specs: list[str] | None = None,
        holding_specs: list[str] | None = None,
        position_notionals: tuple[Decimal, ...] | None = None,
        position_notional_spec: str | None = None,
        max_participation: Decimal = DEFAULT_MAX_PARTICIPATION,
        assumed_fee_bps: Decimal | None = None,
        decision_records: tuple[DecisionFeatureRecord, ...] | None = None,
    ) -> tuple[ExecutableBacktestReport, dict[str, Path]]:
        start_utc = require_utc(start)
        end_utc = require_utc(end)
        latencies = parse_latency_list(latency_specs, default=DEFAULT_LATENCIES)
        holdings = parse_duration_list(holding_specs, default=DEFAULT_HOLDING_PERIODS)
        if position_notionals is None:
            position_notionals = parse_decimal_list(position_notional_spec, default=DEFAULT_POSITION_NOTIONALS)
        fee = assumed_fee_for_venue(venue, assumed_fee_bps)
        self.validate_run_args(
            venue=venue,
            start=start_utc,
            end=end_utc,
            max_events=max_events,
            max_trades=max_trades,
            max_execution_inputs=max_execution_inputs,
            output_dir=output_dir,
            frozen_rules=frozen_rules,
            latencies=latencies,
            holding_periods=holdings,
            position_notionals=position_notionals,
            max_participation=max_participation,
            assumed_fee_bps=fee,
        )
        # Resolve venue enum early for fee map consistency.
        parse_venue(venue, fallback_source=venue)

        events = await self._repo.list_listing_events(
            venue=venue,
            start=start_utc,
            end=end_utc,
            max_events=max_events,
        )
        records = await self._repo.list_decision_records(
            events=events,
            records=list(decision_records) if decision_records is not None else None,
        )
        if decision_records is not None and not records and events:
            # Explicit empty after filter — still emit zero-trade report.
            records = []

        token_ids = [int(e.provenance["token_id"]) for e in events if "token_id" in e.provenance]
        observations = []
        trades = []
        depth_books = []
        if events:
            obs_start, obs_end = execution_observation_bounds(
                events,
                max_latency=max(latencies) if latencies else timedelta(0),
                holding_periods=holdings,
            )
            observations = await self._repo.list_execution_observations(
                token_ids=token_ids,
                venue=venue,
                start=obs_start,
                end=obs_end,
                max_execution_inputs=max_execution_inputs,
            )
            trades = await self._repo.list_execution_trades(
                token_ids=token_ids,
                venue=venue,
                start=obs_start,
                end=obs_end,
                max_trades=max_trades,
            )
            depth_books = await self._repo.list_historical_depth(
                token_ids=token_ids,
                venue=venue,
                start=obs_start,
                end=obs_end,
                max_execution_inputs=max_execution_inputs,
            )

        # When no Phase 4 records were supplied, synthesize nothing and emit empty trades —
        # Phase 5 never runs Phase 4 discovery/feature rebuild.
        simulated: tuple[ExecutableTradeResult, ...] = ()
        if records:
            simulated = run_executable_backtest(
                events=events,
                records=records,
                identities=frozen_rules,
                observations=observations,
                latencies=latencies,
                holding_periods=holdings,
                position_notionals=position_notionals,
                assumed_fee_bps=fee,
                max_participation=max_participation,
                depth_books=depth_books,
                trades=trades,
            )

        report = build_executable_backtest_report(
            trades=simulated,
            venue=venue,
            start=start_utc,
            end=end_utc,
            max_events=max_events,
            max_trades=max_trades,
            max_execution_inputs=max_execution_inputs,
            latencies=latencies,
            holding_periods=holdings,
            position_notionals=position_notionals,
            max_participation=max_participation,
            assumed_fee_bps=fee,
            frozen_identities=frozen_rules,
            event_count=len(events),
        )
        paths = emit_executable_backtest_artifacts(report, output_dir)
        return report, paths
