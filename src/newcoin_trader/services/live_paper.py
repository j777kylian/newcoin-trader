"""Phase 6 bounded live-paper orchestration (injected replay only; no HTTP)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from newcoin_trader.domain.executable_backtest import FrozenCandidateIdentity
from newcoin_trader.domain.live_paper import LivePaperReport, ReplayMarketEvent
from newcoin_trader.domain.numeric import require_finite_decimal
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.event_study_normalize import parse_venue
from newcoin_trader.research.live_paper_config import (
    DEFAULT_DECISION_DELAY,
    DEFAULT_HOLDING_PERIOD,
    DEFAULT_POSITION_NOTIONAL,
    validate_live_paper_bounds,
)
from newcoin_trader.research.live_paper_engine import process_live_paper_session
from newcoin_trader.research.live_paper_run import emit_live_paper_artifacts


def load_replay_events(path: Path) -> tuple[ReplayMarketEvent, ...]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"unable to read live-paper replay JSON: {path}") from exc
    raw = payload.get("events") if isinstance(payload, dict) else payload
    if not isinstance(raw, list):
        raise ConfigError("replay JSON must contain an 'events' list")
    try:
        return tuple(ReplayMarketEvent.model_validate(item) for item in raw)
    except Exception as exc:
        raise ConfigError(f"invalid ReplayMarketEvent payload in {path}") from exc


class LivePaperService:
    """Paper-only session runner. Feed/replay must be injected; never performs HTTP."""

    def __init__(self, session: AsyncSession | None = None) -> None:
        self._session = session
        self._repo = None
        if session is not None:
            from newcoin_trader.database.repositories.live_paper import LivePaperRepository

            self._repo = LivePaperRepository(session)

    def validate_run_args(
        self,
        *,
        venue: str,
        duration: timedelta,
        max_events: int,
        max_signals: int,
        max_trades: int,
        queue_capacity: int,
        starting_cash: Decimal,
        output_dir: Path,
        identity: FrozenCandidateIdentity,
        holding_period: timedelta = DEFAULT_HOLDING_PERIOD,
        position_notional: Decimal = DEFAULT_POSITION_NOTIONAL,
    ) -> None:
        _ = output_dir
        if not venue.strip():
            raise ConfigError("venue is required")
        if not identity.rule_id.strip() or not identity.phase4_config_id.strip():
            raise ConfigError("frozen Phase 4 rule identity is required (no rediscovery)")
        validate_live_paper_bounds(
            duration=duration,
            max_events=max_events,
            max_signals=max_signals,
            max_trades=max_trades,
            queue_capacity=queue_capacity,
            starting_cash=starting_cash,
            position_notional=position_notional,
            holding_period=holding_period,
        )

    async def run_replay(
        self,
        *,
        events: Sequence[ReplayMarketEvent],
        identity: FrozenCandidateIdentity,
        venue: str,
        duration: timedelta,
        max_events: int,
        max_signals: int,
        max_trades: int,
        queue_capacity: int,
        starting_cash: Decimal,
        output_dir: Path,
        session_start: datetime,
        position_notional: Decimal = DEFAULT_POSITION_NOTIONAL,
        holding_period: timedelta = DEFAULT_HOLDING_PERIOD,
        decision_delay: timedelta = DEFAULT_DECISION_DELAY,
        assumed_fee_bps: Decimal | None = None,
        phase4_gross_return: Decimal | None = None,
        phase5_historical_net: Decimal | None = None,
        state_store: dict[str, object] | None = None,
    ) -> tuple[LivePaperReport, dict[str, Path]]:
        venue_enum = parse_venue(venue, fallback_source=venue)
        cash = require_finite_decimal(starting_cash, name="paper_starting_cash")
        notional = require_finite_decimal(position_notional, name="position_notional")
        fee = (
            require_finite_decimal(assumed_fee_bps, name="assumed_fee_bps")
            if assumed_fee_bps is not None
            else Decimal("10")
        )
        self.validate_run_args(
            venue=venue,
            duration=duration,
            max_events=max_events,
            max_signals=max_signals,
            max_trades=max_trades,
            queue_capacity=queue_capacity,
            starting_cash=cash,
            output_dir=output_dir,
            identity=identity,
            holding_period=holding_period,
            position_notional=notional,
        )
        store = state_store if state_store is not None else {}
        if self._repo is not None:
            prior = await self._repo.load_session_state(
                venue=venue_enum.value,
                rule_id=identity.rule_id,
                phase4_config_id=identity.phase4_config_id,
                session_start=session_start,
            )
            if prior:
                store.update(prior)

        report = process_live_paper_session(
            events=events,
            venue=venue_enum,
            session_start=session_start,
            duration=duration,
            max_events=max_events,
            max_signals=max_signals,
            max_trades=max_trades,
            queue_capacity=queue_capacity,
            starting_cash=cash,
            position_notional=notional,
            holding_period=holding_period,
            identity=identity,
            assumed_fee_bps=fee,
            decision_delay=decision_delay,
            state_store=store,
            phase4_gross_return=phase4_gross_return,
            phase5_historical_net=phase5_historical_net,
        )
        if self._repo is not None:
            await self._repo.persist_report(report)
        paths = emit_live_paper_artifacts(report, output_dir)
        return report, paths
