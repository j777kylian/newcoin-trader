"""Phase 3 event-study orchestration service (DB reads → deterministic report)."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from newcoin_trader.database.repositories.event_study import EventStudyRepository
from newcoin_trader.domain.event_study import EventStudyReport
from newcoin_trader.domain.types import require_utc
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.event_study_config import (
    DEFAULT_ENTRY_DELAYS,
    DEFAULT_HOLDING_PERIODS,
    DEFAULT_MAX_OBSERVATIONS,
    observation_snapshot_bounds,
    parse_duration_list,
    validate_event_study_bounds,
)
from newcoin_trader.research.event_study_run import build_report, emit_event_study_artifacts


class EventStudyService:
    def __init__(self, session: AsyncSession) -> None:
        self._repo = EventStudyRepository(session)

    async def run(
        self,
        *,
        venue: str,
        start: datetime,
        end: datetime,
        max_events: int,
        output_dir: Path,
        entry_delay_specs: list[str] | None = None,
        holding_period_specs: list[str] | None = None,
        seed: int | None = None,
        max_observations: int = DEFAULT_MAX_OBSERVATIONS,
    ) -> tuple[EventStudyReport, dict[str, Path]]:
        start_utc = require_utc(start)
        end_utc = require_utc(end)
        entry_delays = parse_duration_list(entry_delay_specs, default=DEFAULT_ENTRY_DELAYS)
        holding_periods = parse_duration_list(holding_period_specs, default=DEFAULT_HOLDING_PERIODS)
        try:
            validate_event_study_bounds(
                start=start_utc,
                end=end_utc,
                max_events=max_events,
                entry_delays=entry_delays,
                holding_periods=holding_periods,
                max_observations=max_observations,
            )
        except ConfigError:
            raise

        if not venue.strip():
            raise ConfigError("venue is required")

        events = await self._repo.list_listing_events(
            venue=venue,
            start=start_utc,
            end=end_utc,
            max_events=max_events,
        )
        token_ids = [int(e.provenance["token_id"]) for e in events if "token_id" in e.provenance]
        data_horizon_end: datetime | None = None
        if events:
            obs_start, obs_end = observation_snapshot_bounds(events, entry_delays, holding_periods)
            data_horizon_end = obs_end
            observations = await self._repo.list_observations_for_tokens(
                token_ids=token_ids,
                venue=venue,
                start=obs_start,
                end=obs_end,
                max_observations=max_observations,
            )
        else:
            observations = []
        report = build_report(
            events=events,
            observations=observations,
            venue=venue,
            start=start_utc,
            end=end_utc,
            max_events=max_events,
            entry_delays=entry_delays,
            holding_periods=holding_periods,
            seed=seed,
            data_horizon_end=data_horizon_end,
        )
        paths = emit_event_study_artifacts(report, output_dir)
        return report, paths


def parse_cli_datetime(value: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp (must be timezone-aware)."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ConfigError(f"invalid datetime: {value!r}") from exc
    return require_utc(parsed)


def split_duration_option(raw: str | None) -> list[str] | None:
    if raw is None or raw.strip() == "":
        return None
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    return parts or None


def resolve_grid(
    entry_delay_specs: list[str] | None,
    holding_period_specs: list[str] | None,
) -> tuple[tuple[timedelta, ...], tuple[timedelta, ...]]:
    return (
        parse_duration_list(entry_delay_specs, default=DEFAULT_ENTRY_DELAYS),
        parse_duration_list(holding_period_specs, default=DEFAULT_HOLDING_PERIODS),
    )
