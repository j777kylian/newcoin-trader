"""Typer CLI entrypoint with explicit subcommands (no single-command collapse)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import typer

from newcoin_trader.config import load_settings
from newcoin_trader.demo import run_offline_smoke
from newcoin_trader.domain.executable_backtest import FrozenCandidateIdentity
from newcoin_trader.domain.feature_research import RuleCondition
from newcoin_trader.domain.numeric import require_finite_decimal
from newcoin_trader.errors import ConfigError
from newcoin_trader.logging_setup import configure_logging
from newcoin_trader.research.event_study_config import (
    MAX_EVENTS_MAX,
    MAX_EVENTS_MIN,
    validate_event_study_bounds,
)
from newcoin_trader.research.executable_backtest_config import (
    DEFAULT_HOLDING_PERIODS as EB_DEFAULT_HOLDINGS,
)
from newcoin_trader.research.executable_backtest_config import (
    DEFAULT_LATENCIES as EB_DEFAULT_LATENCIES,
)
from newcoin_trader.research.executable_backtest_config import (
    DEFAULT_MAX_PARTICIPATION,
    assumed_fee_for_venue,
    parse_decimal_list,
    parse_latency_list,
    validate_executable_backtest_bounds,
)
from newcoin_trader.research.executable_backtest_config import (
    DEFAULT_POSITION_NOTIONALS as EB_DEFAULT_POSITIONS,
)
from newcoin_trader.research.executable_backtest_config import (
    MAX_EVENTS_MAX as EB_MAX_EVENTS_MAX,
)
from newcoin_trader.research.executable_backtest_config import (
    MAX_EVENTS_MIN as EB_MAX_EVENTS_MIN,
)
from newcoin_trader.research.executable_backtest_config import (
    parse_duration_list as parse_eb_duration_list,
)
from newcoin_trader.research.feature_research_config import (
    DEFAULT_FEATURE_WINDOWS as FR_DEFAULT_WINDOWS,
)
from newcoin_trader.research.feature_research_config import (
    DEFAULT_MAX_RULE_CONDITIONS,
    DEFAULT_MAX_RULES,
    DEFAULT_MIN_SAMPLE,
    DEFAULT_WALK_FORWARD_FOLDS,
    MAX_RULE_CONDITIONS,
    parse_duration_list,
    parse_split_ratios,
    validate_feature_research_bounds,
)
from newcoin_trader.research.live_paper_config import (
    DEFAULT_POSITION_NOTIONAL as LP_DEFAULT_NOTIONAL,
)
from newcoin_trader.research.live_paper_config import parse_duration as parse_lp_duration
from newcoin_trader.research.live_paper_config import validate_live_paper_bounds
from newcoin_trader.services.event_study import (
    EventStudyService,
    parse_cli_datetime,
    resolve_grid,
    split_duration_option,
)
from newcoin_trader.services.executable_backtest import (
    ExecutableBacktestService,
    load_phase4_decision_records,
)
from newcoin_trader.services.feature_research import FeatureResearchService, resolve_decision_delay
from newcoin_trader.services.ingestion import (
    INGEST_BINANCE_LIMIT_MAX,
    INGEST_CONTROL_MIN,
    INGEST_GECKO_OHLCV_LIMIT_MAX,
    INGEST_RAYDIUM_PAGE_MAX,
    INGEST_RAYDIUM_PAGE_SIZE_MAX,
    CollectOnceResult,
    PollController,
    validate_ingest_market_history_controls,
)
from newcoin_trader.services.live_paper import LivePaperService, load_replay_events
from newcoin_trader.services.wiring import (
    build_ingestion_service,
    build_market_history_service,
    open_live_stack,
    open_research_db_stack,
)

app = typer.Typer(
    no_args_is_help=True,
    add_completion=False,
    help="Paper-only newcoin research CLI. Live trading is forbidden.",
)


@app.callback()
def main_callback() -> None:
    """Root callback keeps Typer from collapsing a single subcommand."""
    configure_logging()


@app.command("smoke-offline")
def smoke_offline(
    output_dir: Path = typer.Option(Path("artifacts"), help="Directory for JSON/CSV artifacts"),
) -> None:
    """Run the fixtures-only research + paper simulation path (no network, no DB)."""
    code = run_offline_smoke(output_dir=output_dir)
    raise typer.Exit(code)


@app.command("ingest-market-history")
def ingest_market_history(
    binance_symbol: str | None = typer.Option(None, help="Binance symbol scope (e.g. NEWUSDT)"),
    binance_interval: str = typer.Option("1h", help="Binance kline interval"),
    binance_start_ms: int | None = typer.Option(None, help="Binance startTime (ms)"),
    binance_end_ms: int | None = typer.Option(None, help="Binance endTime (ms)"),
    binance_limit: int = typer.Option(
        100,
        help=(f"Bounded Binance klines/aggTrades/trades page size ({INGEST_CONTROL_MIN}–{INGEST_BINANCE_LIMIT_MAX})"),
    ),
    include_binance_recent_trades: bool = typer.Option(
        False,
        help="Also ingest /api/v3/trades (namespaced separately from aggTrades)",
    ),
    raydium_page_size: int | None = typer.Option(
        None,
        help=(f"Raydium pools page size; omit to skip ({INGEST_CONTROL_MIN}–{INGEST_RAYDIUM_PAGE_SIZE_MAX})"),
    ),
    raydium_page: int = typer.Option(
        1,
        help=f"Raydium pools page ({INGEST_CONTROL_MIN}–{INGEST_RAYDIUM_PAGE_MAX})",
    ),
    gecko_network: str | None = typer.Option(None, help="GeckoTerminal network (e.g. solana)"),
    gecko_pool: str | None = typer.Option(None, help="GeckoTerminal pool address"),
    gecko_ohlcv_limit: int = typer.Option(
        100,
        help=(f"Bounded Gecko OHLCV limit ({INGEST_CONTROL_MIN}–{INGEST_GECKO_OHLCV_LIMIT_MAX})"),
    ),
) -> None:
    """Fetch and persist bounded market history (GET-only; research/paper path).

    Request/page/record controls are strictly positive integers with conservative
    upper bounds, validated before any HTTP or database work:
    binance_limit 1–1000, raydium_page 1–100, raydium_page_size 1–100 (omit to skip),
    gecko_ohlcv_limit 1–1000.
    """
    try:
        validate_ingest_market_history_controls(
            binance_limit=binance_limit,
            raydium_page=raydium_page,
            raydium_page_size=raydium_page_size,
            gecko_ohlcv_limit=gecko_ohlcv_limit,
        )
    except ConfigError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc

    async def _run() -> None:
        settings = load_settings()
        async with open_live_stack(settings) as stack:
            async with stack.session_factory() as session:
                service = build_market_history_service(
                    settings=settings,
                    http=stack.http,
                    session=session,
                )
                result = await service.ingest_market_history(
                    binance_symbol=binance_symbol,
                    binance_interval=binance_interval,
                    binance_start_ms=binance_start_ms,
                    binance_end_ms=binance_end_ms,
                    binance_limit=binance_limit,
                    include_binance_recent_trades=include_binance_recent_trades,
                    raydium_page=raydium_page,
                    raydium_page_size=raydium_page_size,
                    gecko_network=gecko_network,
                    gecko_pool=gecko_pool,
                    gecko_ohlcv_limit=gecko_ohlcv_limit,
                )
                await session.commit()
                typer.echo(
                    "ingest-market-history complete: "
                    f"snapshots={result.snapshots} trades={result.trades} "
                    f"pools={result.pools} by_source={result.by_source}"
                )

    try:
        asyncio.run(_run())
    except ConfigError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc


@app.command("collect-once")
def collect_once(
    birdeye_limit: int = typer.Option(10, help="Birdeye page size for tokens/pairs (1-20)"),
    meme_platform_enabled: bool = typer.Option(
        True,
        help="Include meme-platform listings in Birdeye token discovery",
    ),
) -> None:
    """One bounded discovery cycle: Binance listings + Birdeye Solana tokens/pairs → DB.

    Requires DATABASE_URL and BIRDEYE_API_KEY. Uses public GET endpoints only.
    """

    async def _run() -> None:
        settings = load_settings()
        async with open_live_stack(settings) as stack:
            async with stack.session_factory() as session:
                service = build_ingestion_service(
                    settings=settings,
                    http=stack.http,
                    session=session,
                )
                result = await service.collect_once(
                    birdeye_limit=birdeye_limit,
                    meme_platform_enabled=meme_platform_enabled,
                )
                await session.commit()
                typer.echo(
                    "collect-once complete: "
                    f"discovered={result.discovered} upserted={result.upserted} "
                    f"binance={result.binance_count} "
                    f"birdeye_tokens={result.birdeye_token_count} "
                    f"birdeye_pairs={result.birdeye_pair_count}"
                )

    try:
        asyncio.run(_run())
    except ConfigError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc


@app.command("poll")
def poll(
    interval: float = typer.Option(60.0, help="Seconds between collect-once cycles"),
    max_iterations: int | None = typer.Option(
        None,
        help="Stop after N cycles (default: run until interrupted)",
    ),
    birdeye_limit: int = typer.Option(10, help="Birdeye page size for tokens/pairs (1-20)"),
) -> None:
    """Poll collect-once on an interval. Ctrl+C stops safely.

    Requires DATABASE_URL and BIRDEYE_API_KEY. GET/read-only collectors only.
    """

    async def _run() -> None:
        settings = load_settings()
        controller = PollController(interval_seconds=interval, max_iterations=max_iterations)
        async with open_live_stack(settings) as stack:

            async def one_cycle() -> CollectOnceResult:
                async with stack.session_factory() as session:
                    service = build_ingestion_service(
                        settings=settings,
                        http=stack.http,
                        session=session,
                    )
                    result = await service.collect_once(birdeye_limit=birdeye_limit)
                    await session.commit()
                    return result

            results = await controller.run(one_cycle)
            for idx, result in enumerate(results, start=1):
                typer.echo(f"poll cycle={idx} discovered={result.discovered} upserted={result.upserted}")

    try:
        asyncio.run(_run())
    except ConfigError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    except KeyboardInterrupt:
        typer.echo("poll stopped")
        raise typer.Exit(0) from None


@app.command("event-study")
def event_study(
    venue: str = typer.Option(..., help="Venue filter (required; venues are never pooled)"),
    start: str = typer.Option(..., help="Inclusive UTC ISO start for listing events"),
    end: str = typer.Option(..., help="Exclusive UTC ISO end for listing events"),
    max_events: int = typer.Option(
        ...,
        help=f"Hard cap on listing events ({MAX_EVENTS_MIN}–{MAX_EVENTS_MAX}); required",
    ),
    output_dir: Path = typer.Option(
        Path("artifacts/event_study"),
        help="Directory for JSON/CSV/Markdown research artifacts",
    ),
    entry_delays: str | None = typer.Option(
        None,
        help="Comma-separated entry delays (default: 10s,30s,1m,2m,5m,10m,15m,30m)",
    ),
    holding_periods: str | None = typer.Option(
        None,
        help="Comma-separated holdings (default: 1m,5m,15m,30m,1h,2h,4h,24h)",
    ),
) -> None:
    """Bounded descriptive Phase 3 event-study (gross market returns; not PnL).

    Reads existing PostgreSQL token/snapshot rows only. Requires DATABASE_URL.
    Empty databases emit a valid zero-sample report rather than failing.
    """
    try:
        start_dt = parse_cli_datetime(start)
        end_dt = parse_cli_datetime(end)
        delay_specs = split_duration_option(entry_delays)
        holding_specs = split_duration_option(holding_periods)
        entry_delay_values, holding_values = resolve_grid(delay_specs, holding_specs)
        validate_event_study_bounds(
            start=start_dt,
            end=end_dt,
            max_events=max_events,
            entry_delays=entry_delay_values,
            holding_periods=holding_values,
        )
        if not venue.strip():
            raise ConfigError("venue is required")
    except ConfigError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    except ValueError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc

    async def _run() -> None:
        settings = load_settings()
        async with open_research_db_stack(settings) as stack:
            async with stack.session_factory() as session:
                service = EventStudyService(session)
                report, paths = await service.run(
                    venue=venue,
                    start=start_dt,
                    end=end_dt,
                    max_events=max_events,
                    output_dir=output_dir,
                    entry_delay_specs=delay_specs,
                    holding_period_specs=holding_specs,
                )
                typer.echo(
                    "event-study complete: "
                    f"run_id={report.meta.run_id} events={report.meta.event_count} "
                    f"cells={len(report.cell_results)} "
                    f"json={paths['json']} csv={paths['csv']} md={paths['markdown']}"
                )

    try:
        asyncio.run(_run())
    except ConfigError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    except ValueError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc


@app.command("feature-research")
def feature_research(
    venue: str = typer.Option(..., help="Venue filter (required; venues are never pooled)"),
    start: str = typer.Option(..., help="Inclusive UTC ISO start for listing events"),
    end: str = typer.Option(..., help="Exclusive UTC ISO end for listing events"),
    max_events: int = typer.Option(
        ...,
        help=f"Hard cap on listing events ({MAX_EVENTS_MIN}–{MAX_EVENTS_MAX}); required",
    ),
    output_dir: Path = typer.Option(
        Path("artifacts/feature_research"),
        help="Directory for JSON/CSV/Markdown research artifacts",
    ),
    decision_delay: str = typer.Option(
        "1m",
        help="Decision/entry delay after source_event_time (e.g. 1m, 5m)",
    ),
    windows: str | None = typer.Option(
        None,
        help="Comma-separated feature windows (allowed: 1m,5m,15m,30m; default all four)",
    ),
    min_sample: int = typer.Option(
        DEFAULT_MIN_SAMPLE,
        help="Minimum samples for univariate/rule stats",
    ),
    split: str = typer.Option(
        "0.6,0.2,0.2",
        help="Chronological train,validation,test ratios summing to 1",
    ),
    walk_forward_folds: int = typer.Option(
        DEFAULT_WALK_FORWARD_FOLDS,
        help="Bounded rolling walk-forward fold count",
    ),
    max_rules: int = typer.Option(
        DEFAULT_MAX_RULES,
        help="Cap on candidate interpretable rules",
    ),
    max_rule_conditions: int = typer.Option(
        DEFAULT_MAX_RULE_CONDITIONS,
        help=f"Max conditions per rule (1–{MAX_RULE_CONDITIONS})",
    ),
) -> None:
    """Bounded Phase 4 decision-time feature research (gross labels; not execution).

    Reads existing PostgreSQL token/snapshot/trade rows only. Requires DATABASE_URL.
    Features use inputs at-or-before decision_time; future labels stay separated.
    """
    try:
        start_dt = parse_cli_datetime(start)
        end_dt = parse_cli_datetime(end)
        delay = resolve_decision_delay(decision_delay)
        window_specs = split_duration_option(windows)
        window_values = parse_duration_list(window_specs, default=FR_DEFAULT_WINDOWS)
        split_ratios = parse_split_ratios(split)
        validate_feature_research_bounds(
            start=start_dt,
            end=end_dt,
            max_events=max_events,
            decision_delay=delay,
            windows=window_values,
            min_sample=min_sample,
            split_ratios=split_ratios,
            walk_forward_folds=walk_forward_folds,
            max_rules=max_rules,
            max_rule_conditions=max_rule_conditions,
        )
        if not venue.strip():
            raise ConfigError("venue is required")
    except ConfigError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    except ValueError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc

    async def _run() -> None:
        settings = load_settings()
        async with open_research_db_stack(settings) as stack:
            async with stack.session_factory() as session:
                service = FeatureResearchService(session)
                report, paths = await service.run(
                    venue=venue,
                    start=start_dt,
                    end=end_dt,
                    max_events=max_events,
                    output_dir=output_dir,
                    decision_delay=delay,
                    window_specs=window_specs,
                    min_sample=min_sample,
                    split_ratios=split_ratios,
                    walk_forward_folds=walk_forward_folds,
                    max_rules=max_rules,
                    max_rule_conditions=max_rule_conditions,
                )
                typer.echo(
                    "feature-research complete: "
                    f"run_id={report.meta.run_id} records={report.meta.record_count} "
                    f"json={paths['json']} csv={paths['csv']} md={paths['markdown']}"
                )

    try:
        asyncio.run(_run())
    except ConfigError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    except ValueError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc


def _parse_rule_condition(spec: str) -> RuleCondition:
    parts = [p.strip() for p in spec.split(":")]
    if len(parts) != 3:
        raise ConfigError("rule condition must be feature:op:threshold (e.g. age_seconds:gte:0)")
    feature_name, op, threshold = parts
    if op not in {"gt", "gte", "lt", "lte", "eq"}:
        raise ConfigError(f"unsupported rule condition op: {op!r}")
    return RuleCondition(
        feature_name=feature_name,
        op=op,
        threshold=require_finite_decimal(threshold, name="rule_threshold"),
    )


@app.command("executable-backtest")
def executable_backtest(
    venue: str = typer.Option(..., help="Venue filter (required; venues are never pooled)"),
    start: str = typer.Option(..., help="Inclusive UTC ISO start for listing events"),
    end: str = typer.Option(..., help="Exclusive UTC ISO end for listing events"),
    max_events: int = typer.Option(
        ...,
        help=f"Hard cap on listing events ({EB_MAX_EVENTS_MIN}–{EB_MAX_EVENTS_MAX}); required",
    ),
    max_trades: int = typer.Option(
        ...,
        help="Hard cap on historical trade rows read for execution; required",
    ),
    max_execution_inputs: int = typer.Option(
        ...,
        help="Hard cap on price/liquidity observations read for execution; required",
    ),
    output_dir: Path = typer.Option(
        ...,
        help="Directory for JSON/CSV/Markdown executable-backtest artifacts; required",
    ),
    frozen_rule_id: str = typer.Option(
        ...,
        help="Frozen Phase 4 candidate rule_id (required; no rediscovery)",
    ),
    phase4_config_id: str = typer.Option(
        ...,
        help="Frozen Phase 4 config_id provenance (required)",
    ),
    rule_condition: list[str] | None = typer.Option(
        None,
        help="Frozen rule condition feature:op:threshold (repeatable; default age_seconds:gte:0)",
    ),
    split_label: str = typer.Option("test", help="Frozen split label provenance"),
    fold_index: int | None = typer.Option(None, help="Optional frozen walk-forward fold index"),
    latencies: str | None = typer.Option(
        None,
        help="Comma-separated entry fill latencies (default: 0s,10s,30s,1m); 0s allowed",
    ),
    holding_periods: str | None = typer.Option(
        None,
        help="Comma-separated holdings (default: 1m,5m,15m)",
    ),
    position_notionals: str | None = typer.Option(
        None,
        help="Comma-separated position notionals (default: 10,100,1000)",
    ),
    max_participation: str = typer.Option(
        str(DEFAULT_MAX_PARTICIPATION),
        help="Max fraction of observed liquidity per fill",
    ),
    assumed_fee_bps: str | None = typer.Option(
        None,
        help="Assumed venue fee in bps when historical fees unavailable",
    ),
    phase4_records_json: Path | None = typer.Option(
        None,
        help="Optional Phase 4 feature_research_summary.json with frozen decision records",
    ),
) -> None:
    """Bounded Phase 5 historical executable backtest (research simulation only).

    Reads existing PostgreSQL token/snapshot/trade rows only. Never sends orders,
    never rediscovers Phase 4 rules, and never claims AMM-exact or live-book fills.
    Requires DATABASE_URL. Historical depth is not persisted — modeled fallback used.
    """
    try:
        start_dt = parse_cli_datetime(start)
        end_dt = parse_cli_datetime(end)
        latency_specs = split_duration_option(latencies)
        holding_specs = split_duration_option(holding_periods)
        latency_values = parse_latency_list(latency_specs, default=EB_DEFAULT_LATENCIES)
        holding_values = parse_eb_duration_list(holding_specs, default=EB_DEFAULT_HOLDINGS)
        notionals = parse_decimal_list(position_notionals, default=EB_DEFAULT_POSITIONS)
        participation = require_finite_decimal(max_participation, name="max_participation")
        fee_override = (
            require_finite_decimal(assumed_fee_bps, name="assumed_fee_bps") if assumed_fee_bps is not None else None
        )
        fee = assumed_fee_for_venue(venue, fee_override)
        cond_specs = rule_condition if rule_condition else ["age_seconds:gte:0"]
        conditions = tuple(_parse_rule_condition(spec) for spec in cond_specs)
        if not frozen_rule_id.strip() or not phase4_config_id.strip():
            raise ConfigError("frozen Phase 4 rule identity is required (no rediscovery)")
        identity = FrozenCandidateIdentity(
            rule_id=frozen_rule_id.strip(),
            conditions=conditions,
            human_readable=" AND ".join(f"{c.feature_name} {c.op} {c.threshold}" for c in conditions),
            phase4_config_id=phase4_config_id.strip(),
            split_label=split_label,
            fold_index=fold_index,
            provenance={"source": "cli_frozen_phase4"},
        )
        validate_executable_backtest_bounds(
            start=start_dt,
            end=end_dt,
            max_events=max_events,
            max_trades=max_trades,
            max_execution_inputs=max_execution_inputs,
            latencies=latency_values,
            holding_periods=holding_values,
            position_notionals=notionals,
            max_participation=participation,
            assumed_fee_bps=fee,
        )
        if not venue.strip():
            raise ConfigError("venue is required")
        decision_records = (
            load_phase4_decision_records(phase4_records_json) if phase4_records_json is not None else None
        )
    except ConfigError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    except ValueError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc

    async def _run() -> None:
        settings = load_settings()
        async with open_research_db_stack(settings) as stack:
            async with stack.session_factory() as session:
                service = ExecutableBacktestService(session)
                report, paths = await service.run(
                    venue=venue,
                    start=start_dt,
                    end=end_dt,
                    max_events=max_events,
                    max_trades=max_trades,
                    max_execution_inputs=max_execution_inputs,
                    output_dir=output_dir,
                    frozen_rules=(identity,),
                    latency_specs=latency_specs,
                    holding_specs=holding_specs,
                    position_notionals=notionals,
                    max_participation=participation,
                    assumed_fee_bps=fee,
                    decision_records=decision_records,
                )
                typer.echo(
                    "executable-backtest complete: "
                    f"run_id={report.meta.run_id} trades={report.meta.trade_count} "
                    f"json={paths['json']} csv={paths['csv']} md={paths['markdown']}"
                )

    try:
        asyncio.run(_run())
    except ConfigError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    except ValueError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc


@app.command("live-paper")
def live_paper(
    venue: str = typer.Option(..., help="Venue filter (required; venues are never pooled)"),
    duration: str = typer.Option(..., help="Bounded session duration (e.g. 1h); required, no indefinite daemon"),
    max_events: int = typer.Option(..., help="Hard cap on replay/feed events; required"),
    max_signals: int = typer.Option(..., help="Hard cap on accepted paper signals; required"),
    max_trades: int = typer.Option(
        ...,
        help="Hard cap on paper positions/round-trips (not fills); required",
    ),
    queue_capacity: int = typer.Option(..., help="Bounded in-memory event queue capacity; required"),
    frozen_rule_id: str = typer.Option(..., help="Frozen Phase 4 candidate rule_id (required)"),
    phase4_config_id: str = typer.Option(..., help="Frozen Phase 4 config_id provenance (required)"),
    paper_starting_cash: str = typer.Option(..., help="Paper starting cash (required; not real capital)"),
    output_dir: Path = typer.Option(..., help="Directory for JSON/CSV/Markdown live-paper artifacts; required"),
    session_start: str = typer.Option(..., help="UTC ISO session start clock"),
    mode: str = typer.Option(
        "replay",
        help="Mutually exclusive feed mode: replay (offline JSON) or prospective (bounded public GET)",
    ),
    replay_path: Path | None = typer.Option(
        None,
        help="Offline replay JSON with timestamped events (required for --mode replay)",
    ),
    poll_interval: str | None = typer.Option(
        None,
        help="Prospective poll interval (e.g. 5s); required for --mode prospective",
    ),
    symbol: str | None = typer.Option(
        None,
        help="Prospective Binance Spot symbol (required for --mode prospective)",
    ),
    max_polls: int | None = typer.Option(
        None,
        help="Prospective hard cap on polls (required for --mode prospective)",
    ),
    max_observations_per_token: int | None = typer.Option(
        None,
        help="Prospective hard cap on observations per token (required for --mode prospective)",
    ),
    max_total_observations: int | None = typer.Option(
        None,
        help="Prospective hard cap on total market observations (required for --mode prospective)",
    ),
    rule_condition: list[str] | None = typer.Option(
        None,
        help="Frozen rule condition feature:op:threshold (repeatable; default age_source_event_seconds:gte:0)",
    ),
    split_label: str = typer.Option("test", help="Frozen split label provenance"),
    fold_index: int | None = typer.Option(None, help="Optional frozen walk-forward fold index"),
    holding_period: str = typer.Option("5m", help="Frozen exit holding period"),
    position_notional: str = typer.Option(str(LP_DEFAULT_NOTIONAL), help="Paper position notional"),
    assumed_fee_bps: str | None = typer.Option(None, help="Assumed venue fee bps"),
) -> None:
    """Bounded Phase 6/6.5 live-paper (paper-only; never sends orders).

    Modes are mutually exclusive:
    - replay: requires --replay-path (offline JSON; no HTTP in LivePaperService)
    - prospective: requires --poll-interval, --symbol, --max-polls,
      --max-observations-per-token, --max-total-observations; Binance public Spot only
    """
    try:
        mode_norm = mode.strip().lower()
        if mode_norm not in {"replay", "prospective"}:
            raise ConfigError("live-paper --mode must be 'replay' or 'prospective'")
        if mode_norm == "replay" and replay_path is None:
            raise ConfigError("live-paper --mode replay requires --replay-path")
        if mode_norm == "prospective" and replay_path is not None:
            raise ConfigError("live-paper --mode prospective cannot be combined with --replay-path")
        if mode_norm == "replay" and any(
            value is not None
            for value in (poll_interval, symbol, max_polls, max_observations_per_token, max_total_observations)
        ):
            raise ConfigError("live-paper --mode replay does not accept prospective-only options")

        start_dt = parse_cli_datetime(session_start)
        duration_td = parse_lp_duration(duration)
        holding_td = parse_lp_duration(holding_period)
        cash = require_finite_decimal(paper_starting_cash, name="paper_starting_cash")
        notional = require_finite_decimal(position_notional, name="position_notional")
        fee = require_finite_decimal(assumed_fee_bps, name="assumed_fee_bps") if assumed_fee_bps is not None else None
        cond_specs = rule_condition if rule_condition else ["age_source_event_seconds:gte:0"]
        conditions = tuple(_parse_rule_condition(spec) for spec in cond_specs)
        if not frozen_rule_id.strip() or not phase4_config_id.strip():
            raise ConfigError("frozen Phase 4 rule identity is required (no rediscovery)")
        if not venue.strip():
            raise ConfigError("venue is required")
        identity = FrozenCandidateIdentity(
            rule_id=frozen_rule_id.strip(),
            conditions=conditions,
            human_readable=" AND ".join(f"{c.feature_name} {c.op} {c.threshold}" for c in conditions),
            phase4_config_id=phase4_config_id.strip(),
            split_label=split_label,
            fold_index=fold_index,
            provenance={"source": "cli_frozen_phase4"},
        )
        validate_live_paper_bounds(
            duration=duration_td,
            max_events=max_events,
            max_signals=max_signals,
            max_trades=max_trades,
            queue_capacity=queue_capacity,
            starting_cash=cash,
            position_notional=notional,
            holding_period=holding_td,
        )

        if mode_norm == "prospective":
            from newcoin_trader.research.prospective_capabilities import prospective_venue_supported
            from newcoin_trader.research.prospective_feed import validate_prospective_feed_bounds

            if not prospective_venue_supported(venue):
                raise ConfigError(
                    f"unsupported prospective venue: {venue!r} "
                    "(Phase 6.5 supports binance only; no silent replay fallback)"
                )
            if poll_interval is None:
                raise ConfigError("live-paper --mode prospective requires --poll-interval")
            if symbol is None or not symbol.strip():
                raise ConfigError("live-paper --mode prospective requires --symbol")
            if max_polls is None:
                raise ConfigError("live-paper --mode prospective requires --max-polls")
            if max_observations_per_token is None:
                raise ConfigError("live-paper --mode prospective requires --max-observations-per-token")
            if max_total_observations is None:
                raise ConfigError("live-paper --mode prospective requires --max-total-observations")
            poll_td = parse_lp_duration(poll_interval)
            validate_prospective_feed_bounds(
                poll_interval=poll_td,
                duration=duration_td,
                max_polls=max_polls,
                max_events=max_events,
                max_observations_per_token=max_observations_per_token,
                max_total_observations=max_total_observations,
                queue_capacity=queue_capacity,
            )
            events = None
        else:
            assert replay_path is not None
            events = load_replay_events(replay_path)
            poll_td = None
    except ConfigError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    except ValueError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc

    async def _run() -> None:
        service = LivePaperService()
        if mode_norm == "prospective":
            from datetime import UTC, datetime

            from newcoin_trader.collectors.binance.client import BinanceClient
            from newcoin_trader.collectors.http import AsyncHttpClient
            from newcoin_trader.research.prospective_feed import build_prospective_feed

            assert poll_td is not None
            assert symbol is not None
            assert max_polls is not None
            assert max_observations_per_token is not None
            assert max_total_observations is not None
            settings = load_settings()
            http = AsyncHttpClient(
                timeout_seconds=settings.http_timeout_seconds,
                max_attempts=settings.http_max_attempts,
                backoff_seconds=settings.http_backoff_seconds,
                rate_limit_per_second=settings.http_rate_limit_per_second,
            )
            try:
                feed = build_prospective_feed(
                    venue=venue,
                    client=BinanceClient(http=http, base_url=settings.binance_base_url),
                    now=lambda: datetime.now(UTC),
                    symbol=symbol,
                    poll_interval=poll_td,
                    duration=duration_td,
                    max_polls=max_polls,
                    max_events=max_events,
                    max_observations_per_token=max_observations_per_token,
                    max_total_observations=max_total_observations,
                    queue_capacity=queue_capacity,
                )
                report, paths = await service.run_prospective(
                    feed=feed,
                    identity=identity,
                    venue=venue,
                    duration=duration_td,
                    max_events=max_events,
                    max_signals=max_signals,
                    max_trades=max_trades,
                    queue_capacity=queue_capacity,
                    starting_cash=cash,
                    output_dir=output_dir,
                    session_start=start_dt,
                    position_notional=notional,
                    holding_period=holding_td,
                    assumed_fee_bps=fee,
                )
            finally:
                await http.aclose()
        else:
            assert events is not None
            report, paths = await service.run_replay(
                events=events,
                identity=identity,
                venue=venue,
                duration=duration_td,
                max_events=max_events,
                max_signals=max_signals,
                max_trades=max_trades,
                queue_capacity=queue_capacity,
                starting_cash=cash,
                output_dir=output_dir,
                session_start=start_dt,
                position_notional=notional,
                holding_period=holding_td,
                assumed_fee_bps=fee,
            )
        typer.echo(
            "live-paper complete: "
            f"session_id={report.meta.session_id} signals={report.meta.signal_count} "
            f"json={paths['json']} csv={paths['csv']} md={paths['markdown']}"
        )

    try:
        asyncio.run(_run())
    except ConfigError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    except ValueError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc


def main() -> None:
    app()


if __name__ == "__main__":
    main()
