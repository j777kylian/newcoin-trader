"""Typer CLI entrypoint with explicit subcommands (no single-command collapse)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import typer

from newcoin_trader.config import load_settings
from newcoin_trader.demo import run_offline_smoke
from newcoin_trader.errors import ConfigError
from newcoin_trader.logging_setup import configure_logging
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
from newcoin_trader.services.wiring import (
    build_ingestion_service,
    build_market_history_service,
    open_live_stack,
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


def main() -> None:
    app()


if __name__ == "__main__":
    main()
