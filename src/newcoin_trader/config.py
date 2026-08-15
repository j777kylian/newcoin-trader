"""Environment-only settings. A local .env file is never loaded."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from newcoin_trader.domain.numeric import require_finite_float


class Settings(BaseSettings):
    """Runtime configuration sourced exclusively from process environment."""

    model_config = SettingsConfigDict(
        env_file=None,
        extra="ignore",
        case_sensitive=False,
    )

    database_url: str = "postgresql+asyncpg://newcoin:newcoin@localhost:5432/newcoin"
    execution_mode: Literal["paper"] = "paper"

    http_timeout_seconds: float = 15.0
    http_max_attempts: int = 4
    http_backoff_seconds: float = 0.25
    http_rate_limit_per_second: float = 8.0

    binance_base_url: str = "https://api.binance.com"
    birdeye_base_url: str = "https://public-api.birdeye.so"
    birdeye_api_key: str = ""
    birdeye_chain: str = "solana"
    raydium_pool_base_url: str = "https://api-v3.raydium.io"
    raydium_quote_base_url: str = "https://transaction-v1.raydium.io"
    gecko_base_url: str = "https://api.geckoterminal.com/api/v2"

    paper_fee_bps: float = 10.0
    paper_slippage_bps: float = 25.0
    paper_max_fill_liquidity_fraction: float = 0.10

    risk_max_notional: float = 1000.0
    risk_max_position_size: float = 500.0
    risk_max_open_positions: int = 3
    risk_max_drawdown: float = 0.25
    risk_min_liquidity: float = 5000.0

    reports_dir: Path = Field(default=Path("artifacts"))
    log_level: str = "INFO"
    poll_interval_seconds: float = 60.0

    @field_validator("execution_mode")
    @classmethod
    def _paper_only(cls, value: str) -> str:
        if value != "paper":
            raise ValueError("EXECUTION_MODE must be 'paper'; live trading is forbidden")
        return value

    @field_validator(
        "http_timeout_seconds",
        "http_backoff_seconds",
        "http_rate_limit_per_second",
        "paper_fee_bps",
        "paper_slippage_bps",
        "paper_max_fill_liquidity_fraction",
        "risk_max_notional",
        "risk_max_position_size",
        "risk_max_drawdown",
        "risk_min_liquidity",
        "poll_interval_seconds",
    )
    @classmethod
    def _finite_floats(cls, value: float, info: object) -> float:
        name = getattr(info, "field_name", "value")
        return require_finite_float(value, name=str(name))

    @field_validator("paper_fee_bps", "paper_slippage_bps")
    @classmethod
    def _non_negative_bps(cls, value: float) -> float:
        if value < 0:
            raise ValueError("paper fee/slippage bps must be >= 0")
        return value

    @field_validator("paper_max_fill_liquidity_fraction")
    @classmethod
    def _liquidity_fraction(cls, value: float) -> float:
        if value <= 0 or value > 1:
            raise ValueError("paper_max_fill_liquidity_fraction must be in (0, 1]")
        return value


def load_settings() -> Settings:
    return Settings()
