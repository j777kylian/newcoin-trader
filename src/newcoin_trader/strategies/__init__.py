"""Code strategy registry. No LLM adapters."""

from __future__ import annotations

from collections.abc import Callable

from newcoin_trader.errors import ConfigError
from newcoin_trader.strategies.base import Strategy
from newcoin_trader.strategies.listing_momentum import ListingMomentumStrategy

_REGISTRY: dict[str, Callable[[], Strategy]] = {
    ListingMomentumStrategy.name: ListingMomentumStrategy,
}


def get_strategy(name: str) -> Strategy:
    try:
        factory = _REGISTRY[name]
    except KeyError as exc:
        raise ConfigError(f"unknown strategy: {name}") from exc
    return factory()


def list_strategies() -> list[str]:
    return sorted(_REGISTRY)
