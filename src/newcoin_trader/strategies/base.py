"""Deterministic Strategy protocol. No I/O. No LLM."""

from __future__ import annotations

from typing import Protocol

from newcoin_trader.domain.strategy import Signal, StrategyContext


class Strategy(Protocol):
    name: str
    version: str

    def generate(self, ctx: StrategyContext) -> list[Signal]:
        """Deterministic. Same context → same signals."""
        ...
