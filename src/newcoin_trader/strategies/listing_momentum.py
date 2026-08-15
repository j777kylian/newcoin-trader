"""Simple listing-momentum strategy (deterministic, non-LLM)."""

from __future__ import annotations

from decimal import Decimal

from newcoin_trader.domain.enums import SignalKind
from newcoin_trader.domain.strategy import Signal, StrategyContext
from newcoin_trader.research.windows import without_lookahead


class ListingMomentumStrategy:
    name = "listing_momentum"
    version = "1.0.0"

    def generate(self, ctx: StrategyContext) -> list[Signal]:
        snaps = without_lookahead(ctx.snapshots, ctx.evaluation_time)
        snaps = tuple(s for s in snaps if s.timestamp >= ctx.listing_time)
        snaps = tuple(sorted(snaps, key=lambda s: s.timestamp))
        if len(snaps) < 2:
            return []
        first = snaps[0].price
        last = snaps[-1].price
        if first == 0:
            return []
        ret = (last - first) / first
        threshold = Decimal(str(ctx.parameters.get("momentum_threshold", "0.05")))
        exit_threshold = Decimal(str(ctx.parameters.get("exit_threshold", "-0.05")))
        qty = Decimal(str(ctx.parameters.get("qty", "1")))
        signals: list[Signal] = []
        if ret >= threshold:
            signals.append(
                Signal(
                    kind=SignalKind.BUY,
                    token_address=ctx.token_address,
                    timestamp=ctx.evaluation_time,
                    price=last,
                    qty=qty,
                    reason=f"listing_momentum_return={ret}",
                )
            )
        elif ret <= exit_threshold:
            signals.append(
                Signal(
                    kind=SignalKind.SELL,
                    token_address=ctx.token_address,
                    timestamp=ctx.evaluation_time,
                    price=last,
                    qty=qty,
                    reason=f"listing_momentum_exit_return={ret}",
                )
            )
        return signals
