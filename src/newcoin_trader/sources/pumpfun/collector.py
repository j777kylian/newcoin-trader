"""Bounded Pump bonding-curve trade collection: walk signatures, decode trades.

Collects per-curve trades from finalized Alchemy data:
- outer buy/sell instruction (decoder-named roles + instruction token amount)
- inner TradeEvent (authoritative executed SOL + token + side + user + event time)

Bounded by explicit signature-page and per-curve trade caps; completeness is
`RESEARCH_GRADE_BOUNDED_COMPLETENESS_V1` — provider-backend omission cannot be
independently ruled out and is reported as a limitation, never hidden.
"""

from __future__ import annotations

from typing import Any

from newcoin_trader.sources.pumpfun.evidence import (
    PUMP_PROGRAM_ADDRESS,
    _instruction_data_hex,
    decode_pump_trade_amount,
)
from newcoin_trader.sources.pumpfun.rpc import PumpRpcProvider
from newcoin_trader.sources.pumpfun.trade_event import extract_pump_trade_events

BUY_DISCRIMINATOR = "66063d1201daebea"
SELL_DISCRIMINATOR = "33e685a4017f83ad"


class PumpCurveTradeCollector:
    """Bounded finalized trade walker for one Pump bonding curve."""

    def __init__(self, provider: PumpRpcProvider, *, signature_page_limit: int, max_signatures: int) -> None:
        if signature_page_limit <= 0 or max_signatures <= 0:
            raise ValueError("collector bounds must be positive")
        self._provider = provider
        self._signature_page_limit = signature_page_limit
        self._max_signatures = max_signatures

    async def collect(self, bonding_curve: str) -> list[dict[str, Any]]:
        signatures: list[dict[str, Any]] = []
        before: str | None = None
        while len(signatures) < self._max_signatures:
            limit = min(self._signature_page_limit, self._max_signatures - len(signatures))
            params: dict[str, Any] = {"commitment": "finalized", "limit": limit}
            if before is not None:
                params["before"] = before
            page = await self._provider.call("getSignaturesForAddress", [bonding_curve, params], attempt_budget=[6])
            records = page.result if isinstance(page.result, list) else []
            if not records:
                break
            signatures.extend(record for record in records if isinstance(record, dict))
            before = str(records[-1].get("signature"))
            if len(records) < limit:
                break
        trades: list[dict[str, Any]] = []
        for record in signatures:
            if record.get("err") is not None:
                continue
            signature = record.get("signature")
            if not isinstance(signature, str):
                continue
            tx = await self._provider.call(
                "getTransaction",
                [signature, {"commitment": "finalized", "encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
                attempt_budget=[6],
            )
            if not isinstance(tx.result, dict):
                continue
            trades.extend(
                self._decode_transaction(signature=signature, slot=int(record.get("slot", 0)), response=tx.result)
            )
        return trades

    def _decode_transaction(self, *, signature: str, slot: int, response: dict[str, Any]) -> list[dict[str, Any]]:
        message = response.get("transaction", {}).get("message", {})
        meta = response.get("meta", {})
        events = dict(extract_pump_trade_events(meta))
        if not events:
            return []
        trades: list[dict[str, Any]] = []
        for instruction_index, instruction in enumerate(message.get("instructions", [])):
            if instruction.get("programId") != PUMP_PROGRAM_ADDRESS:
                continue
            hexdata = _instruction_data_hex(instruction.get("data"))
            if hexdata is None:
                continue
            if hexdata.startswith(BUY_DISCRIMINATOR):
                side = "buy"
            elif hexdata.startswith(SELL_DISCRIMINATOR):
                side = "sell"
            else:
                continue
            event = events.get(instruction_index)
            if event is None:
                # No TradeEvent bound to this outer instruction: fail closed, never reuse another's.
                continue
            token_amount = decode_pump_trade_amount(hexdata)
            if token_amount is None:
                continue
            accounts = instruction.get("accounts", [])
            if len(accounts) < 4:
                continue
            trades.append(
                {
                    "signature": signature,
                    "slot": slot,
                    "side": side,
                    "mint": accounts[2],
                    "bonding_curve": accounts[3],
                    "instruction_token_amount": token_amount,
                    "trade_event": event.model_dump(),
                }
            )
        return trades


async def collect_curve_trades(
    provider: PumpRpcProvider, bonding_curve: str, *, signature_page_limit: int = 100, max_signatures: int = 1000
) -> list[dict[str, Any]]:
    collector = PumpCurveTradeCollector(
        provider, signature_page_limit=signature_page_limit, max_signatures=max_signatures
    )
    return await collector.collect(bonding_curve)
