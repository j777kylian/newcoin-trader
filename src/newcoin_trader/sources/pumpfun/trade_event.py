"""Pump bonding-curve trade-event extraction: authoritative executed SOL/token amounts.

Layout of the inner TradeEvent (anchor event, discriminator ``e445a52e51cb9a1d``),
empirically pinned against finalized getTransaction responses:

- ``[0:8]``    discriminator
- ``[16:48]``  mint pubkey
- ``[48:56]``  u64 LE sol_amount (lamports moved by the trade, fees excluded)
- ``[56:64]``  u64 LE token_amount (raw, mint decimals)
- ``[64]``     is_buy flag (1 = buy)
- ``[65:97]``  user pubkey
- ``[97:105]`` i64 LE unix timestamp (equals finalized blockTime)

The trailing reserve fields are program-version-specific and are not consumed.
"""

from __future__ import annotations

import struct
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict

from newcoin_trader.sources.pumpfun.evidence import PUMP_PROGRAM_ADDRESS, _instruction_data_hex

TRADE_EVENT_DISCRIMINATOR = "e445a52e51cb9a1d"
EVENT_MIN_LENGTH = 105
_MIN_TS = 1_500_000_000
_MAX_TS = 2_100_000_000


class PumpTradeEventFact(BaseModel):
    """Authoritative per-trade amounts extracted from the Pump TradeEvent."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    inner_instruction_index: int
    is_buy: bool
    sol_lamports: int
    token_amount: int
    user: str
    event_time: int
    event_discriminator: str = TRADE_EVENT_DISCRIMINATOR

    @classmethod
    def from_payload(cls, *, inner_instruction_index: int, payload: bytes) -> PumpTradeEventFact:
        if inner_instruction_index < 0:
            raise ValueError("inner instruction coordinate must be a non-negative integer")
        if len(payload) < EVENT_MIN_LENGTH:
            raise ValueError("trade event payload is truncated")
        sol = struct.unpack_from("<Q", payload, 48)[0]
        token = struct.unpack_from("<Q", payload, 56)[0]
        is_buy = payload[64]
        user_raw = payload[65:97]
        ts = struct.unpack_from("<q", payload, 97)[0]
        if is_buy not in (0, 1):
            raise ValueError("trade event is_buy flag is invalid")
        if not _MIN_TS <= ts <= _MAX_TS:
            raise ValueError("trade event timestamp is out of range")
        if sol == 0 or token == 0:
            raise ValueError("trade event amounts must be positive")
        return cls(
            inner_instruction_index=inner_instruction_index,
            is_buy=is_buy == 1,
            sol_lamports=sol,
            token_amount=token,
            user=_encode_pubkey(user_raw),
            event_time=ts,
        )


def extract_pump_trade_events(meta: Mapping[str, Any]) -> tuple[tuple[int, PumpTradeEventFact], ...]:
    """Extract (outer instruction index, TradeEvent) pairs from finalized getTransaction meta.

    Each inner-instruction group carries the ``index`` of the outer instruction it
    belongs to, so a multi-trade transaction binds each TradeEvent to its own
    outer buy/sell instruction rather than reusing the first event.
    """
    facts: list[tuple[int, PumpTradeEventFact]] = []
    inner_instructions = meta.get("innerInstructions", ())
    if not isinstance(inner_instructions, list):
        return ()
    for group in inner_instructions:
        if not isinstance(group, Mapping):
            continue
        outer_index = group.get("index")
        if isinstance(outer_index, bool) or not isinstance(outer_index, int) or outer_index < 0:
            continue
        instructions = group.get("instructions", ())
        if not isinstance(instructions, list):
            continue
        for position, instruction in enumerate(instructions):
            if not isinstance(instruction, Mapping) or instruction.get("programId") != PUMP_PROGRAM_ADDRESS:
                continue
            payload_hex = _instruction_data_hex(instruction.get("data"))
            if payload_hex is None or not payload_hex.startswith(TRADE_EVENT_DISCRIMINATOR):
                continue
            try:
                facts.append(
                    (
                        outer_index,
                        PumpTradeEventFact.from_payload(
                            inner_instruction_index=position, payload=bytes.fromhex(payload_hex)
                        ),
                    )
                )
            except ValueError:
                # Version-tail or malformed payloads fail closed per event, not per transaction.
                continue
    return tuple(facts)


_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _encode_pubkey(raw: bytes) -> str:
    if len(raw) != 32:
        raise ValueError("pubkey payload must be 32 bytes")
    number = int.from_bytes(raw, "big")
    encoded = ""
    while number:
        number, remainder = divmod(number, 58)
        encoded = _BASE58_ALPHABET[remainder] + encoded
    leading_zeros = len(raw) - len(raw.lstrip(b"\0"))
    return "1" * leading_zeros + encoded
