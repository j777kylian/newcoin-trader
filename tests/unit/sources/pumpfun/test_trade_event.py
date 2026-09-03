"""Trust-boundary tests for Pump TradeEvent extraction."""

from __future__ import annotations

import pytest

from newcoin_trader.sources.pumpfun.trade_event import (
    TRADE_EVENT_DISCRIMINATOR,
    PumpTradeEventFact,
    extract_pump_trade_events,
)

MINT = "So11111111111111111111111111111111111111112"
USER = "4Nd1mBQtrMJVYVfKf2PJy9NZUZdTAsp7D4xWLs4gDB4T"


def _payload(*, sol: int, token: int, is_buy: bool, ts: int = 1_754_006_400) -> bytes:
    raw = bytearray(105)
    raw[0:8] = bytes.fromhex(TRADE_EVENT_DISCRIMINATOR)
    raw[16:48] = bytes(32)  # mint raw bytes irrelevant for extraction shape
    raw[48:56] = sol.to_bytes(8, "little")
    raw[56:64] = token.to_bytes(8, "little")
    raw[64] = 1 if is_buy else 0
    raw[65:97] = bytes(range(32))
    raw[97:105] = ts.to_bytes(8, "little", signed=True)
    return bytes(raw)


def _meta(payload: bytes) -> dict[str, object]:
    return {
        "innerInstructions": [
            {
                "index": 0,
                "instructions": [
                    {"programId": "other", "data": "aaaa"},
                    {"programId": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", "data": payload.hex()},
                ],
            }
        ]
    }


def test_trade_event_fact_extracts_authoritative_sol() -> None:
    fact = PumpTradeEventFact.from_payload(
        inner_instruction_index=1, payload=_payload(sol=9_442_556, token=59_495_627_565_763, is_buy=True)
    )
    assert fact.is_buy is True
    assert fact.sol_lamports == 9_442_556
    assert fact.token_amount == 59_495_627_565_763
    assert fact.event_time == 1_754_006_400
    assert fact.user == "1thX6LZfHDZZKUs92febYZhYRcXddmzfzF2NvTkPNE"


def test_trade_event_fact_rejects_zero_amount_and_bad_timestamp() -> None:
    with pytest.raises(ValueError, match="amounts must be positive"):
        PumpTradeEventFact.from_payload(inner_instruction_index=0, payload=_payload(sol=0, token=5, is_buy=False))
    with pytest.raises(ValueError, match="timestamp"):
        PumpTradeEventFact.from_payload(
            inner_instruction_index=0, payload=_payload(sol=5, token=5, is_buy=False, ts=99)
        )
    with pytest.raises(ValueError, match="truncated"):
        PumpTradeEventFact.from_payload(inner_instruction_index=0, payload=_payload(sol=5, token=5, is_buy=False)[:50])


def test_extract_events_skips_non_pump_and_non_trade_instructions() -> None:
    meta = {
        "innerInstructions": [
            {
                "index": 0,
                "instructions": [
                    {"programId": "other", "data": _payload(sol=1, token=1, is_buy=True).hex()},
                    {
                        "programId": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
                        "data": "66063d1201daebea40420f000000000000ca9a3b00000000",
                    },
                    {
                        "programId": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
                        "data": _payload(sol=7, token=9, is_buy=False).hex(),
                    },
                ],
            }
        ]
    }
    events = extract_pump_trade_events(meta)
    assert len(events) == 1
    assert events[0][1].sol_lamports == 7
    assert events[0][1].token_amount == 9
    assert events[0][1].is_buy is False


def test_extract_events_skips_malformed_trade_event_payload() -> None:
    truncated = _payload(sol=5, token=5, is_buy=True)[:50].hex()
    meta = {
        "innerInstructions": [
            {
                "index": 0,
                "instructions": [
                    {"programId": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", "data": truncated},
                    {
                        "programId": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
                        "data": _payload(sol=7, token=9, is_buy=False).hex(),
                    },
                ],
            }
        ]
    }
    events = extract_pump_trade_events(meta)
    assert len(events) == 1
    assert events[0][1].sol_lamports == 7


def test_extract_events_binds_outer_instruction_index() -> None:
    meta = {
        "innerInstructions": [
            {
                "index": 1,
                "instructions": [
                    {
                        "programId": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
                        "data": _payload(sol=7, token=9, is_buy=False).hex(),
                    },
                ],
            }
        ]
    }
    events = extract_pump_trade_events(meta)
    assert len(events) == 1
    assert events[0][0] == 1
    meta["innerInstructions"][0]["index"] = 0  # type: ignore[index]
    events = extract_pump_trade_events(meta)
    assert len(events) == 1
    assert events[0][0] == 0
    assert events[0][1].sol_lamports == 7


def test_extract_events_handles_missing_meta() -> None:
    assert extract_pump_trade_events({}) == ()
