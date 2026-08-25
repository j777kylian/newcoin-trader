"""Decode Uniswap V3 Swap logs from raw EVM log dicts (no ABI libs)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from newcoin_trader.sources.base_uniswap_v3.contracts import CHAIN_ID, PROTOCOL_VERSION, SWAP_TOPIC
from newcoin_trader.sources.base_uniswap_v3.models import (
    SwapLogRecord,
    normalize_address,
    normalize_hex32,
    parse_hex_uint,
)


def _require_mapping(raw: object) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping):
        raise ValueError("raw log must be a mapping")
    return raw


def _require_topics(value: object) -> list[str]:
    if not isinstance(value, list) or len(value) != 3:
        raise ValueError("Swap requires exactly 3 topics")
    out: list[str] = []
    for i, topic in enumerate(value):
        if not isinstance(topic, str):
            raise ValueError(f"topic{i} must be a hex string")
        text = topic.strip().lower()
        if not text.startswith("0x"):
            raise ValueError(f"topic{i} must start with 0x")
        body = text[2:]
        if len(body) != 64 or any(c not in "0123456789abcdef" for c in body):
            raise ValueError(f"topic{i} malformed")
        out.append(text)
    return out


def _indexed_address(topic: str, *, field_name: str) -> str:
    body = topic[2:]
    if body[:24] != "0" * 24:
        raise ValueError(f"{field_name} indexed address padding malformed")
    return normalize_address("0x" + body[24:], field_name=field_name)


def _word(data_body: str, index: int) -> str:
    start = index * 64
    end = start + 64
    if end > len(data_body):
        raise ValueError("malformed ABI data")
    return data_body[start:end]


def _decode_int256(word: str) -> int:
    value = int(word, 16)
    if value >= (1 << 255):
        return value - (1 << 256)
    return value


def _decode_uint160(word: str) -> int:
    value = int(word, 16)
    if value >= (1 << 160):
        raise ValueError("sqrtPriceX96 exceeds uint160")
    return value


def _decode_uint128(word: str) -> int:
    value = int(word, 16)
    if value >= (1 << 128):
        raise ValueError("liquidity exceeds uint128")
    return value


def _decode_int24_word(word: str) -> int:
    value = int(word, 16)
    sign_bit = 1 << 23
    mask = (1 << 24) - 1
    low = value & mask
    high = value >> 24
    expected_neg = (1 << 232) - 1
    if high not in (0, expected_neg):
        raise ValueError("malformed int24 ABI word")
    if low & sign_bit:
        return low - (1 << 24)
    return low


def decode_swap_log(raw: object) -> SwapLogRecord:
    """Decode a pool Swap log into a frozen record."""
    mapping = _require_mapping(raw)
    required = (
        "address",
        "topics",
        "data",
        "blockNumber",
        "blockHash",
        "transactionHash",
        "transactionIndex",
        "logIndex",
    )
    for key in required:
        if key not in mapping:
            raise ValueError(f"missing raw log field: {key}")

    pool_address = normalize_address(mapping["address"], field_name="address")
    topics = _require_topics(mapping["topics"])
    if topics[0] != SWAP_TOPIC:
        raise ValueError("wrong Swap topic0")

    sender = _indexed_address(topics[1], field_name="sender")
    recipient = _indexed_address(topics[2], field_name="recipient")

    data = mapping["data"]
    if not isinstance(data, str) or not data.startswith("0x"):
        raise ValueError("data must be hex string")
    body = data[2:].lower()
    if len(body) != 320 or any(c not in "0123456789abcdef" for c in body):
        raise ValueError("malformed ABI data for Swap; expected exactly 5 words")

    amount0 = _decode_int256(_word(body, 0))
    amount1 = _decode_int256(_word(body, 1))
    sqrt_price_x96 = _decode_uint160(_word(body, 2))
    liquidity = _decode_uint128(_word(body, 3))
    tick = _decode_int24_word(_word(body, 4))

    return SwapLogRecord.model_validate(
        {
            "chain_id": CHAIN_ID,
            "protocol_version": PROTOCOL_VERSION,
            "pool_address": pool_address,
            "sender": sender,
            "recipient": recipient,
            "amount0": amount0,
            "amount1": amount1,
            "sqrt_price_x96": sqrt_price_x96,
            "liquidity": liquidity,
            "tick": tick,
            "block_number": parse_hex_uint(mapping["blockNumber"], field_name="blockNumber"),
            "block_hash": normalize_hex32(mapping["blockHash"], field_name="blockHash"),
            "transaction_hash": normalize_hex32(mapping["transactionHash"], field_name="transactionHash"),
            "transaction_index": parse_hex_uint(mapping["transactionIndex"], field_name="transactionIndex"),
            "log_index": parse_hex_uint(mapping["logIndex"], field_name="logIndex"),
        }
    )


__all__ = ["decode_swap_log"]
