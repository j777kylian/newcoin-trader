"""Decode Uniswap V3 PoolCreated logs from raw EVM log dicts (no ABI libs)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from newcoin_trader.sources.base_uniswap_v3.contracts import (
    CHAIN_ID,
    FACTORY_ADDRESS,
    POOL_CREATED_TOPIC,
    PROTOCOL_VERSION,
)
from newcoin_trader.sources.base_uniswap_v3.models import (
    FactoryPoolCreatedRecord,
    normalize_address,
    normalize_hex32,
    parse_hex_uint,
)


def _require_mapping(raw: object) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping):
        raise ValueError("raw log must be a mapping")
    return raw


def _require_topics(value: object) -> list[str]:
    if not isinstance(value, list) or len(value) != 4:
        raise ValueError("PoolCreated requires exactly 4 topics")
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


def _indexed_uint24(topic: str, *, field_name: str) -> int:
    value = int(topic[2:], 16)
    if value >= (1 << 24):
        raise ValueError(f"{field_name} exceeds uint24")
    return value


def _word(data_body: str, index: int) -> str:
    start = index * 64
    end = start + 64
    if end > len(data_body):
        raise ValueError("malformed ABI data")
    return data_body[start:end]


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


def _decode_address_word(word: str, *, field_name: str) -> str:
    if word[:24] != "0" * 24:
        raise ValueError(f"{field_name} address word padding malformed")
    return normalize_address("0x" + word[24:], field_name=field_name)


def decode_pool_created_log(raw: object) -> FactoryPoolCreatedRecord:
    """Decode a factory PoolCreated log into a frozen record."""
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

    factory = normalize_address(mapping["address"], field_name="address")
    if factory != FACTORY_ADDRESS:
        raise ValueError("wrong factory address for PoolCreated")

    topics = _require_topics(mapping["topics"])
    if topics[0] != POOL_CREATED_TOPIC:
        raise ValueError("wrong PoolCreated topic0")

    token0 = _indexed_address(topics[1], field_name="token0")
    token1 = _indexed_address(topics[2], field_name="token1")
    fee = _indexed_uint24(topics[3], field_name="fee")

    data = mapping["data"]
    if not isinstance(data, str) or not data.startswith("0x"):
        raise ValueError("data must be hex string")
    body = data[2:].lower()
    if len(body) != 128 or any(c not in "0123456789abcdef" for c in body):
        raise ValueError("malformed ABI data for PoolCreated")

    tick_spacing = _decode_int24_word(_word(body, 0))
    pool_address = _decode_address_word(_word(body, 1), field_name="pool")

    return FactoryPoolCreatedRecord.model_validate(
        {
            "chain_id": CHAIN_ID,
            "protocol_version": PROTOCOL_VERSION,
            "factory_address": factory,
            "token0": token0,
            "token1": token1,
            "fee": fee,
            "tick_spacing": tick_spacing,
            "pool_address": pool_address,
            "block_number": parse_hex_uint(mapping["blockNumber"], field_name="blockNumber"),
            "block_hash": normalize_hex32(mapping["blockHash"], field_name="blockHash"),
            "transaction_hash": normalize_hex32(mapping["transactionHash"], field_name="transactionHash"),
            "transaction_index": parse_hex_uint(mapping["transactionIndex"], field_name="transactionIndex"),
            "log_index": parse_hex_uint(mapping["logIndex"], field_name="logIndex"),
        }
    )


__all__ = ["decode_pool_created_log"]
