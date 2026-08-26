"""In-memory frozen models for Base Uniswap V3 evidence (no persistence)."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Self, TypeVar, cast

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator, model_validator

from newcoin_trader.domain.early_market_events import (
    AssetIdentity,
    EventAvailability,
    EventAvailabilityStatus,
)
from newcoin_trader.domain.types import require_utc
from newcoin_trader.sources.base_uniswap_v3.contracts import (
    CHAIN_ID,
    FACTORY_ADDRESS,
    POOL_CREATED_TOPIC,
    PROTOCOL_VERSION,
    SWAP_TOPIC,
    ZERO_ADDRESS,
)


def normalize_address(value: object, *, field_name: str = "address") -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a hex string")
    text = value.strip().lower()
    if not text.startswith("0x"):
        raise ValueError(f"{field_name} must start with 0x")
    body = text[2:]
    if len(body) != 40 or any(c not in "0123456789abcdef" for c in body):
        raise ValueError(f"{field_name} must be 0x-prefixed 40-hex address")
    if text == ZERO_ADDRESS:
        raise ValueError(f"{field_name} must not be the zero address")
    return text


def normalize_hex32(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a hex string")
    text = value.strip().lower()
    if not text.startswith("0x"):
        raise ValueError(f"{field_name} must start with 0x")
    body = text[2:]
    if len(body) != 64 or any(c not in "0123456789abcdef" for c in body):
        raise ValueError(f"{field_name} must be 0x-prefixed 32-byte hex")
    return text


def parse_hex_uint(value: object, *, field_name: str) -> int:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a hex string")
    text = value.strip().lower()
    if not text.startswith("0x"):
        raise ValueError(f"{field_name} must start with 0x")
    body = text[2:]
    if not body or any(c not in "0123456789abcdef" for c in body):
        raise ValueError(f"{field_name} malformed hex integer")
    return int(body, 16)


def require_non_bool_int(value: object, *, field_name: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an int (non-bool)")
    if minimum is not None and value < minimum:
        raise ValueError(f"{field_name} must be >= {minimum}")
    return value


def topic_address_word(address: str) -> str:
    addr = normalize_address(address, field_name="address")
    return "0x" + ("0" * 24) + addr[2:]


def reject_removed_log(raw: dict[str, Any]) -> None:
    if raw.get("removed", False) is True:
        raise ValueError("removed log rejected")


class ScanKind(StrEnum):
    FACTORY_POOL_CREATED = "factory_pool_created"
    POOL_SWAP = "pool_swap"


class ScanStatus(StrEnum):
    INCOMPLETE = "INCOMPLETE"
    COMPLETED_EMPTY = "COMPLETED_EMPTY"
    COMPLETED_NONEMPTY = "COMPLETED_NONEMPTY"
    FAILED_CAP_AMBIGUITY = "FAILED_CAP_AMBIGUITY"
    FAILED_PROVIDER = "FAILED_PROVIDER"


class CapPolicy(StrEnum):
    REFUSE_ON_HIT = "refuse_on_hit"


FAILURE_STATUSES = frozenset({ScanStatus.FAILED_CAP_AMBIGUITY, ScanStatus.FAILED_PROVIDER, ScanStatus.INCOMPLETE})
COMPLETED_STATUSES = frozenset({ScanStatus.COMPLETED_EMPTY, ScanStatus.COMPLETED_NONEMPTY})


class FactoryPoolCreatedRecord(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    chain_id: int
    protocol_version: str
    factory_address: str
    token0: str
    token1: str
    fee: int
    tick_spacing: int
    pool_address: str
    block_number: int
    block_hash: str
    transaction_hash: str
    transaction_index: int
    log_index: int

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        out["factory_address"] = normalize_address(out.get("factory_address"), field_name="factory_address")
        out["token0"] = normalize_address(out.get("token0"), field_name="token0")
        out["token1"] = normalize_address(out.get("token1"), field_name="token1")
        out["pool_address"] = normalize_address(out.get("pool_address"), field_name="pool_address")
        out["block_hash"] = normalize_hex32(out.get("block_hash"), field_name="block_hash")
        out["transaction_hash"] = normalize_hex32(out.get("transaction_hash"), field_name="transaction_hash")
        return out

    @model_validator(mode="after")
    def _canonical(self) -> FactoryPoolCreatedRecord:
        if self.chain_id != CHAIN_ID:
            raise ValueError("chain_id must equal Base CHAIN_ID")
        if self.protocol_version != PROTOCOL_VERSION:
            raise ValueError("protocol_version mismatch")
        if self.factory_address != FACTORY_ADDRESS:
            raise ValueError("factory_address must equal canonical FACTORY_ADDRESS")
        require_non_bool_int(self.fee, field_name="fee", minimum=0)
        require_non_bool_int(self.tick_spacing, field_name="tick_spacing")
        require_non_bool_int(self.block_number, field_name="block_number", minimum=0)
        require_non_bool_int(self.transaction_index, field_name="transaction_index", minimum=0)
        require_non_bool_int(self.log_index, field_name="log_index", minimum=0)
        return self

    @property
    def order_key(self) -> tuple[int, int, int]:
        return (self.block_number, self.transaction_index, self.log_index)


class SwapLogRecord(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    chain_id: int
    protocol_version: str
    pool_address: str
    sender: str
    recipient: str
    amount0: int
    amount1: int
    sqrt_price_x96: int
    liquidity: int
    tick: int
    block_number: int
    block_hash: str
    transaction_hash: str
    transaction_index: int
    log_index: int

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        out["pool_address"] = normalize_address(out.get("pool_address"), field_name="pool_address")
        out["sender"] = normalize_address(out.get("sender"), field_name="sender")
        out["recipient"] = normalize_address(out.get("recipient"), field_name="recipient")
        out["block_hash"] = normalize_hex32(out.get("block_hash"), field_name="block_hash")
        out["transaction_hash"] = normalize_hex32(out.get("transaction_hash"), field_name="transaction_hash")
        return out

    @model_validator(mode="after")
    def _canonical(self) -> SwapLogRecord:
        if self.chain_id != CHAIN_ID:
            raise ValueError("chain_id must equal Base CHAIN_ID")
        if self.protocol_version != PROTOCOL_VERSION:
            raise ValueError("protocol_version mismatch")
        for name in ("amount0", "amount1", "sqrt_price_x96", "liquidity", "tick"):
            require_non_bool_int(getattr(self, name), field_name=name)
        require_non_bool_int(self.block_number, field_name="block_number", minimum=0)
        require_non_bool_int(self.transaction_index, field_name="transaction_index", minimum=0)
        require_non_bool_int(self.log_index, field_name="log_index", minimum=0)
        if self.sqrt_price_x96 < 0:
            raise ValueError("sqrt_price_x96 must be unsigned")
        if self.liquidity < 0:
            raise ValueError("liquidity must be unsigned")
        return self

    @property
    def order_key(self) -> tuple[int, int, int]:
        return (self.block_number, self.transaction_index, self.log_index)


class VerifiedBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    number: int
    hash: str
    timestamp: datetime

    @field_validator("timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        out["hash"] = normalize_hex32(out.get("hash"), field_name="hash")
        return out

    @model_validator(mode="after")
    def _bounds(self) -> VerifiedBlock:
        require_non_bool_int(self.number, field_name="number", minimum=0)
        return self


class VerifiedReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    transaction_hash: str
    block_hash: str
    block_number: int
    transaction_index: int
    status: int

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        out["transaction_hash"] = normalize_hex32(out.get("transaction_hash"), field_name="transaction_hash")
        out["block_hash"] = normalize_hex32(out.get("block_hash"), field_name="block_hash")
        return out

    @model_validator(mode="after")
    def _bounds(self) -> VerifiedReceipt:
        require_non_bool_int(self.block_number, field_name="block_number", minimum=0)
        require_non_bool_int(self.transaction_index, field_name="transaction_index", minimum=0)
        require_non_bool_int(self.status, field_name="status", minimum=0)
        return self


class FinalityBoundary(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    number: int
    hash: str
    policy: str
    version: str
    source: str
    verified_timestamp: datetime

    @field_validator("verified_timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        out["hash"] = normalize_hex32(out.get("hash"), field_name="hash")
        return out

    @model_validator(mode="after")
    def _bounds(self) -> FinalityBoundary:
        require_non_bool_int(self.number, field_name="number", minimum=0)
        if not isinstance(self.policy, str) or not self.policy:
            raise ValueError("finality policy required")
        if not isinstance(self.version, str) or not self.version:
            raise ValueError("finality version required")
        if not isinstance(self.source, str) or not self.source:
            raise ValueError("finality source required")
        return self


def _require_raw_log_coords(raw: dict[str, Any]) -> tuple[str, str, int, int, int]:
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
        if key not in raw:
            raise ValueError(f"raw_log missing {key}")
    reject_removed_log(raw)
    tx = normalize_hex32(raw["transactionHash"], field_name="raw_log.transactionHash")
    block_hash = normalize_hex32(raw["blockHash"], field_name="raw_log.blockHash")
    block_number = parse_hex_uint(raw["blockNumber"], field_name="raw_log.blockNumber")
    tx_index = parse_hex_uint(raw["transactionIndex"], field_name="raw_log.transactionIndex")
    log_index = parse_hex_uint(raw["logIndex"], field_name="raw_log.logIndex")
    return tx, block_hash, block_number, tx_index, log_index


def _bind_receipt_block(
    *,
    raw_tx: str,
    raw_block_hash: str,
    raw_block_number: int,
    raw_tx_index: int,
    receipt: VerifiedReceipt,
    block: VerifiedBlock,
    expected_tx_index: int,
) -> None:
    if raw_tx != receipt.transaction_hash:
        raise ValueError("transaction hash binding mismatch")
    if raw_block_hash != receipt.block_hash:
        raise ValueError("block hash binding mismatch")
    if raw_block_hash != block.hash:
        raise ValueError("verified block hash binding mismatch")
    if raw_block_number != receipt.block_number:
        raise ValueError("block number binding mismatch")
    if raw_block_number != block.number:
        raise ValueError("verified block number binding mismatch")
    if receipt.status != 1:
        raise ValueError("receipt status must succeed")
    if receipt.transaction_index != expected_tx_index or raw_tx_index != expected_tx_index:
        raise ValueError("transaction index binding mismatch")
    if block.timestamp is None:
        raise ValueError("missing timestamp")


class CanonicalSwapEvidence(BaseModel):
    """Verified decoded swap evidence with raw/receipt/block bindings (no I/O)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    raw_log: dict[str, Any]
    swap: SwapLogRecord
    receipt: VerifiedReceipt
    block: VerifiedBlock

    @model_validator(mode="after")
    def _bindings(self) -> CanonicalSwapEvidence:
        from newcoin_trader.sources.base_uniswap_v3.swap_decoder import decode_swap_log

        raw = self.raw_log
        raw_tx, raw_block_hash, raw_block_number, raw_tx_index, raw_log_index = _require_raw_log_coords(raw)

        topics = raw["topics"]
        if not isinstance(topics, list) or len(topics) != 3:
            raise ValueError("Swap requires exactly 3 topics")
        topic0 = topics[0]
        if not isinstance(topic0, str) or topic0.strip().lower() != SWAP_TOPIC:
            raise ValueError("wrong Swap topic")

        pool = normalize_address(raw["address"], field_name="raw_log.address")
        if pool != self.swap.pool_address:
            raise ValueError("raw log address must equal pool address")

        expected_t1 = topic_address_word(self.swap.sender)
        expected_t2 = topic_address_word(self.swap.recipient)
        t1 = topics[1].strip().lower() if isinstance(topics[1], str) else ""
        t2 = topics[2].strip().lower() if isinstance(topics[2], str) else ""
        if t1 != expected_t1 or t2 != expected_t2:
            raise ValueError("Swap topic address encoding mismatch")

        data = raw["data"]
        if not isinstance(data, str) or not data.startswith("0x"):
            raise ValueError("zero/malformed Swap ABI data")
        body = data[2:].lower()
        if not body or len(body) != 320 or any(c not in "0123456789abcdef" for c in body):
            raise ValueError("zero/malformed Swap ABI data")

        decoded = decode_swap_log(raw)
        for field in (
            "pool_address",
            "sender",
            "recipient",
            "transaction_hash",
            "block_hash",
            "block_number",
            "transaction_index",
            "log_index",
            "amount0",
            "amount1",
            "sqrt_price_x96",
            "liquidity",
            "tick",
        ):
            if getattr(decoded, field) != getattr(self.swap, field):
                raise ValueError(f"decoded Swap field mismatch: {field}")

        if raw_tx != self.swap.transaction_hash:
            raise ValueError("transaction hash binding mismatch")
        if raw_block_hash != self.swap.block_hash:
            raise ValueError("block hash binding mismatch")
        if raw_block_number != self.swap.block_number:
            raise ValueError("block number binding mismatch")
        if raw_log_index != self.swap.log_index:
            raise ValueError("log index binding mismatch")

        _bind_receipt_block(
            raw_tx=raw_tx,
            raw_block_hash=raw_block_hash,
            raw_block_number=raw_block_number,
            raw_tx_index=raw_tx_index,
            receipt=self.receipt,
            block=self.block,
            expected_tx_index=self.swap.transaction_index,
        )
        return self


class CanonicalPoolCreatedEvidence(BaseModel):
    """Verified PoolCreated evidence; bare FactoryPoolCreatedRecord is insufficient."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    raw_log: dict[str, Any]
    creation: FactoryPoolCreatedRecord
    receipt: VerifiedReceipt
    block: VerifiedBlock

    @model_validator(mode="after")
    def _bindings(self) -> CanonicalPoolCreatedEvidence:
        from newcoin_trader.sources.base_uniswap_v3.pool_created_decoder import decode_pool_created_log

        raw = self.raw_log
        raw_tx, raw_block_hash, raw_block_number, raw_tx_index, raw_log_index = _require_raw_log_coords(raw)

        factory = normalize_address(raw["address"], field_name="raw_log.address")
        if factory != FACTORY_ADDRESS or factory != self.creation.factory_address:
            raise ValueError("wrong factory address for PoolCreated")

        topics = raw["topics"]
        if not isinstance(topics, list) or len(topics) != 4:
            raise ValueError("PoolCreated requires exactly 4 topics")
        topic0 = topics[0]
        if not isinstance(topic0, str) or topic0.strip().lower() != POOL_CREATED_TOPIC:
            raise ValueError("wrong PoolCreated topic")

        expected_topics = [
            POOL_CREATED_TOPIC,
            topic_address_word(self.creation.token0),
            topic_address_word(self.creation.token1),
            "0x" + f"{self.creation.fee:064x}",
        ]
        normalized_topics = [t.strip().lower() if isinstance(t, str) else "" for t in topics]
        if normalized_topics != expected_topics:
            raise ValueError("PoolCreated topic ABI mismatch")

        data = raw["data"]
        if not isinstance(data, str) or not data.startswith("0x"):
            raise ValueError("malformed PoolCreated ABI data")
        body = data[2:].lower()
        if len(body) != 128 or any(c not in "0123456789abcdef" for c in body):
            raise ValueError("malformed PoolCreated ABI data")

        decoded = decode_pool_created_log(raw)
        for field in (
            "factory_address",
            "token0",
            "token1",
            "fee",
            "tick_spacing",
            "pool_address",
            "transaction_hash",
            "block_hash",
            "block_number",
            "transaction_index",
            "log_index",
        ):
            if getattr(decoded, field) != getattr(self.creation, field):
                raise ValueError(f"decoded PoolCreated field mismatch: {field}")

        if raw_tx != self.creation.transaction_hash:
            raise ValueError("transaction hash binding mismatch")
        if raw_block_hash != self.creation.block_hash:
            raise ValueError("block hash binding mismatch")
        if raw_block_number != self.creation.block_number:
            raise ValueError("block number binding mismatch")
        if raw_log_index != self.creation.log_index:
            raise ValueError("log index binding mismatch")

        _bind_receipt_block(
            raw_tx=raw_tx,
            raw_block_hash=raw_block_hash,
            raw_block_number=raw_block_number,
            raw_tx_index=raw_tx_index,
            receipt=self.receipt,
            block=self.block,
            expected_tx_index=self.creation.transaction_index,
        )
        return self


class ScanLedgerEntry(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    scan_id: str
    parent_scan_id: str | None = None
    scan_kind: ScanKind
    status: ScanStatus
    from_block: int
    to_block: int
    response_count: int
    response_digest: str = ""
    configured_cap: int
    cap_policy: CapPolicy = CapPolicy.REFUSE_ON_HIT
    possible_truncation: bool
    address: str | None = None
    topic0: str | None = None
    provider_endpoint: str = ""
    provider_version: str = ""
    attempt: int = 0
    split_depth: int = 0
    note: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        if out.get("address") is not None:
            out["address"] = normalize_address(out.get("address"), field_name="address")
        if out.get("topic0") is not None:
            out["topic0"] = normalize_hex32(out.get("topic0"), field_name="topic0")
        return out

    @model_validator(mode="after")
    def _bounds(self) -> ScanLedgerEntry:
        if not isinstance(self.scan_id, str) or not self.scan_id:
            raise ValueError("scan_id required")
        if self.parent_scan_id is not None and (not isinstance(self.parent_scan_id, str) or not self.parent_scan_id):
            raise ValueError("parent_scan_id malformed")
        require_non_bool_int(self.from_block, field_name="from_block", minimum=0)
        require_non_bool_int(self.to_block, field_name="to_block", minimum=0)
        if self.to_block < self.from_block:
            raise ValueError("to_block must be >= from_block")
        require_non_bool_int(self.response_count, field_name="response_count", minimum=0)
        require_non_bool_int(self.configured_cap, field_name="configured_cap", minimum=1)
        require_non_bool_int(self.attempt, field_name="attempt", minimum=0)
        require_non_bool_int(self.split_depth, field_name="split_depth", minimum=0)
        if not isinstance(self.response_digest, str):
            raise ValueError("response_digest must be a string")
        if not isinstance(self.provider_endpoint, str):
            raise ValueError("provider_endpoint must be a string")
        if not isinstance(self.provider_version, str):
            raise ValueError("provider_version must be a string")
        return self


class CapAmbiguityError(ValueError):
    """Raised when a scan cannot be marked completed because the response hit the cap."""

    def __init__(self, entry: ScanLedgerEntry) -> None:
        self.entry = entry
        super().__init__("FAILED_CAP_AMBIGUITY: response_count hit configured cap")


def _intervals_cover(intervals: list[tuple[int, int]], *, lower: int, upper: int) -> bool:
    if upper < lower:
        return True
    if not intervals:
        return False
    ordered = sorted(intervals)
    cursor = lower
    for start, end in ordered:
        if start > cursor:
            return False
        if end >= cursor:
            cursor = end + 1
        if cursor > upper:
            return True
    return cursor > upper


def validate_ordered_scan_ledger_completeness(
    entries: tuple[ScanLedgerEntry, ...],
    *,
    scan_kind: ScanKind,
    address: str,
    topic0: str,
    lower_block: int,
    upper_block: int,
) -> None:
    """Derive completeness from ledger facts; never a caller boolean."""
    addr = normalize_address(address, field_name="address")
    topic = normalize_hex32(topic0, field_name="topic0")
    require_non_bool_int(lower_block, field_name="lower_block", minimum=0)
    require_non_bool_int(upper_block, field_name="upper_block", minimum=0)
    if upper_block < lower_block:
        raise ValueError("upper_block must be >= lower_block")

    for entry in entries:
        if entry.scan_kind is not scan_kind:
            raise ValueError("ledger entry scan kind mismatch")
        if entry.address != addr:
            raise ValueError("ledger entry address filter mismatch")
        if entry.topic0 != topic:
            raise ValueError("ledger entry topic filter mismatch")
        if not entry.provider_endpoint or not entry.provider_version:
            raise ValueError("provider endpoint/version required in scan proof")
        if not isinstance(entry.response_digest, str) or (entry.response_count > 0 and not entry.response_digest):
            raise ValueError("response_digest required for nonempty scan responses")
        if entry.status is ScanStatus.FAILED_CAP_AMBIGUITY or entry.possible_truncation:
            raise ValueError("cap ambiguity in scan ledger")
        if entry.status is ScanStatus.FAILED_PROVIDER:
            raise ValueError("failed provider scan ledger entry")
        if entry.status in COMPLETED_STATUSES and entry.response_count >= entry.configured_cap:
            raise ValueError("cap-hit success refused")
        if entry.cap_policy is not CapPolicy.REFUSE_ON_HIT:
            raise ValueError("unsupported cap policy")

    by_id = {entry.scan_id: entry for entry in entries}
    children_by_parent: dict[str, list[ScanLedgerEntry]] = defaultdict(list)
    for entry in entries:
        if entry.parent_scan_id is not None:
            children_by_parent[entry.parent_scan_id].append(entry)

    for entry in entries:
        if entry.status is ScanStatus.INCOMPLETE:
            kids = children_by_parent.get(entry.scan_id, [])
            if not kids:
                raise ValueError("unresolved split: incomplete parent without children")
            if any(child.status not in COMPLETED_STATUSES for child in kids):
                raise ValueError("unresolved split: non-terminal child")
            child_intervals = [(child.from_block, child.to_block) for child in kids]
            if not _intervals_cover(child_intervals, lower=entry.from_block, upper=entry.to_block):
                raise ValueError("unresolved split: children do not cover parent range")
        elif entry.status not in COMPLETED_STATUSES:
            raise ValueError("non-terminal scan ledger entry")

    for parent_id, _kids in children_by_parent.items():
        parent = by_id.get(parent_id)
        if parent is None:
            raise ValueError("unresolved split: missing parent")
        if parent.status not in COMPLETED_STATUSES and parent.status is not ScanStatus.INCOMPLETE:
            raise ValueError("unresolved split: non-terminal parent")

    completed = [entry for entry in entries if entry.status in COMPLETED_STATUSES]
    intervals = [(entry.from_block, entry.to_block) for entry in completed]
    if not _intervals_cover(intervals, lower=lower_block, upper=upper_block):
        raise ValueError("incomplete scan coverage or gap")


class FactoryUniverseScanProof(BaseModel):
    """Machine-valid proof that factory PoolCreated history is complete to finality."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    factory_address: str
    topic0: str
    deployment_lower_block: int
    finality: FinalityBoundary
    entries: tuple[ScanLedgerEntry, ...]

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        out["factory_address"] = normalize_address(out.get("factory_address"), field_name="factory_address")
        out["topic0"] = normalize_hex32(out.get("topic0"), field_name="topic0")
        return out

    @model_validator(mode="after")
    def _validate(self) -> FactoryUniverseScanProof:
        if self.factory_address != FACTORY_ADDRESS:
            raise ValueError("factory proof requires canonical factory address")
        if self.topic0 != POOL_CREATED_TOPIC:
            raise ValueError("factory proof requires PoolCreated topic")
        require_non_bool_int(self.deployment_lower_block, field_name="deployment_lower_block", minimum=0)
        validate_ordered_scan_ledger_completeness(
            self.entries,
            scan_kind=ScanKind.FACTORY_POOL_CREATED,
            address=self.factory_address,
            topic0=self.topic0,
            lower_block=self.deployment_lower_block,
            upper_block=self.finality.number,
        )
        return self

    def assert_covers_block(self, block_number: int) -> None:
        require_non_bool_int(block_number, field_name="block_number", minimum=0)
        if block_number < self.deployment_lower_block or block_number > self.finality.number:
            raise ValueError("creation block not covered by factory scan proof")
        completed = [entry for entry in self.entries if entry.status in COMPLETED_STATUSES]
        if not _intervals_cover(
            [(entry.from_block, entry.to_block) for entry in completed],
            lower=block_number,
            upper=block_number,
        ):
            raise ValueError("creation block not covered by factory scan proof")


class ExactPoolHistoryScanProof(BaseModel):
    """Machine-valid proof that exact-pool Swap history is complete from creation to finality."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    pool_address: str
    topic0: str
    creation_block: int
    finality: FinalityBoundary
    entries: tuple[ScanLedgerEntry, ...]

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        out["pool_address"] = normalize_address(out.get("pool_address"), field_name="pool_address")
        out["topic0"] = normalize_hex32(out.get("topic0"), field_name="topic0")
        return out

    @model_validator(mode="after")
    def _validate(self) -> ExactPoolHistoryScanProof:
        if self.topic0 != SWAP_TOPIC:
            raise ValueError("pool history proof requires Swap topic")
        require_non_bool_int(self.creation_block, field_name="creation_block", minimum=0)
        validate_ordered_scan_ledger_completeness(
            self.entries,
            scan_kind=ScanKind.POOL_SWAP,
            address=self.pool_address,
            topic0=self.topic0,
            lower_block=self.creation_block,
            upper_block=self.finality.number,
        )
        return self

    def assert_covers_block(self, block_number: int) -> None:
        require_non_bool_int(block_number, field_name="block_number", minimum=0)
        if block_number < self.creation_block or block_number > self.finality.number:
            raise ValueError("swap block not covered by pool scan proof")
        completed = [entry for entry in self.entries if entry.status in COMPLETED_STATUSES]
        if not _intervals_cover(
            [(entry.from_block, entry.to_block) for entry in completed],
            lower=block_number,
            upper=block_number,
        ):
            raise ValueError("swap block not covered by pool scan proof")


def _stable_json_sha256_digest(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return "0x" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


_ModelT = TypeVar("_ModelT", bound=BaseModel)


def _plain_python_for_validate(value: Any) -> Any:
    """Reduce nested models/mappings to plain Python so validation always re-runs."""
    if isinstance(value, BaseModel):
        try:
            return value.model_dump(mode="python")
        except (TypeError, ValueError, ValidationError) as exc:
            raise ValueError(f"model_dump failed closed during plain reconstruct: {exc}") from exc
        except Exception as exc:  # noqa: BLE001 — fail closed on odd construct dump forms
            raise ValueError(f"model_dump failed closed during plain reconstruct: {exc}") from exc
    if isinstance(value, Mapping):
        return {key: _plain_python_for_validate(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain_python_for_validate(item) for item in value]
    if isinstance(value, tuple):
        return [_plain_python_for_validate(item) for item in value]
    return value


def strict_reconstruct_model(model_type: type[_ModelT], value: Any) -> _ModelT:
    """
    Fail-closed reconstruct through plain dump + model_validate.

    model_construct / revalidate_instances='never' cannot sneak unvalidated nested
    evidence, scan digests, ledger proofs, or finality into public adapters.
    Does not mutate the input value.
    """
    try:
        if isinstance(value, BaseModel):
            payload = _plain_python_for_validate(value)
        elif isinstance(value, Mapping):
            payload = _plain_python_for_validate(dict(value))
        else:
            raise ValueError(f"strict reconstruct requires BaseModel or mapping, got {type(value).__name__}")
        if not isinstance(payload, dict):
            raise ValueError("strict reconstruct dump must produce a mapping")
    except ValidationError:
        raise
    except ValueError:
        raise
    except Exception as exc:  # noqa: BLE001 — fail closed on unexpected dump shapes
        raise ValueError(f"strict reconstruct dump failed: {exc}") from exc
    return model_type.model_validate(payload)


def validated_model_copy(
    instance: BaseModel,
    *,
    update: Mapping[str, Any] | None = None,
    deep: bool = False,
) -> Any:
    """
    Copy through full model_validate so update cannot bypass nested validators.

    Rebuild always starts from a complete model_dump(mode='python') of self,
    overlays a plain dump of update values (so injected BaseModel instances are
    not shallow-merged), then model_validate. include/exclude field filtering is
    not supported on this path.
    """
    if update is None:
        return BaseModel.model_copy(instance, deep=deep)
    if not isinstance(update, Mapping):
        raise TypeError("model_copy update must be a mapping")
    try:
        payload = _plain_python_for_validate(instance)
        if not isinstance(payload, dict):
            raise ValueError("model_copy dump must produce a mapping")
        payload.update({key: _plain_python_for_validate(value) for key, value in update.items()})
    except ValidationError:
        raise
    except ValueError:
        raise
    except Exception as exc:  # noqa: BLE001 — fail closed on odd construct dump forms
        raise ValueError(f"validated model_copy dump failed: {exc}") from exc
    return type(instance).model_validate(payload)


def compute_raw_canonical_binding_digest(evidence: CanonicalSwapEvidence) -> str:
    """Digest of immutable raw log coordinates/topics/data that bind canonical Swap evidence."""
    raw = evidence.raw_log
    topics = raw["topics"]
    if not isinstance(topics, list):
        raise ValueError("raw_log topics must be a list")
    payload = {
        "address": normalize_address(raw["address"], field_name="raw_log.address"),
        "blockHash": normalize_hex32(raw["blockHash"], field_name="raw_log.blockHash"),
        "blockNumber": parse_hex_uint(raw["blockNumber"], field_name="raw_log.blockNumber"),
        "data": raw["data"].strip().lower() if isinstance(raw["data"], str) else raw["data"],
        "logIndex": parse_hex_uint(raw["logIndex"], field_name="raw_log.logIndex"),
        "topics": [item.strip().lower() if isinstance(item, str) else item for item in topics],
        "transactionHash": normalize_hex32(raw["transactionHash"], field_name="raw_log.transactionHash"),
        "transactionIndex": parse_hex_uint(raw["transactionIndex"], field_name="raw_log.transactionIndex"),
    }
    return _stable_json_sha256_digest(payload)


def compute_canonical_swap_candidate_identity(evidence: CanonicalSwapEvidence) -> dict[str, object]:
    swap = evidence.swap
    return {
        "amount0": swap.amount0,
        "amount1": swap.amount1,
        "block_hash": swap.block_hash,
        "block_number": swap.block_number,
        "liquidity": swap.liquidity,
        "log_index": swap.log_index,
        "pool_address": swap.pool_address,
        "raw_binding_digest": compute_raw_canonical_binding_digest(evidence),
        "recipient": swap.recipient,
        "sender": swap.sender,
        "sqrt_price_x96": swap.sqrt_price_x96,
        "tick": swap.tick,
        "transaction_hash": swap.transaction_hash,
        "transaction_index": swap.transaction_index,
    }


def compute_canonical_swap_candidates_digest(candidates: Sequence[CanonicalSwapEvidence]) -> str:
    """Deterministic digest of complete canonical candidates in stable order_key order."""
    if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
        raise ValueError("candidates must be a sequence of CanonicalSwapEvidence")
    ordered = sorted(candidates, key=lambda item: item.swap.order_key)
    payload = [compute_canonical_swap_candidate_identity(item) for item in ordered]
    return _stable_json_sha256_digest(payload)


def compute_aggregate_exact_pool_scan_digest(proof: ExactPoolHistoryScanProof) -> tuple[int, str]:
    """
    Bind completed exact-pool ledger response digests/counts.

    Single nonempty completed entry: digest is that entry's response_digest.
    Multiple nonempty completed entries: deterministic aggregate over ordered
    (response_digest, response_count) pairs.
    """
    completed = [entry for entry in proof.entries if entry.status in COMPLETED_STATUSES]
    ordered = sorted(completed, key=lambda entry: (entry.from_block, entry.to_block, entry.scan_id))
    total_count = sum(entry.response_count for entry in ordered)
    nonempty = [entry for entry in ordered if entry.response_count > 0]
    if not nonempty:
        return total_count, ""
    if len(nonempty) == 1:
        return total_count, nonempty[0].response_digest
    payload = [{"response_count": entry.response_count, "response_digest": entry.response_digest} for entry in nonempty]
    return total_count, _stable_json_sha256_digest(payload)


def compute_raw_pool_created_binding_digest(evidence: CanonicalPoolCreatedEvidence) -> str:
    raw = evidence.raw_log
    topics = raw["topics"]
    if not isinstance(topics, list):
        raise ValueError("raw_log topics must be a list")
    payload = {
        "address": normalize_address(raw["address"], field_name="raw_log.address"),
        "blockHash": normalize_hex32(raw["blockHash"], field_name="raw_log.blockHash"),
        "blockNumber": parse_hex_uint(raw["blockNumber"], field_name="raw_log.blockNumber"),
        "data": raw["data"].strip().lower() if isinstance(raw["data"], str) else raw["data"],
        "logIndex": parse_hex_uint(raw["logIndex"], field_name="raw_log.logIndex"),
        "topics": [item.strip().lower() if isinstance(item, str) else item for item in topics],
        "transactionHash": normalize_hex32(raw["transactionHash"], field_name="raw_log.transactionHash"),
        "transactionIndex": parse_hex_uint(raw["transactionIndex"], field_name="raw_log.transactionIndex"),
    }
    return _stable_json_sha256_digest(payload)


def compute_canonical_pool_created_candidates_digest(candidates: Sequence[CanonicalPoolCreatedEvidence]) -> str:
    if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
        raise ValueError("candidates must be a sequence of CanonicalPoolCreatedEvidence")
    ordered = sorted(candidates, key=lambda item: item.creation.order_key)
    payload = [
        {
            "creation": item.creation.model_dump(mode="python"),
            "raw_binding_digest": compute_raw_pool_created_binding_digest(item),
        }
        for item in ordered
    ]
    return _stable_json_sha256_digest(payload)


def _compute_aggregate_scan_digest(entries: Sequence[ScanLedgerEntry]) -> tuple[int, str]:
    completed = [entry for entry in entries if entry.status in COMPLETED_STATUSES]
    ordered = sorted(completed, key=lambda entry: (entry.from_block, entry.to_block, entry.scan_id))
    total_count = sum(entry.response_count for entry in ordered)
    nonempty = [entry for entry in ordered if entry.response_count > 0]
    if not nonempty:
        return total_count, ""
    if len(nonempty) == 1:
        return total_count, nonempty[0].response_digest
    payload = [{"response_count": entry.response_count, "response_digest": entry.response_digest} for entry in nonempty]
    return total_count, _stable_json_sha256_digest(payload)


class VerifiedFactoryUniverse(BaseModel):
    """Complete canonical PoolCreated candidate set bound to a factory scan proof."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    factory_scan_proof: FactoryUniverseScanProof
    candidates: tuple[CanonicalPoolCreatedEvidence, ...]
    candidate_count: int
    candidate_digest: str
    aggregate_scan_digest: str

    @model_validator(mode="after")
    def _validate(self) -> VerifiedFactoryUniverse:
        if not isinstance(self.candidates, tuple):
            raise ValueError("factory universe candidates must be a tuple")
        require_non_bool_int(self.candidate_count, field_name="factory universe candidate_count", minimum=0)
        if self.candidate_count != len(self.candidates):
            raise ValueError("factory universe candidate count mismatch")
        if not isinstance(self.candidate_digest, str):
            raise ValueError("factory universe candidate digest must be a string")
        if not isinstance(self.aggregate_scan_digest, str):
            raise ValueError("factory universe aggregate scan digest must be a string")

        ordered = tuple(sorted(self.candidates, key=lambda item: item.creation.order_key))
        if ordered != self.candidates:
            object.__setattr__(self, "candidates", ordered)
        pools: set[str] = set()
        for evidence in ordered:
            creation = evidence.creation
            if creation.pool_address in pools:
                raise ValueError("duplicate pool in factory universe")
            pools.add(creation.pool_address)
            self.factory_scan_proof.assert_covers_block(creation.block_number)
            assert_finality_for_block(evidence.block, self.factory_scan_proof.finality)

        digest = compute_canonical_pool_created_candidates_digest(ordered)
        if self.candidate_digest != digest:
            raise ValueError("factory universe candidate digest mismatch")
        bound_count, bound_digest = _compute_aggregate_scan_digest(self.factory_scan_proof.entries)
        if self.candidate_count != bound_count:
            raise ValueError("factory universe candidate count mismatch with ledger response_count")
        if self.aggregate_scan_digest != bound_digest:
            raise ValueError("factory universe aggregate scan digest mismatch with ledger")
        for entry in self.factory_scan_proof.entries:
            if entry.status not in COMPLETED_STATUSES or entry.response_count == 0:
                continue
            in_range = tuple(
                item for item in ordered if entry.from_block <= item.creation.block_number <= entry.to_block
            )
            if len(in_range) != entry.response_count:
                raise ValueError("factory universe candidate count mismatch with ledger entry range")
            if entry.response_digest != compute_canonical_pool_created_candidates_digest(in_range):
                raise ValueError("factory universe ledger response_digest mismatch with canonical candidates")
        return self

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        return cast(Self, validated_model_copy(self, update=update, deep=deep))

    @classmethod
    def from_complete_candidates(
        cls,
        *,
        factory_scan_proof: FactoryUniverseScanProof,
        candidates: Sequence[CanonicalPoolCreatedEvidence],
    ) -> VerifiedFactoryUniverse:
        ordered = tuple(sorted(candidates, key=lambda item: item.creation.order_key))
        _count, aggregate = _compute_aggregate_scan_digest(factory_scan_proof.entries)
        return cls.model_validate(
            {
                "factory_scan_proof": factory_scan_proof,
                "candidates": ordered,
                "candidate_count": len(ordered),
                "candidate_digest": compute_canonical_pool_created_candidates_digest(ordered),
                "aggregate_scan_digest": aggregate,
            }
        )


class VerifiedExactPoolSwapScanResult(BaseModel):
    """
    Frozen complete exact-pool Swap scan result.

    Owns exact pool identity, complete ExactPoolHistoryScanProof, all canonical
    Swap evidence candidates, and immutable candidate_count / candidate_digest.
    Callers cannot omit earlier canonical swaps without failing digest/count binding.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    pool_address: str
    pool_scan_proof: ExactPoolHistoryScanProof
    candidates: tuple[CanonicalSwapEvidence, ...]
    candidate_count: int
    candidate_digest: str
    aggregate_scan_digest: str

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        out["pool_address"] = normalize_address(out.get("pool_address"), field_name="pool_address")
        return out

    @model_validator(mode="after")
    def _validate(self) -> VerifiedExactPoolSwapScanResult:
        proof = self.pool_scan_proof
        if proof.pool_address != self.pool_address:
            raise ValueError("scan result pool mismatch with exact pool history proof")
        if proof.topic0 != SWAP_TOPIC:
            raise ValueError("scan result proof requires Swap topic")

        if not isinstance(self.candidates, tuple):
            raise ValueError("scan result candidates must be a tuple")
        if self.candidate_count != len(self.candidates):
            raise ValueError("scan result candidate count mismatch")
        require_non_bool_int(self.candidate_count, field_name="candidate_count", minimum=1)
        if not isinstance(self.candidate_digest, str) or not self.candidate_digest:
            raise ValueError("scan result candidate digest required")
        if not isinstance(self.aggregate_scan_digest, str) or not self.aggregate_scan_digest:
            raise ValueError("scan result aggregate scan digest required")

        for evidence in self.candidates:
            if not isinstance(evidence, CanonicalSwapEvidence):
                raise ValueError("scan result candidates must be CanonicalSwapEvidence")

        ordered = tuple(sorted(self.candidates, key=lambda item: item.swap.order_key))
        if ordered != self.candidates:
            object.__setattr__(self, "candidates", ordered)

        seen_keys: set[tuple[int, int, int]] = set()
        for evidence in ordered:
            if evidence.swap.pool_address != self.pool_address:
                raise ValueError("scan result candidate pool mismatch")
            key = evidence.swap.order_key
            if key in seen_keys:
                raise ValueError("scan result duplicate candidate identity")
            seen_keys.add(key)
            block_number = evidence.swap.block_number
            if block_number < proof.creation_block or block_number > proof.finality.number:
                raise ValueError("scan result candidate out of scan range")
            proof.assert_covers_block(block_number)

        computed_digest = compute_canonical_swap_candidates_digest(ordered)
        if self.candidate_digest != computed_digest:
            raise ValueError("scan result candidate digest mismatch")

        bound_count, bound_digest = compute_aggregate_exact_pool_scan_digest(proof)
        if self.candidate_count != bound_count:
            raise ValueError("scan result candidate count mismatch with ledger response_count")
        if not bound_digest or self.aggregate_scan_digest != bound_digest:
            raise ValueError("scan result aggregate scan digest mismatch with ledger")

        # Bind each nonempty completed entry digest to candidates in its block range
        # (rejects opaque arbitrary ledger digests that do not match canonical evidence).
        completed = [entry for entry in proof.entries if entry.status in COMPLETED_STATUSES]
        nonempty_entries = [
            entry
            for entry in sorted(completed, key=lambda item: (item.from_block, item.to_block, item.scan_id))
            if entry.response_count > 0
        ]
        for entry in nonempty_entries:
            in_range = tuple(item for item in ordered if entry.from_block <= item.swap.block_number <= entry.to_block)
            if len(in_range) != entry.response_count:
                raise ValueError("scan result candidate count mismatch with ledger entry range")
            range_digest = compute_canonical_swap_candidates_digest(in_range)
            if entry.response_digest != range_digest:
                raise ValueError("scan result ledger response_digest mismatch with canonical candidates")

        # Single nonempty ledger entry binds response_digest directly to candidate_digest.
        # Multiple nonempty entries bind via aggregate_scan_digest over ordered entry digests/counts.
        if len(nonempty_entries) == 1:
            if self.candidate_digest != bound_digest:
                raise ValueError("scan result candidate digest mismatch with ledger binding")
        if self.candidate_count == 1 and bound_count != 1:
            raise ValueError("singleton scan result requires ledger response_count exactly 1")

        return self

    @property
    def ordered_candidates(self) -> tuple[CanonicalSwapEvidence, ...]:
        return self.candidates

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        """Copy through validation so candidate/proof/digest updates cannot bypass bindings."""
        return cast(Self, validated_model_copy(self, update=update, deep=deep))

    @classmethod
    def from_complete_candidates(
        cls,
        *,
        pool_address: str,
        pool_scan_proof: ExactPoolHistoryScanProof,
        candidates: Sequence[CanonicalSwapEvidence],
    ) -> VerifiedExactPoolSwapScanResult:
        """Build a digest-bound scan result; digests are computed, never caller-opaque."""
        if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
            raise ValueError("candidates must be a sequence of CanonicalSwapEvidence")
        ordered = tuple(sorted(candidates, key=lambda item: item.swap.order_key))
        digest = compute_canonical_swap_candidates_digest(ordered)
        _bound_count, aggregate = compute_aggregate_exact_pool_scan_digest(pool_scan_proof)
        return cls.model_validate(
            {
                "pool_address": pool_address,
                "pool_scan_proof": pool_scan_proof,
                "candidates": ordered,
                "candidate_count": len(ordered),
                "candidate_digest": digest,
                "aggregate_scan_digest": aggregate,
            }
        )


def compute_token_decimals_evidence_digest(
    *,
    chain_id: object,
    token_address: object,
    decimals: object,
    evidence_block_number: object,
    evidence_block_hash: object,
    verification_version: object,
) -> str:
    """SHA-256 binding for normalized Base token-decimals evidence fields."""
    bound_chain_id = require_non_bool_int(chain_id, field_name="chain_id")
    address = normalize_address(token_address, field_name="token_address")
    bound_decimals = require_non_bool_int(decimals, field_name="decimals", minimum=0)
    bound_block_number = require_non_bool_int(evidence_block_number, field_name="evidence_block_number", minimum=1)
    block_hash = normalize_hex32(evidence_block_hash, field_name="evidence_block_hash")
    if not isinstance(verification_version, str) or not verification_version.strip():
        raise ValueError("verification_version required")
    return _stable_json_sha256_digest(
        {
            "domain": "newcoin-trader:base_uniswap_v3:token_decimals_evidence_v1",
            "chain_id": bound_chain_id,
            "token_address": address,
            "decimals": bound_decimals,
            "evidence_block_number": bound_block_number,
            "evidence_block_hash": block_hash,
            "verification_version": verification_version.strip(),
        }
    )


class TokenDecimalsEvidence(BaseModel):
    """Exact-address decimal evidence consumed by pure historical normalization."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    chain_id: int
    token_address: str
    decimals: int
    evidence_block_number: int
    evidence_block_hash: str
    verification_version: str
    evidence_digest: str

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        out["token_address"] = normalize_address(out.get("token_address"), field_name="token_address")
        out["evidence_block_hash"] = normalize_hex32(out.get("evidence_block_hash"), field_name="evidence_block_hash")
        return out

    @model_validator(mode="after")
    def _validate(self) -> TokenDecimalsEvidence:
        if self.chain_id != CHAIN_ID:
            raise ValueError("token decimals evidence chain_id must equal Base CHAIN_ID")
        decimals = require_non_bool_int(self.decimals, field_name="decimals", minimum=0)
        if decimals > 255:
            raise ValueError("decimals must be <= 255")
        require_non_bool_int(self.evidence_block_number, field_name="evidence_block_number", minimum=1)
        if not isinstance(self.verification_version, str) or not self.verification_version.strip():
            raise ValueError("verification_version required")
        if not isinstance(self.evidence_digest, str) or not self.evidence_digest.strip():
            raise ValueError("evidence_digest required")
        expected_digest = compute_token_decimals_evidence_digest(
            chain_id=self.chain_id,
            token_address=self.token_address,
            decimals=self.decimals,
            evidence_block_number=self.evidence_block_number,
            evidence_block_hash=self.evidence_block_hash,
            verification_version=self.verification_version,
        )
        if self.evidence_digest != expected_digest:
            raise ValueError("evidence_digest does not bind token decimals evidence fields")
        return self

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        return cast(Self, validated_model_copy(self, update=update, deep=deep))


class HistoricalSwapPointObservation(BaseModel):
    """One canonical Base Uniswap V3 Swap, normalized as a source-time-only price point."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    observation_definition_version: str
    source: str
    chain_id: int
    protocol_version: str
    pool_address: str
    base_asset: AssetIdentity
    quote_asset: AssetIdentity
    source_observation_time: datetime
    availability: EventAvailability
    realized_execution_price_quote_per_base: Decimal
    base_decimals: int
    quote_decimals: int
    transaction_hash: str
    block_number: int
    block_hash: str
    transaction_index: int
    log_index: int
    swap_evidence_binding_digest: str
    base_decimals_evidence_digest: str
    quote_decimals_evidence_digest: str
    quote_policy_version: str

    @field_validator("source_observation_time")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        for field in ("pool_address",):
            out[field] = normalize_address(out.get(field), field_name=field)
        for field in ("transaction_hash", "block_hash"):
            out[field] = normalize_hex32(out.get(field), field_name=field)
        return out

    @model_validator(mode="after")
    def _validate(self) -> HistoricalSwapPointObservation:
        if self.source != "base_uniswap_v3" or self.chain_id != CHAIN_ID:
            raise ValueError("historical swap observation source/chain mismatch")
        if self.protocol_version != PROTOCOL_VERSION:
            raise ValueError("historical swap observation protocol mismatch")
        if self.base_asset.chain != "base" or self.quote_asset.chain != "base":
            raise ValueError("historical swap observation assets must be Base identities")
        if self.base_asset.asset_key == self.quote_asset.asset_key:
            raise ValueError("base and quote assets must differ")
        if self.availability.status is not EventAvailabilityStatus.SOURCE_TIME_ONLY:
            raise ValueError("historical swap observation requires SOURCE_TIME_ONLY availability")
        if self.availability.source_event_time != self.source_observation_time:
            raise ValueError("availability source time must equal observation source time")
        price = self.realized_execution_price_quote_per_base
        if price <= 0 or not price.is_finite():
            raise ValueError("realized execution price must be finite and positive")
        for field in ("base_decimals", "quote_decimals"):
            decimals = require_non_bool_int(getattr(self, field), field_name=field, minimum=0)
            if decimals > 255:
                raise ValueError(f"{field} must be <= 255")
        for field in ("block_number", "transaction_index", "log_index"):
            require_non_bool_int(getattr(self, field), field_name=field, minimum=0)
        for field in (
            "observation_definition_version",
            "swap_evidence_binding_digest",
            "base_decimals_evidence_digest",
            "quote_decimals_evidence_digest",
            "quote_policy_version",
        ):
            if not isinstance(getattr(self, field), str) or not getattr(self, field).strip():
                raise ValueError(f"{field} required")
        return self

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        return cast(Self, validated_model_copy(self, update=update, deep=deep))


def assert_finality_for_block(block: VerifiedBlock, finality: FinalityBoundary) -> None:
    if block.number > finality.number:
        raise ValueError("verified block after finality boundary")
    if block.number == finality.number and block.hash != finality.hash:
        raise ValueError("verified block at finality number requires exact boundary hash")


__all__ = [
    "CanonicalPoolCreatedEvidence",
    "CanonicalSwapEvidence",
    "CapAmbiguityError",
    "CapPolicy",
    "COMPLETED_STATUSES",
    "ExactPoolHistoryScanProof",
    "FactoryPoolCreatedRecord",
    "FactoryUniverseScanProof",
    "FinalityBoundary",
    "HistoricalSwapPointObservation",
    "TokenDecimalsEvidence",
    "VerifiedFactoryUniverse",
    "ScanKind",
    "ScanLedgerEntry",
    "ScanStatus",
    "SwapLogRecord",
    "VerifiedBlock",
    "VerifiedExactPoolSwapScanResult",
    "VerifiedReceipt",
    "assert_finality_for_block",
    "compute_aggregate_exact_pool_scan_digest",
    "compute_canonical_pool_created_candidates_digest",
    "compute_canonical_swap_candidates_digest",
    "compute_token_decimals_evidence_digest",
    "compute_raw_pool_created_binding_digest",
    "compute_raw_canonical_binding_digest",
    "normalize_address",
    "normalize_hex32",
    "parse_hex_uint",
    "require_non_bool_int",
    "strict_reconstruct_model",
    "topic_address_word",
    "validate_ordered_scan_ledger_completeness",
    "validated_model_copy",
    "FAILURE_STATUSES",
]
