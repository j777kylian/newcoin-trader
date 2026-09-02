"""Bounded Helius Pump discovery claims; indexed data never becomes canonical evidence."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, Self, cast
from urllib.parse import urlsplit, urlunsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, ValidationInfo, model_validator

from newcoin_trader.sources.pumpfun.evidence import (
    PUMP_PROGRAM_ADDRESS,
    PumpRawTransactionEvidence,
    parse_pump_instruction,
    pinned_pump_decoder_evidence,
)

_PROTOCOL = "HeliusIndexedPumpDiscoveryProtocolV1"
_FACTORY = object()
_BASE58 = frozenset("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _hex_digest(value: object) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("digest must be lowercase sha256")
    return value


def _index(value: object, field: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _signature(value: object, field: str = "signature") -> str:
    if not isinstance(value, str) or not 32 <= len(value) <= 88 or any(c not in _BASE58 for c in value):
        raise ValueError(f"{field} must be base58")
    return value


def sanitize_helius_endpoint(endpoint: object) -> str:
    if not isinstance(endpoint, str):
        raise ValueError("Helius endpoint must be a string")
    parsed = urlsplit(endpoint.strip())
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password or parsed.fragment:
        raise ValueError("Helius endpoint must be credential-free HTTPS")
    return urlunsplit(("https", parsed.netloc.lower(), "", "", ""))


def _transport_endpoint(endpoint: str) -> str:
    sanitize_helius_endpoint(endpoint)
    parsed = urlsplit(endpoint.strip())
    return urlunsplit(("https", parsed.netloc, parsed.path.rstrip("/"), parsed.query, ""))


class HeliusIndexedError(RuntimeError):
    """Secret-safe Helius indexed boundary failure."""


class HeliusIndexedCapError(HeliusIndexedError):
    pass


class HeliusIndexedTransport(Protocol):
    async def post_json(self, endpoint: str, payload: dict[str, object], *, timeout_seconds: float) -> object: ...


class _HttpxTransport:
    async def post_json(self, endpoint: str, payload: dict[str, object], *, timeout_seconds: float) -> object:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds)) as client:
            response = await client.post(endpoint, json=payload)
            response.raise_for_status()
            return response.json()


class HeliusIndexedPumpDiscoveryProtocolV1(BaseModel):
    """Frozen source-query contract; only direct historical source time is admitted."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    window_start: int
    window_end: int
    page_limit: int = 1000
    max_attempts: int = 120
    max_records: int = 100_000
    max_credits: int = 25_000
    query_version: str = "getTransactionsForAddress-v1"

    @model_validator(mode="after")
    def _bound(self) -> Self:
        if self.window_end <= self.window_start:
            raise ValueError("historical window must be non-empty")
        if type(self.page_limit) is not int or not 1 <= self.page_limit <= 1000:
            raise ValueError("page limit must be in 1..1000")
        if type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 120:
            raise ValueError("attempt cap must be in 1..120")
        if type(self.max_records) is not int or not 1 <= self.max_records <= 100_000:
            raise ValueError("record cap must be in 1..100000")
        if type(self.max_credits) is not int or not 1 <= self.max_credits <= 25_000:
            raise ValueError("credit cap must be in 1..25000")
        if self.query_version != "getTransactionsForAddress-v1":
            raise ValueError("unsupported Helius query version")
        return self

    @property
    def query_digest(self) -> str:
        return _digest({"protocol": _PROTOCOL, **self.model_dump(mode="json")})


class IndexedPumpCandidateClaim(BaseModel):
    """Immutable Helius source claim. It deliberately has no receipt/decision clock or canonical evidence."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    source: str
    query_digest: str
    page_digest: str
    signature: str
    slot: int
    transaction_index: int
    instruction_index: int
    inner_instruction_index: int | None = None
    mint: str
    method: str
    source_time: int
    claim_digest: str
    _factory_digest: str | None = PrivateAttr(default=None)

    @classmethod
    def model_construct(cls, _fields_set: set[str] | None = None, **values: Any) -> Self:
        raise TypeError("IndexedPumpCandidateClaim.model_construct is not a public construction boundary")

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        if self._factory_digest is None:
            raise ValueError("indexed claim lacks controlled construction evidence")
        rebuilt = type(self).model_validate(
            self.model_dump() if update is None else {**self.model_dump(), **update}, context=_FACTORY
        )
        if rebuilt.claim_digest != self._factory_digest:
            raise ValueError("indexed claim factory binding mismatch")
        rebuilt._factory_digest = self._factory_digest
        return rebuilt

    @model_validator(mode="after")
    def _bound(self, info: ValidationInfo) -> Self:
        if info.context is not _FACTORY:
            raise ValueError("indexed claims require controlled construction")
        if self.source != "helius" or self.method not in {"create", "create_v2"}:
            raise ValueError("indexed claim source or method is invalid")
        for value in (self.query_digest, self.page_digest, self.claim_digest):
            _hex_digest(value)
        _signature(self.signature)
        _signature(self.mint, "mint")
        _index(self.slot, "slot")
        _index(self.transaction_index, "transaction index")
        _index(self.instruction_index, "instruction index")
        _index(self.source_time, "source time")
        if self.inner_instruction_index is not None:
            _index(self.inner_instruction_index, "inner instruction index")
        payload = self.model_dump(exclude={"claim_digest"})
        if self.claim_digest != _digest(payload):
            raise ValueError("indexed claim digest does not bind source identity")
        return self

    @classmethod
    def _create(cls, **values: Any) -> Self:
        payload = {**values, "source": "helius"}
        payload["claim_digest"] = _digest({key: value for key, value in payload.items() if key != "claim_digest"})
        result = cls.model_validate(payload, context=_FACTORY)
        result._factory_digest = result.claim_digest
        return result


class HeliusIndexedPageReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    query_digest: str
    page_index: int
    request_token_digest: str | None
    response_token_digest: str | None
    raw_count: int
    page_digest: str
    raw_transaction_identities: tuple[str, ...] = Field(repr=False)
    raw_payload_digests: tuple[str, ...] = Field(repr=False)
    candidate_claims: tuple[IndexedPumpCandidateClaim, ...] = Field(repr=False)

    @model_validator(mode="after")
    def _bound(self) -> Self:
        _hex_digest(self.query_digest)
        _hex_digest(self.page_digest)
        if self.request_token_digest is not None:
            _hex_digest(self.request_token_digest)
        if self.response_token_digest is not None:
            _hex_digest(self.response_token_digest)
        _index(self.page_index, "page index")
        _index(self.raw_count, "raw count")
        if len(self.raw_transaction_identities) != self.raw_count or len(self.raw_payload_digests) != self.raw_count:
            raise ValueError("page raw transaction commitments do not reconcile")
        for identity in (*self.raw_transaction_identities, *self.raw_payload_digests):
            _hex_digest(identity)
        if len(set(self.raw_transaction_identities)) != len(self.raw_transaction_identities):
            raise ValueError("duplicate indexed transaction within page")
        claims = tuple(
            IndexedPumpCandidateClaim.model_validate(item.model_dump(), context=_FACTORY)
            for item in self.candidate_claims
        )
        if any(item.query_digest != self.query_digest or item.page_digest != self.page_digest for item in claims):
            raise ValueError("page claim does not bind query/page receipt")
        return self


class HeliusIndexedUsageLedger(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    attempts: int
    successful_full_records: int
    estimated_metered_credits: int
    observed_provider_credits: int | None = None
    rate_limit_events: int = 0

    @model_validator(mode="after")
    def _bound(self) -> Self:
        for value, field in (
            (self.attempts, "attempts"),
            (self.successful_full_records, "records"),
            (self.estimated_metered_credits, "estimated credits"),
            (self.rate_limit_events, "rate limits"),
        ):
            _index(value, field)
        if self.observed_provider_credits is not None:
            _index(self.observed_provider_credits, "observed credits")
        if self.estimated_metered_credits != 10 * max(1, (self.successful_full_records + 99) // 100):
            raise ValueError("estimated Helius credits must use documented full-response formula")
        return self


class HeliusIndexedDiscoveryResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @classmethod
    def model_construct(cls, _fields_set: set[str] | None = None, **values: Any) -> Self:
        raise TypeError("HeliusIndexedDiscoveryResult.model_construct is not a public construction boundary")

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        raise TypeError("HeliusIndexedDiscoveryResult.model_copy is not a public mutation boundary")

    protocol: HeliusIndexedPumpDiscoveryProtocolV1
    pages: tuple[HeliusIndexedPageReceipt, ...] = Field(repr=False)
    raw_transaction_count: int
    deduplicated_candidate_count: int
    candidate_set_digest: str
    terminal_proof: str
    retrieved_at: datetime
    usage: HeliusIndexedUsageLedger

    @model_validator(mode="after")
    def _bound(self, info: ValidationInfo) -> Self:
        if info.context is not _FACTORY:
            raise ValueError("indexed discovery result requires controlled construction")
        if self.terminal_proof != "null_paginationToken" or self.retrieved_at.tzinfo is None:
            raise ValueError("discovery terminal or retrieval time is invalid")
        pages = tuple(
            HeliusIndexedPageReceipt.model_validate(page.model_dump(), context=_FACTORY) for page in self.pages
        )
        if not pages or tuple(page.page_index for page in pages) != tuple(range(len(pages))):
            raise ValueError("page indices must be contiguous")
        if any(
            page.request_token_digest != previous.response_token_digest
            for previous, page in zip(pages, pages[1:], strict=False)
        ):
            raise ValueError("pagination token progression is invalid")
        if pages[-1].response_token_digest is not None:
            raise ValueError("terminal page must have null pagination token")
        raw_identities = tuple(identity for page in pages for identity in page.raw_transaction_identities)
        if len(raw_identities) != len(set(raw_identities)):
            raise ValueError("duplicate indexed transaction across pages")
        claims = tuple(claim for page in pages for claim in page.candidate_claims)
        identities = tuple(
            (
                claim.signature,
                claim.slot,
                claim.transaction_index,
                claim.instruction_index,
                claim.inner_instruction_index,
            )
            for claim in claims
        )
        if len(identities) != len(set(identities)) or self.deduplicated_candidate_count != len(claims):
            raise ValueError("duplicate indexed candidate identity")
        if self.raw_transaction_count != sum(page.raw_count for page in pages):
            raise ValueError("raw count does not reconcile pages")
        expected = _digest([claim.claim_digest for claim in claims])
        if self.candidate_set_digest != expected:
            raise ValueError("candidate digest does not bind complete claim set")
        if self.usage.successful_full_records != self.raw_transaction_count:
            raise ValueError("usage does not bind raw source count")
        return self


def _source_transaction(row: object) -> tuple[dict[str, object], dict[str, object]]:
    if not isinstance(row, Mapping):
        raise ValueError("malformed Helius indexed row")
    slot, block_time, transaction_index = row.get("slot"), row.get("blockTime"), row.get("transactionIndex")
    transaction, meta = row.get("transaction"), row.get("meta")
    if (
        type(slot) is not int
        or type(block_time) is not int
        or type(transaction_index) is not int
        or not isinstance(transaction, Mapping)
        or not isinstance(meta, Mapping)
    ):
        raise ValueError("indexed row lacks transaction identity")
    signatures, message = transaction.get("signatures"), transaction.get("message")
    if (
        not isinstance(signatures, list)
        or not signatures
        or not isinstance(signatures[0], str)
        or not isinstance(message, Mapping)
    ):
        raise ValueError("indexed row lacks signature/message")
    return {
        "slot": slot,
        "meta": {"err": meta.get("err"), "innerInstructions": meta.get("innerInstructions", [])},
        "transaction": {"signatures": signatures, "message": dict(message)},
    }, {"signature": signatures[0], "slot": slot, "block_time": block_time, "transaction_index": transaction_index}


def _claims_from_row(row: object, *, query_digest: str, page_digest: str) -> tuple[IndexedPumpCandidateClaim, ...]:
    response, identity = _source_transaction(row)
    source_identity = {key: value for key, value in identity.items() if key != "block_time"}
    source_identity["source_time"] = identity["block_time"]
    transaction = PumpRawTransactionEvidence.from_get_transaction(
        signature=cast(str, source_identity["signature"]), commitment="finalized", response=response
    )
    decoder = pinned_pump_decoder_evidence()
    claims: list[IndexedPumpCandidateClaim] = []
    for outer_index, _ in enumerate(transaction.message["instructions"]):
        for inner_index in (None,):
            try:
                fact = parse_pump_instruction(
                    transaction, decoder, instruction_index=outer_index, inner_instruction_index=inner_index
                )
            except ValueError:
                continue
            if fact.instruction_kind in {"create", "create_v2"}:
                claims.append(
                    IndexedPumpCandidateClaim._create(
                        query_digest=query_digest,
                        page_digest=page_digest,
                        **source_identity,
                        instruction_index=outer_index,
                        inner_instruction_index=None,
                        mint=fact.mint,
                        method=fact.instruction_kind,
                    )
                )
    for parent in transaction.inner_instructions:
        if not isinstance(parent.get("instructions"), list) or type(parent.get("index")) is not int:
            raise ValueError("malformed indexed inner instruction")
        for inner_index, _ in enumerate(parent["instructions"]):
            try:
                fact = parse_pump_instruction(
                    transaction, decoder, instruction_index=parent["index"], inner_instruction_index=inner_index
                )
            except ValueError:
                continue
            if fact.instruction_kind in {"create", "create_v2"}:
                claims.append(
                    IndexedPumpCandidateClaim._create(
                        query_digest=query_digest,
                        page_digest=page_digest,
                        **source_identity,
                        instruction_index=parent["index"],
                        inner_instruction_index=inner_index,
                        mint=fact.mint,
                        method=fact.instruction_kind,
                    )
                )
    return tuple(claims)


class HeliusIndexedHistoryClient:
    """Minimal explicit-endpoint Helius client for a single frozen Pump historical window."""

    def __init__(
        self,
        endpoint: str,
        *,
        transport: HeliusIndexedTransport | None = None,
        timeout_seconds: float = 15.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if type(timeout_seconds) not in {int, float} or not 0 < timeout_seconds <= 15:
            raise ValueError("Helius timeout must be in (0, 15]")
        self._endpoint = _transport_endpoint(endpoint)
        self.provider_origin = sanitize_helius_endpoint(endpoint)
        self._transport = transport or _HttpxTransport()
        self._timeout = float(timeout_seconds)
        self._sleep = sleep

    async def discover(self, protocol: HeliusIndexedPumpDiscoveryProtocolV1) -> HeliusIndexedDiscoveryResult:
        protocol = HeliusIndexedPumpDiscoveryProtocolV1.model_validate(protocol.model_dump())
        pages: list[HeliusIndexedPageReceipt] = []
        token: str | None = None
        seen_tokens: set[str] = set()
        attempts = rate_limits = raw_total = 0
        while True:
            if attempts >= protocol.max_attempts:
                raise HeliusIndexedCapError("Helius indexed attempt cap exhausted")
            params: dict[str, object] = {
                "transactionDetails": "full",
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
                "commitment": "finalized",
                "sortOrder": "asc",
                "limit": protocol.page_limit,
                "filters": {
                    "blockTime": {"gte": protocol.window_start, "lt": protocol.window_end},
                    "status": "succeeded",
                },
            }
            if token is not None:
                params["paginationToken"] = token
            attempts += 1
            try:
                raw = await self._transport.post_json(
                    self._endpoint,
                    {
                        "jsonrpc": "2.0",
                        "id": attempts,
                        "method": "getTransactionsForAddress",
                        "params": [PUMP_PROGRAM_ADDRESS, params],
                    },
                    timeout_seconds=self._timeout,
                )
            except Exception as error:
                if isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 429:
                    rate_limits += 1
                raise HeliusIndexedError("Helius indexed transport failure") from None
            if (
                not isinstance(raw, Mapping)
                or raw.get("jsonrpc") != "2.0"
                or raw.get("id") != attempts
                or not isinstance(raw.get("result"), Mapping)
                or "error" in raw
            ):
                raise HeliusIndexedError("Helius indexed response is malformed")
            result = raw["result"]
            rows = result.get("data")
            if not isinstance(rows, list) or len(rows) > protocol.page_limit:
                raise HeliusIndexedError("Helius indexed response data is malformed")
            request_token_digest = None if token is None else _digest(token)
            summaries: list[dict[str, object]] = []
            for row in rows:
                _, identity = _source_transaction(row)
                block_time = cast(int, identity["block_time"])
                if not protocol.window_start <= block_time < protocol.window_end:
                    raise HeliusIndexedError("Helius indexed row lies outside frozen window")
                summaries.append(
                    {
                        "signature": identity["signature"],
                        "slot": identity["slot"],
                        "blockTime": identity["block_time"],
                        "transactionIndex": identity["transaction_index"],
                    }
                )
            raw_transaction_identities = tuple(_digest(summary) for summary in summaries)
            raw_payload_digests = tuple(_digest(row) for row in rows)
            page_digest = _digest(
                {
                    "protocol": _PROTOCOL,
                    "query": protocol.query_digest,
                    "requestToken": request_token_digest,
                    "rows": summaries,
                    "rawPayloadDigests": raw_payload_digests,
                }
            )
            claims = tuple(
                claim
                for row in rows
                for claim in _claims_from_row(row, query_digest=protocol.query_digest, page_digest=page_digest)
            )
            next_token = result.get("paginationToken")
            if next_token is not None and (
                not isinstance(next_token, str) or not next_token or next_token in seen_tokens or next_token == token
            ):
                raise HeliusIndexedError("Helius pagination token is non-advancing")
            response_token_digest = None if next_token is None else _digest(next_token)
            pages.append(
                HeliusIndexedPageReceipt.model_validate(
                    {
                        "query_digest": protocol.query_digest,
                        "page_index": len(pages),
                        "request_token_digest": request_token_digest,
                        "response_token_digest": response_token_digest,
                        "raw_count": len(rows),
                        "page_digest": page_digest,
                        "raw_transaction_identities": raw_transaction_identities,
                        "raw_payload_digests": raw_payload_digests,
                        "candidate_claims": claims,
                    },
                    context=_FACTORY,
                )
            )
            raw_total += len(rows)
            if raw_total > protocol.max_records:
                raise HeliusIndexedCapError("Helius indexed record cap exhausted")
            estimated = 10 * max(1, (raw_total + 99) // 100)
            if estimated > protocol.max_credits:
                raise HeliusIndexedCapError("Helius indexed credit cap exhausted")
            if next_token is None:
                break
            seen_tokens.add(next_token)
            token = next_token
            await self._sleep(0.25)
        usage = HeliusIndexedUsageLedger(
            attempts=attempts,
            successful_full_records=raw_total,
            estimated_metered_credits=10 * max(1, (raw_total + 99) // 100),
            rate_limit_events=rate_limits,
        )
        claims = tuple(claim for page in pages for claim in page.candidate_claims)
        return HeliusIndexedDiscoveryResult.model_validate(
            {
                "protocol": protocol,
                "pages": tuple(pages),
                "raw_transaction_count": raw_total,
                "deduplicated_candidate_count": len(claims),
                "candidate_set_digest": _digest([claim.claim_digest for claim in claims]),
                "terminal_proof": "null_paginationToken",
                "retrieved_at": datetime.now(UTC),
                "usage": usage,
            },
            context=_FACTORY,
        )


_CORPUS_V2_PLAN_FILE = "source_window_plan_v2.json"
_CORPUS_V2_MANIFEST_FILE = "source_coordinate_manifest_v2.json"
_CORPUS_V2_PLAN_VERSION = "PumpCorpusV2WindowPlanV1"
_CORPUS_V2_MANIFEST_VERSION = "PumpCorpusV2SourceCoordinateManifestV1"


class PumpCorpusV2WindowPlan(BaseModel):
    """Frozen, return-independent historical windows; no decoded identities."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    version: str
    reference_time: datetime
    protocols: tuple[HeliusIndexedPumpDiscoveryProtocolV1, ...]
    plan_digest: str
    _factory_digest: str | None = PrivateAttr(default=None)

    @classmethod
    def model_construct(cls, _fields_set: set[str] | None = None, **values: Any) -> Self:
        raise TypeError("PumpCorpusV2WindowPlan.model_construct is not a public construction boundary")

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        if self._factory_digest != self.plan_digest:
            raise ValueError("window plan lacks controlled construction evidence")
        rebuilt = type(self).model_validate(
            self.model_dump(mode="python") if update is None else {**self.model_dump(mode="python"), **update},
            context=_FACTORY,
        )
        if rebuilt.plan_digest != self._factory_digest:
            raise ValueError("window plan factory binding mismatch")
        rebuilt._factory_digest = self._factory_digest
        return rebuilt

    @classmethod
    def freeze(cls, *, reference_time: datetime, extension_blocks: int = 100) -> Self:
        if reference_time.tzinfo is None or reference_time.utcoffset() != timedelta(0):
            raise ValueError("reference time must be UTC")
        if reference_time != reference_time.replace(minute=0, second=0, microsecond=0):
            raise ValueError("reference time must be an exact UTC hour")
        if type(extension_blocks) is not int or not 1 <= extension_blocks <= 100:
            raise ValueError("extension blocks must be in 1..100")
        protocols = tuple(
            HeliusIndexedPumpDiscoveryProtocolV1(
                window_start=int((reference_time - timedelta(days=age, seconds=90 * (block + 1))).timestamp()),
                window_end=int((reference_time - timedelta(days=age, seconds=90 * block)).timestamp()),
            )
            for block in range(extension_blocks)
            for age in (3, 7, 14)
        )
        payload = {
            "version": _CORPUS_V2_PLAN_VERSION,
            "reference_time": reference_time.isoformat(),
            "protocols": [p.model_dump(mode="json") for p in protocols],
        }
        result = cls.model_validate(
            {
                "version": _CORPUS_V2_PLAN_VERSION,
                "reference_time": reference_time,
                "protocols": protocols,
                "plan_digest": _digest(payload),
            },
            context=_FACTORY,
        )
        result._factory_digest = result.plan_digest
        return result

    @model_validator(mode="after")
    def _bound(self, info: ValidationInfo) -> Self:
        if info.context is not _FACTORY:
            raise ValueError("window plan requires controlled construction")
        if self.version != _CORPUS_V2_PLAN_VERSION:
            raise ValueError("unsupported corpus V2 window plan")
        if self.reference_time.tzinfo is None or self.reference_time.utcoffset() != timedelta(0):
            raise ValueError("reference time must be UTC")
        if self.reference_time != self.reference_time.replace(minute=0, second=0, microsecond=0):
            raise ValueError("reference time must be an exact UTC hour")
        if not self.protocols or len({item.query_digest for item in self.protocols}) != len(self.protocols):
            raise ValueError("window plan protocols must be unique and non-empty")
        payload = {
            "version": self.version,
            "reference_time": self.reference_time.isoformat(),
            "protocols": [p.model_dump(mode="json") for p in self.protocols],
        }
        if self.plan_digest != _digest(payload):
            raise ValueError("window plan digest does not bind content")
        return self


class PumpCorpusV2SourceCoordinate(BaseModel):
    """Source coordinate only: deliberately no decoded identity fields."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    window_query_digest: str
    page_digest: str
    signature: str
    slot: int
    transaction_index: int
    instruction_index: int
    inner_instruction_index: int | None = None
    source_time: int
    method: str
    coordinate_digest: str

    @model_validator(mode="after")
    def _bound(self) -> Self:
        _hex_digest(self.window_query_digest)
        _hex_digest(self.page_digest)
        _signature(self.signature)
        for value, field in (
            (self.slot, "slot"),
            (self.transaction_index, "transaction index"),
            (self.instruction_index, "instruction index"),
            (self.source_time, "source time"),
        ):
            _index(value, field)
        if self.inner_instruction_index is not None:
            _index(self.inner_instruction_index, "inner instruction index")
        if self.method not in {"create", "create_v2"}:
            raise ValueError("source coordinate method is invalid")
        payload = self.model_dump(exclude={"coordinate_digest"})
        if self.coordinate_digest != _digest(payload):
            raise ValueError("source coordinate digest does not bind content")
        return self

    @classmethod
    def from_claim(cls, claim: IndexedPumpCandidateClaim) -> Self:
        payload = {
            "window_query_digest": claim.query_digest,
            "page_digest": claim.page_digest,
            "signature": claim.signature,
            "slot": claim.slot,
            "transaction_index": claim.transaction_index,
            "instruction_index": claim.instruction_index,
            "inner_instruction_index": claim.inner_instruction_index,
            "source_time": claim.source_time,
            "method": claim.method,
        }
        return cls.model_validate({**payload, "coordinate_digest": _digest(payload)})


class PumpCorpusV2SourceManifest(BaseModel):
    """Digest-bound consumed-row and coordinate evidence, independent of decoder output."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    version: str
    plan_digest: str
    windows: tuple[dict[str, object], ...]
    coordinates: tuple[PumpCorpusV2SourceCoordinate, ...]
    manifest_digest: str
    _factory_digest: str | None = PrivateAttr(default=None)

    @classmethod
    def model_construct(cls, _fields_set: set[str] | None = None, **values: Any) -> Self:
        raise TypeError("PumpCorpusV2SourceManifest.model_construct is not a public construction boundary")

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        if self._factory_digest != self.manifest_digest:
            raise ValueError("source manifest lacks controlled construction evidence")
        rebuilt = type(self).model_validate(
            self.model_dump(mode="python") if update is None else {**self.model_dump(mode="python"), **update},
            context=_FACTORY,
        )
        if rebuilt.manifest_digest != self._factory_digest:
            raise ValueError("source manifest factory binding mismatch")
        rebuilt._factory_digest = self._factory_digest
        return rebuilt

    @model_validator(mode="after")
    def _bound(self, info: ValidationInfo) -> Self:
        if info.context is not _FACTORY:
            raise ValueError("source manifest requires controlled construction")
        if self.version != _CORPUS_V2_MANIFEST_VERSION:
            raise ValueError("unsupported corpus V2 source manifest")
        _hex_digest(self.plan_digest)
        if not self.windows:
            raise ValueError("source manifest requires consumed windows")
        window_digests = tuple(item.get("query_digest") for item in self.windows)
        if any(not isinstance(item, str) for item in window_digests) or len(set(window_digests)) != len(window_digests):
            raise ValueError("source manifest window receipts are invalid")
        for window in self.windows:
            if set(window) != {
                "query_digest",
                "terminal_proof",
                "raw_transaction_count",
                "candidate_set_digest",
                "page_receipts",
            }:
                raise ValueError("source manifest window shape is invalid")
            _hex_digest(window["query_digest"])
            _hex_digest(window["candidate_set_digest"])
            _index(window["raw_transaction_count"], "raw transaction count")
            if window["terminal_proof"] != "null_paginationToken" or not isinstance(window["page_receipts"], list):
                raise ValueError("source manifest window terminal proof is invalid")
        coordinates = tuple(
            PumpCorpusV2SourceCoordinate.model_validate(item.model_dump(mode="python")) for item in self.coordinates
        )
        identities = tuple(
            (item.signature, item.slot, item.transaction_index, item.instruction_index, item.inner_instruction_index)
            for item in coordinates
        )
        if len(identities) != len(set(identities)):
            raise ValueError("source manifest contains duplicate coordinates")
        if any(item.window_query_digest not in window_digests for item in coordinates):
            raise ValueError("source coordinate lacks retained window receipt")
        payload = self.model_dump(exclude={"manifest_digest"}, mode="json")
        if self.manifest_digest != _digest(payload):
            raise ValueError("source manifest digest does not bind content")
        return self


def _source_window_receipt(result: HeliusIndexedDiscoveryResult) -> dict[str, object]:
    return {
        "query_digest": result.protocol.query_digest,
        "terminal_proof": result.terminal_proof,
        "raw_transaction_count": result.raw_transaction_count,
        "candidate_set_digest": result.candidate_set_digest,
        "page_receipts": [
            {
                "page_index": page.page_index,
                "request_token_digest": page.request_token_digest,
                "response_token_digest": page.response_token_digest,
                "raw_count": page.raw_count,
                "page_digest": page.page_digest,
                "raw_transaction_identities": list(page.raw_transaction_identities),
                "raw_payload_digests": list(page.raw_payload_digests),
            }
            for page in result.pages
        ],
    }


def _create_only_json(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    with path.open("x", encoding="utf-8") as handle:
        handle.write(encoded)
    return path


def _verified_corpus_v2_plan(plan: PumpCorpusV2WindowPlan) -> PumpCorpusV2WindowPlan:
    if not isinstance(plan, PumpCorpusV2WindowPlan) or plan._factory_digest != plan.plan_digest:
        raise ValueError("window plan lacks controlled construction evidence")
    verified = PumpCorpusV2WindowPlan.model_validate(plan.model_dump(mode="python"), context=_FACTORY)
    verified._factory_digest = verified.plan_digest
    return verified


def write_pump_corpus_v2_window_plan(root: Path, *, plan: PumpCorpusV2WindowPlan) -> Path:
    """Freeze the return-independent source window plan before any provider request."""
    verified = _verified_corpus_v2_plan(plan)
    return _create_only_json(root / _CORPUS_V2_PLAN_FILE, verified.model_dump(mode="json"))


def write_pump_corpus_v2_source_manifest(
    root: Path, *, plan: PumpCorpusV2WindowPlan, discoveries: tuple[HeliusIndexedDiscoveryResult, ...]
) -> Path:
    """Create immutable V2 plan/coordinates; refuses overwrite and drops decoded identities."""
    verified_plan = _verified_corpus_v2_plan(plan)
    plan_path = root / _CORPUS_V2_PLAN_FILE
    try:
        persisted_plan = PumpCorpusV2WindowPlan.model_validate(
            json.loads(plan_path.read_text(encoding="utf-8")), context=_FACTORY
        )
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("source manifest requires an already-frozen window plan") from error
    if persisted_plan.plan_digest != verified_plan.plan_digest:
        raise ValueError("source manifest does not match frozen window plan")
    if not discoveries:
        raise ValueError("source manifest requires discoveries")
    verified = tuple(
        HeliusIndexedDiscoveryResult.model_validate(item.model_dump(mode="python"), context=_FACTORY)
        for item in discoveries
    )
    allowed = {item.query_digest for item in verified_plan.protocols}
    if any(item.protocol.query_digest not in allowed for item in verified):
        raise ValueError("discovery does not belong to frozen window plan")
    if len({item.protocol.query_digest for item in verified}) != len(verified):
        raise ValueError("source manifest has duplicate window discovery")
    coordinates = tuple(
        PumpCorpusV2SourceCoordinate.from_claim(claim)
        for result in verified
        for page in result.pages
        for claim in page.candidate_claims
    )
    payload = {
        "version": _CORPUS_V2_MANIFEST_VERSION,
        "plan_digest": verified_plan.plan_digest,
        "windows": [_source_window_receipt(item) for item in verified],
        "coordinates": [item.model_dump(mode="json") for item in coordinates],
    }
    manifest = PumpCorpusV2SourceManifest.model_validate(
        {**payload, "manifest_digest": _digest(payload)}, context=_FACTORY
    )
    manifest._factory_digest = manifest.manifest_digest
    manifest_path = root / _CORPUS_V2_MANIFEST_FILE
    if manifest_path.exists():
        raise FileExistsError("durable corpus V2 manifest path already exists")
    return _create_only_json(manifest_path, manifest.model_dump(mode="json"))


def recover_pump_corpus_v2_source_manifest(root: Path) -> tuple[PumpCorpusV2WindowPlan, PumpCorpusV2SourceManifest]:
    """Fresh-process recovery boundary; only durable JSON may supply V2 windows/coordinates."""
    try:
        plan = PumpCorpusV2WindowPlan.model_validate(
            json.loads((root / _CORPUS_V2_PLAN_FILE).read_text(encoding="utf-8")), context=_FACTORY
        )
        manifest = PumpCorpusV2SourceManifest.model_validate(
            json.loads((root / _CORPUS_V2_MANIFEST_FILE).read_text(encoding="utf-8")), context=_FACTORY
        )
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("durable corpus V2 evidence is unreadable") from error
    if manifest.plan_digest != plan.plan_digest:
        raise ValueError("source manifest does not bind recovered window plan")
    plan._factory_digest = plan.plan_digest
    manifest._factory_digest = manifest.manifest_digest
    return plan, manifest


__all__ = [
    "HeliusIndexedDiscoveryResult",
    "HeliusIndexedError",
    "HeliusIndexedHistoryClient",
    "HeliusIndexedPageReceipt",
    "HeliusIndexedPumpDiscoveryProtocolV1",
    "HeliusIndexedUsageLedger",
    "IndexedPumpCandidateClaim",
    "PumpCorpusV2SourceCoordinate",
    "PumpCorpusV2SourceManifest",
    "PumpCorpusV2WindowPlan",
    "recover_pump_corpus_v2_source_manifest",
    "sanitize_helius_endpoint",
    "write_pump_corpus_v2_source_manifest",
    "write_pump_corpus_v2_window_plan",
]
