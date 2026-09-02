"""Bounded Pump source qualification receipt; explicit invocation only, no collection."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, ValidationInfo, model_validator

from newcoin_trader.sources.pumpfun.evidence import (
    PUMP_PROGRAM_ADDRESS,
    PumpCandidateDispositionKind,
    PumpHistoricalFirstSuccessfulBuyFact,
    PumpMintSelectionProof,
    PumpPageRecordDisposition,
    PumpQualifiedSourceProfile,
    PumpRawBlockEvidence,
    PumpRawProgramDataEvidence,
    PumpRawSignaturePageEvidence,
    PumpRawTransactionEvidence,
    PumpSignatureCandidate,
    PumpSignaturePage,
    parse_pump_instruction,
    pinned_pump_decoder_evidence,
    select_first_successful_buy,
)
from newcoin_trader.sources.pumpfun.rpc import PumpRpcProvider, sanitize_pump_rpc_endpoint

_RECEIPT_FACTORY_CONTEXT = object()
_LIVE_SLICE_FACTORY_CONTEXT = object()


class PumpQualificationStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    INCONCLUSIVE = "INCONCLUSIVE"


class PumpQualificationConfig(BaseModel):
    """One small finalized page only; 500 is a hard operational ceiling."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    page_limit: int = 5
    max_pages: int = Field(default=1, strict=True)
    candidate_cap: int = 3
    lower_signature: str | None = None
    attempt_cap: int = 25

    @model_validator(mode="after")
    def _bounded(self) -> Self:
        if type(self.page_limit) is not int or not 1 <= self.page_limit <= 100:
            raise ValueError("Pump qualification page_limit must be in 1..100")
        if type(self.attempt_cap) is not int or not 1 <= self.attempt_cap <= 500:
            raise ValueError("Pump qualification attempt_cap must be in 1..500")
        if type(self.max_pages) is not int or self.max_pages != 1:
            raise ValueError("Pump qualification preflight supports exactly one page")
        if type(self.candidate_cap) is not int or not 1 <= self.candidate_cap <= 10:
            raise ValueError("Pump qualification candidate_cap must be in 1..10")
        return self


class PumpLiveSliceConfig(BaseModel):
    """Separate 8D.C one-page raw-evidence sample; never a corpus traversal."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    page_limit: int = 5
    candidate_cap: int = 3
    attempt_cap: int = 25

    @model_validator(mode="after")
    def _bounded(self) -> Self:
        if type(self.page_limit) is not int or not 1 <= self.page_limit <= 100:
            raise ValueError("Pump live slice page_limit must be in 1..100")
        if type(self.candidate_cap) is not int or not 1 <= self.candidate_cap <= 10:
            raise ValueError("Pump live slice candidate_cap must be in 1..10")
        if type(self.attempt_cap) is not int or not 1 <= self.attempt_cap <= 500:
            raise ValueError("Pump live slice attempt_cap must be in 1..500")
        return self


class PumpLiveSliceResult(BaseModel):
    """One retained raw-evidence page; source-only first-buy needs supplied complete proof."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider_origin: str
    profile: PumpQualifiedSourceProfile = Field(repr=False)
    signature_page: PumpSignaturePage = Field(repr=False)
    frozen_upper_slot: int
    rpc_attempts: int
    raw_binding_result: str
    price_evidence_result: str
    status: PumpQualificationStatus
    selection_proof: PumpMintSelectionProof | None = Field(default=None, repr=False, exclude=True)
    first_buy: PumpHistoricalFirstSuccessfulBuyFact | None = None

    _frozen_upper_evidence: int | None = PrivateAttr(default=None)
    _attempt_cap: int | None = PrivateAttr(default=None)
    _rpc_attempts: int | None = PrivateAttr(default=None)

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        if self._frozen_upper_evidence is None or self._attempt_cap is None or self._rpc_attempts is None:
            raise ValueError("Pump live slice lacks factory-bound operation evidence")
        values = dict(self.__dict__)
        rebuilt = type(self).model_validate(
            values if update is None else {**values, **update}, context=_LIVE_SLICE_FACTORY_CONTEXT
        )
        if (
            rebuilt.frozen_upper_slot != self._frozen_upper_evidence
            or rebuilt.rpc_attempts != self._rpc_attempts
            or rebuilt.rpc_attempts > self._attempt_cap
        ):
            raise ValueError("Pump live slice conflicts with factory-bound operation evidence")
        rebuilt._frozen_upper_evidence = self._frozen_upper_evidence
        rebuilt._attempt_cap = self._attempt_cap
        rebuilt._rpc_attempts = self._rpc_attempts
        return rebuilt

    @classmethod
    def model_construct(cls, _fields_set: set[str] | None = None, **values: Any) -> Self:
        raise TypeError("PumpLiveSliceResult.model_construct is not a public construction boundary")

    @model_validator(mode="after")
    def _bound(self, info: ValidationInfo) -> Self:
        if info.context is not _LIVE_SLICE_FACTORY_CONTEXT:
            raise ValueError("Pump live slice requires the controlled factory boundary")
        profile = PumpQualifiedSourceProfile.model_validate(self.profile.model_dump(mode="python"))
        page = PumpSignaturePage.model_validate(self.signature_page.model_dump(mode="python"))
        if (
            self.provider_origin != sanitize_pump_rpc_endpoint(self.provider_origin)
            or type(self.frozen_upper_slot) is not int
            or self.frozen_upper_slot < 0
            or type(self.rpc_attempts) is not int
            or not 0 <= self.rpc_attempts <= 500
            or self.raw_binding_result != "FAIL"
            or self.price_evidence_result != "DEFERRED"
            or self.status is not PumpQualificationStatus.INCONCLUSIVE
            or profile.programdata.last_deploy_slot > self.frozen_upper_slot
            or any(slot > self.frozen_upper_slot for slot in profile.programdata.context_slots)
            or any(item.slot > self.frozen_upper_slot for item in page.classifications)
        ):
            raise ValueError("Pump live slice result is invalid")
        if self.selection_proof is None:
            if self.first_buy is not None:
                raise ValueError("Pump first-buy requires a supplied bounded universe proof")
            return self
        proof = PumpMintSelectionProof.model_validate(self.selection_proof.model_dump(mode="python"))
        if proof.universe.profile != profile or proof.universe.pages != (page,):
            raise ValueError("Pump selection proof must bind this live page and profile")
        if self.first_buy != select_first_successful_buy(proof):
            raise ValueError("Pump first-buy must be selected from supplied bounded proof")
        return self

    @classmethod
    def _create(cls, *, _factory_context: object, **values: Any) -> Self:
        if _factory_context is not _LIVE_SLICE_FACTORY_CONTEXT:
            raise ValueError("Pump live slice requires the controlled factory boundary")
        payload = dict(values)
        frozen_upper_evidence = payload.pop("frozen_upper_evidence", None)
        attempt_cap = payload.pop("attempt_cap", None)
        if (
            type(frozen_upper_evidence) is not int
            or frozen_upper_evidence < 0
            or type(attempt_cap) is not int
            or not 1 <= attempt_cap <= 500
            or payload.get("frozen_upper_slot") != frozen_upper_evidence
            or payload.get("rpc_attempts", 0) > attempt_cap
        ):
            raise ValueError("Pump live slice factory evidence is invalid")
        result = cls.model_validate(payload, context=_LIVE_SLICE_FACTORY_CONTEXT)
        result._frozen_upper_evidence = frozen_upper_evidence
        result._attempt_cap = attempt_cap
        result._rpc_attempts = result.rpc_attempts
        return result


class PumpQualificationReceipt(BaseModel):
    """Digest-bound, public-safe result with no raw response or endpoint path/query."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider_origin: str
    program_address: str
    program_version: str
    frozen_lower_slot: int
    frozen_upper_slot: int
    finality: str
    candidate_count: int
    decoded_count: int
    first_buy_count: int
    ambiguous_count: int
    raw_binding_result: str
    raw_account_digest: str
    page_digest: str
    account_evidence: PumpRawProgramDataEvidence = Field(repr=False, exclude=True)
    page_evidence: tuple[PumpRawSignaturePageEvidence, ...] = Field(repr=False, exclude=True)
    price_evidence_result: str
    source_only_boundary_version: str
    rpc_attempts: int
    page_count: int
    terminal_cursor: str
    terminal_reason: str
    status: PumpQualificationStatus
    receipt_digest: str

    _frozen_slot_evidence: int | None = PrivateAttr(default=None)
    _attempt_cap: int | None = PrivateAttr(default=None)
    _rpc_attempts: int | None = PrivateAttr(default=None)

    @staticmethod
    def _digest_payload(values: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in values.items()
            if key
            not in {
                "receipt_digest",
                "account_evidence",
                "page_evidence",
            }
        }

    @staticmethod
    def _page_digest(pages: tuple[PumpRawSignaturePageEvidence, ...]) -> str:
        return hashlib.sha256(
            json.dumps([page.raw_payload_digest for page in pages], separators=(",", ":")).encode()
        ).hexdigest()

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        if self._frozen_slot_evidence is None or self._attempt_cap is None or self._rpc_attempts is None:
            raise ValueError("Pump qualification receipt lacks factory-bound operation evidence")
        values = dict(self.__dict__)
        rebuilt = type(self).model_validate(
            values if update is None else {**values, **update}, context=_RECEIPT_FACTORY_CONTEXT
        )
        if (
            rebuilt.frozen_upper_slot != self._frozen_slot_evidence
            or rebuilt.rpc_attempts != self._rpc_attempts
            or rebuilt.rpc_attempts > self._attempt_cap
        ):
            raise ValueError("Pump qualification receipt conflicts with factory-bound operation evidence")
        rebuilt._frozen_slot_evidence = self._frozen_slot_evidence
        rebuilt._attempt_cap = self._attempt_cap
        rebuilt._rpc_attempts = self._rpc_attempts
        return rebuilt

    @classmethod
    def model_construct(cls, _fields_set: set[str] | None = None, **values: Any) -> Self:
        raise TypeError("PumpQualificationReceipt.model_construct is not a public construction boundary")

    @model_validator(mode="after")
    def _bound(self, info: ValidationInfo) -> Self:
        if info.context is not _RECEIPT_FACTORY_CONTEXT:
            raise ValueError("Pump qualification receipts require the controlled factory boundary")
        if self.program_address != PUMP_PROGRAM_ADDRESS or self.finality != "finalized":
            raise ValueError("Pump qualification scope is invalid")
        if self.provider_origin != sanitize_pump_rpc_endpoint(self.provider_origin):
            raise ValueError("Pump qualification provider origin must be sanitized")
        if (
            type(self.frozen_lower_slot) is not int
            or type(self.frozen_upper_slot) is not int
            or self.frozen_lower_slot < 0
            or self.frozen_upper_slot < self.frozen_lower_slot
        ):
            raise ValueError("Pump frozen slot interval is invalid")
        if (
            any(
                type(value) is not int or value < 0
                for value in (
                    self.candidate_count,
                    self.decoded_count,
                    self.first_buy_count,
                    self.ambiguous_count,
                    self.rpc_attempts,
                )
            )
            or self.rpc_attempts > 500
        ):
            raise ValueError("Pump qualification attempt accounting is invalid")
        if self.terminal_reason != "page_cap_reached":
            raise ValueError("Pump qualification preflight stops only at its configured page cap")
        if type(self.page_count) is not int or self.page_count != 1:
            raise ValueError("Pump qualification preflight receipt must retain exactly one page")
        if (
            self.candidate_count != self.decoded_count + self.ambiguous_count
            or self.first_buy_count > self.decoded_count
        ):
            raise ValueError("Pump qualification counts do not reconcile")
        if self.raw_binding_result != "FAIL" or self.price_evidence_result not in {
            "NOT_QUALIFIED",
            "DEFERRED",
        }:
            raise ValueError("Pump qualification result is invalid")
        if self.status is not PumpQualificationStatus.INCONCLUSIVE:
            raise ValueError("Pump qualification status remains inconclusive until full evidence arrives")
        if any(
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            for digest in (self.raw_account_digest, self.page_digest, self.receipt_digest)
        ):
            raise ValueError("Pump qualification digest is invalid")
        account = PumpRawProgramDataEvidence.model_validate(self.account_evidence.model_dump(mode="python"))
        pages = tuple(
            PumpRawSignaturePageEvidence.model_validate(page.model_dump(mode="python")) for page in self.page_evidence
        )
        if (
            not pages
            or self.raw_account_digest != account.raw_account_digest
            or self.page_digest != self._page_digest(pages)
            or self.page_count != len(pages)
            or any(not page.records for page in pages)
            or self.terminal_cursor != pages[-1].records[-1]["signature"]
            or any(
                page.request_before != pages[index - 1].records[-1]["signature"]
                for index, page in enumerate(pages)
                if index > 0
            )
            or (self.terminal_reason == "exhausted" and len(pages[-1].records) >= pages[-1].limit)
        ):
            raise ValueError("Pump qualification receipt must bind actual raw account, page, and terminal evidence")
        page_slots = tuple(_slot(record["slot"]) for page in pages for record in page.records)
        if (
            account.last_deploy_slot > self.frozen_upper_slot
            or any(slot > self.frozen_upper_slot for slot in account.context_slots)
            or any(slot > self.frozen_upper_slot for slot in page_slots)
            or self.frozen_lower_slot != min(page_slots)
        ):
            raise ValueError("Pump qualification receipt evidence exceeds or disagrees with frozen slot interval")
        payload = self._digest_payload(self.model_dump(mode="json"))
        expected = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()
        if self.receipt_digest != expected:
            raise ValueError("Pump qualification receipt digest does not bind content")
        return self

    @classmethod
    def _create(cls, *, _factory_context: object, **values: Any) -> Self:
        if _factory_context is not _RECEIPT_FACTORY_CONTEXT:
            raise ValueError("Pump qualification receipt requires the controlled factory boundary")
        payload = dict(values)
        frozen_slot_evidence = payload.pop("frozen_slot_evidence", None)
        attempt_cap = payload.pop("attempt_cap", None)
        if (
            type(frozen_slot_evidence) is not int
            or frozen_slot_evidence < 0
            or type(attempt_cap) is not int
            or not 1 <= attempt_cap <= 500
            or payload.get("frozen_upper_slot") != frozen_slot_evidence
            or payload.get("rpc_attempts", 0) > attempt_cap
        ):
            raise ValueError("Pump qualification receipt factory evidence is invalid")
        account = payload.get("account_evidence")
        pages = payload.get("page_evidence")
        if not isinstance(account, PumpRawProgramDataEvidence) or not isinstance(pages, tuple):
            raise ValueError("Pump qualification receipt requires evidence models")
        validated_pages = tuple(
            PumpRawSignaturePageEvidence.model_validate(page.model_dump(mode="python")) for page in pages
        )
        payload["raw_account_digest"] = account.raw_account_digest
        payload["page_digest"] = cls._page_digest(validated_pages)
        payload["raw_binding_result"] = "FAIL"
        payload["status"] = PumpQualificationStatus.INCONCLUSIVE
        payload["receipt_digest"] = hashlib.sha256(
            json.dumps(cls._digest_payload(payload), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()
        receipt = cls.model_validate(payload, context=_RECEIPT_FACTORY_CONTEXT)
        receipt._frozen_slot_evidence = frozen_slot_evidence
        receipt._attempt_cap = attempt_cap
        receipt._rpc_attempts = receipt.rpc_attempts
        return receipt


def _slot(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("Pump finalized slot response is invalid")
    return value


async def _classify_page(
    provider: PumpRpcProvider,
    page: PumpRawSignaturePageEvidence,
    *,
    decoder: Any,
    upper: int,
    budget: list[int],
    remaining_candidates: int,
) -> tuple[PumpSignaturePage, int]:
    classifications: list[PumpPageRecordDisposition] = []
    for record in page.records:
        signature, slot = record["signature"], _slot(record["slot"])
        if record["err"] is not None:
            classifications.append(
                PumpPageRecordDisposition(
                    signature=signature, slot=slot, disposition=PumpCandidateDispositionKind.FAILED_TRANSACTION
                )
            )
            continue
        if record["blockTime"] is None or remaining_candidates < 1:
            classifications.append(
                PumpPageRecordDisposition(
                    signature=signature,
                    slot=slot,
                    disposition=(
                        PumpCandidateDispositionKind.NULL_BLOCK_TIME
                        if record["blockTime"] is None
                        else PumpCandidateDispositionKind.CANDIDATE_CAP_REACHED
                    ),
                )
            )
            continue
        remaining_candidates -= 1
        transaction_response = (
            await provider.call(
                "getTransaction",
                [
                    signature,
                    {"commitment": "finalized", "encoding": "jsonParsed", "maxSupportedTransactionVersion": 0},
                ],
                attempt_budget=budget,
            )
        ).result
        block_response = (
            await provider.call(
                "getBlock",
                [
                    slot,
                    {
                        "commitment": "finalized",
                        "encoding": "json",
                        "transactionDetails": "signatures",
                        "maxSupportedTransactionVersion": 0,
                    },
                ],
                attempt_budget=budget,
            )
        ).result
        try:
            if not isinstance(transaction_response, Mapping) or not isinstance(block_response, Mapping):
                raise ValueError("unsupported raw candidate")
            transaction = PumpRawTransactionEvidence.from_get_transaction(
                signature=signature, commitment="finalized", response=transaction_response
            )
            block = PumpRawBlockEvidence.from_get_block(slot=slot, commitment="finalized", response=block_response)
            if slot > upper:
                raise ValueError("candidate exceeds frozen upper slot")
            facts = tuple(
                parse_pump_instruction(transaction, decoder, instruction_index=index)
                for index in range(len(transaction.message["instructions"]))
            )
            facts = tuple(fact for fact in facts if fact.instruction_kind in {"create", "create_v2", "buy"})
            candidate = PumpSignatureCandidate(
                signature=signature,
                slot=slot,
                transaction_index=block.signatures.index(signature),
                source_time=record["blockTime"],
                raw_transaction=transaction,
                raw_block=block,
                decoder=decoder,
                instruction_facts=facts,
            )
            classifications.append(
                PumpPageRecordDisposition(
                    signature=signature,
                    slot=slot,
                    disposition=PumpCandidateDispositionKind.ELIGIBLE_DECODED,
                    candidate=candidate,
                )
            )
        except (KeyError, ValueError):
            classifications.append(
                PumpPageRecordDisposition(
                    signature=signature,
                    slot=slot,
                    disposition=PumpCandidateDispositionKind.UNSUPPORTED_OR_NO_RELEVANT_INSTRUCTION,
                )
            )
    return PumpSignaturePage(raw_page=page, classifications=tuple(classifications)), remaining_candidates


async def acquire_pump_live_slice(
    provider: PumpRpcProvider,
    *,
    config: PumpLiveSliceConfig | None = None,
    selection_proof: PumpMintSelectionProof | None = None,
) -> PumpLiveSliceResult:
    """Acquire one finalized Pump page and a capped raw transaction/block sample only."""
    config = PumpLiveSliceConfig.model_validate((config or PumpLiveSliceConfig()).model_dump(mode="python"))
    budget = [config.attempt_cap]
    pre_read_upper = _slot(
        (await provider.call("getSlot", [{"commitment": "finalized"}], attempt_budget=budget)).result
    )
    program_response = (
        await provider.call(
            "getAccountInfo",
            [PUMP_PROGRAM_ADDRESS, {"commitment": "finalized", "encoding": "jsonParsed", "withContext": True}],
            attempt_budget=budget,
        )
    ).result
    try:
        programdata_address = program_response["value"]["data"]["parsed"]["info"]["programData"]  # type: ignore[index]
    except (KeyError, TypeError):
        raise ValueError("Pump Program account response is invalid") from None
    if not isinstance(programdata_address, str):
        raise ValueError("Pump ProgramData pointer is invalid")
    programdata_response = (
        await provider.call(
            "getAccountInfo",
            [programdata_address, {"commitment": "finalized", "encoding": "jsonParsed", "withContext": True}],
            attempt_budget=budget,
        )
    ).result
    programdata = PumpRawProgramDataEvidence.from_program_accounts(
        program_address=PUMP_PROGRAM_ADDRESS,
        programdata_address=programdata_address,
        program_response=program_response,  # type: ignore[arg-type]
        programdata_response=programdata_response,  # type: ignore[arg-type]
    )
    decoder = pinned_pump_decoder_evidence()
    profile = PumpQualifiedSourceProfile(programdata=programdata, decoder=decoder, idl_digest=decoder.idl_digest)
    page_response = (
        await provider.call(
            "getSignaturesForAddress",
            [PUMP_PROGRAM_ADDRESS, {"commitment": "finalized", "limit": config.page_limit}],
            attempt_budget=budget,
        )
    ).result
    page = PumpRawSignaturePageEvidence.from_get_signatures_for_address(
        before=None, limit=config.page_limit, response=page_response
    )
    signature_page, _ = await _classify_page(
        provider, page, decoder=decoder, upper=2**63 - 1, budget=budget, remaining_candidates=config.candidate_cap
    )
    required_upper = max(
        pre_read_upper,
        programdata.last_deploy_slot,
        *programdata.context_slots,
        *(_slot(record["slot"]) for record in page.records),
    )
    upper = _slot((await provider.call("getSlot", [{"commitment": "finalized"}], attempt_budget=budget)).result)
    while upper < required_upper:
        upper = _slot((await provider.call("getSlot", [{"commitment": "finalized"}], attempt_budget=budget)).result)
    if upper < required_upper:
        raise ValueError("Pump live slice evidence exceeds frozen finalized upper slot")
    proof = (
        None
        if selection_proof is None
        else PumpMintSelectionProof.model_validate(selection_proof.model_dump(mode="python"))
    )
    first_buy = None if proof is None else select_first_successful_buy(proof)
    return PumpLiveSliceResult._create(
        _factory_context=_LIVE_SLICE_FACTORY_CONTEXT,
        provider_origin=provider.provider_origin,
        profile=profile,
        signature_page=signature_page,
        frozen_upper_slot=upper,
        frozen_upper_evidence=upper,
        rpc_attempts=config.attempt_cap - budget[0],
        attempt_cap=config.attempt_cap,
        raw_binding_result="FAIL",
        price_evidence_result="DEFERRED",
        status=PumpQualificationStatus.INCONCLUSIVE,
        selection_proof=proof,
        first_buy=first_buy,
    )


async def qualify_pump_source(
    provider: PumpRpcProvider, *, config: PumpQualificationConfig | None = None
) -> PumpQualificationReceipt:
    """Freeze finalized upper slot, bind ProgramData, then fetch one bounded signature page.

    This is an explicit runtime operation. It never follows the page, fetches transactions,
    builds a corpus, or derives price evidence.
    """
    config = PumpQualificationConfig.model_validate((config or PumpQualificationConfig()).model_dump(mode="python"))
    budget = [config.attempt_cap]
    pre_read_upper = _slot(
        (await provider.call("getSlot", [{"commitment": "finalized"}], attempt_budget=budget)).result
    )
    program_response = (
        await provider.call(
            "getAccountInfo",
            [PUMP_PROGRAM_ADDRESS, {"commitment": "finalized", "encoding": "jsonParsed", "withContext": True}],
            attempt_budget=budget,
        )
    ).result
    try:
        programdata_address = program_response["value"]["data"]["parsed"]["info"]["programData"]  # type: ignore[index]
    except (KeyError, TypeError):
        raise ValueError("Pump Program account response is invalid") from None
    if not isinstance(programdata_address, str):
        raise ValueError("Pump ProgramData pointer is invalid")
    programdata_response = (
        await provider.call(
            "getAccountInfo",
            [programdata_address, {"commitment": "finalized", "encoding": "jsonParsed", "withContext": True}],
            attempt_budget=budget,
        )
    ).result
    programdata = PumpRawProgramDataEvidence.from_program_accounts(
        program_address=PUMP_PROGRAM_ADDRESS,
        programdata_address=programdata_address,
        program_response=program_response,  # type: ignore[arg-type]
        programdata_response=programdata_response,  # type: ignore[arg-type]
    )
    profile = PumpQualifiedSourceProfile(
        programdata=programdata,
        decoder=pinned_pump_decoder_evidence(),
        idl_digest=pinned_pump_decoder_evidence().idl_digest,
    )
    page_response = (
        await provider.call(
            "getSignaturesForAddress",
            [PUMP_PROGRAM_ADDRESS, {"commitment": "finalized", "limit": config.page_limit}],
            attempt_budget=budget,
        )
    ).result
    page = PumpRawSignaturePageEvidence.from_get_signatures_for_address(
        before=None, limit=config.page_limit, response=page_response
    )
    slots = tuple(_slot(record["slot"]) for record in page.records)
    required_upper = max(pre_read_upper, programdata.last_deploy_slot, *programdata.context_slots, *slots)
    upper = _slot((await provider.call("getSlot", [{"commitment": "finalized"}], attempt_budget=budget)).result)
    while upper < required_upper:
        upper = _slot((await provider.call("getSlot", [{"commitment": "finalized"}], attempt_budget=budget)).result)
    if upper < required_upper:
        raise ValueError("Pump qualification evidence exceeds frozen finalized upper slot")
    candidates = len(page.records)
    return PumpQualificationReceipt._create(
        _factory_context=_RECEIPT_FACTORY_CONTEXT,
        provider_origin=provider.provider_origin,
        program_address=profile.program_address,
        program_version=profile.source_version,
        frozen_lower_slot=min(slots),
        frozen_upper_slot=upper,
        frozen_slot_evidence=upper,
        finality=profile.finality_commitment,
        candidate_count=candidates,
        decoded_count=0,
        first_buy_count=0,
        ambiguous_count=candidates,
        raw_binding_result="FAIL",
        raw_account_digest=programdata.raw_account_digest,
        page_digest=page.raw_payload_digest,
        account_evidence=programdata,
        page_evidence=(page,),
        price_evidence_result="DEFERRED",
        source_only_boundary_version="pumpfun-source-time-only-v3",
        rpc_attempts=config.attempt_cap - budget[0],
        attempt_cap=config.attempt_cap,
        page_count=1,
        terminal_cursor=page.records[-1]["signature"],
        terminal_reason="page_cap_reached",
        status=PumpQualificationStatus.INCONCLUSIVE,
    )


__all__ = [
    "PumpLiveSliceConfig",
    "PumpLiveSliceResult",
    "PumpQualificationConfig",
    "PumpQualificationReceipt",
    "PumpQualificationStatus",
    "acquire_pump_live_slice",
    "qualify_pump_source",
]
