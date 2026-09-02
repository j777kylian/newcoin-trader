"""Pump V2 canonical identity validation; independent on-chain account evidence.

The corrected decoder names a mint/bonding-curve/associated-bonding-curve role.
This module validates those roles against logically independent finalized
getAccountInfo evidence (owner/type relationships), never against the same
decoder output that produced them.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, PrivateAttr, ValidationInfo, model_validator

from newcoin_trader.sources.pumpfun.evidence import (
    PUMP_PROGRAM_ADDRESS,
    PumpDecodedInstructionFact,
    PumpDecoderEvidence,
    PumpRawTransactionEvidence,
)

_IDENTITY_FACTORY = object()
_IDENTITY_MANIFEST_VERSION = "PumpCanonicalIdentityManifestV1"
_IDENTITY_MANIFEST_FILE = "canonical_identity_manifest_v2.json"
_DECODER_REGIME = "pumpfun-decoder-v1"

TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"


class PumpIdentityDisposition(StrEnum):
    VERIFIED_PUMP_CREATE = "VERIFIED_PUMP_CREATE"
    NOT_CREATE_OR_CREATE_V2 = "NOT_CREATE_OR_CREATE_V2"
    INVALID_MINT_ROLE = "INVALID_MINT_ROLE"
    INVALID_MINT_ACCOUNT_TYPE = "INVALID_MINT_ACCOUNT_TYPE"
    INVALID_BONDING_CURVE = "INVALID_BONDING_CURVE"
    BONDING_CURVE_PDA_MISMATCH = "BONDING_CURVE_PDA_MISMATCH"
    INVALID_ASSOCIATED_BONDING_CURVE = "INVALID_ASSOCIATED_BONDING_CURVE"
    WRONG_PROGRAM = "WRONG_PROGRAM"
    WRONG_METHOD = "WRONG_METHOD"
    SLOT_MISMATCH = "SLOT_MISMATCH"
    SIGNATURE_NOT_IN_FINALIZED_BLOCK = "SIGNATURE_NOT_IN_FINALIZED_BLOCK"
    FAILED_TRANSACTION = "FAILED_TRANSACTION"
    UNSUPPORTED_PROGRAM_REGIME = "UNSUPPORTED_PROGRAM_REGIME"
    UNSUPPORTED_TOKEN_PROGRAM = "UNSUPPORTED_TOKEN_PROGRAM"
    AMBIGUOUS_ACCOUNT_ROLE = "AMBIGUOUS_ACCOUNT_ROLE"
    CANONICAL_RPC_UNAVAILABLE = "CANONICAL_RPC_UNAVAILABLE"
    OTHER_EXPLICIT_SAFE_CATEGORY = "OTHER_EXPLICIT_SAFE_CATEGORY"


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


class PumpAccountInfoEvidence(BaseModel):
    """Digest-bound sanitized finalized getAccountInfo result for one account role."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    account: str
    owner: str
    parsed_type: str
    parsed_info: dict[str, Any]
    raw_account_digest: str

    @classmethod
    def from_get_account_info(cls, *, account: str, response: Mapping[str, object]) -> Self:
        value = response.get("value")
        if not isinstance(value, Mapping):
            raise ValueError("account not found at finalized query")
        owner = value.get("owner")
        if not isinstance(owner, str):
            raise ValueError("account owner is missing")
        data = value.get("data")
        if (
            isinstance(data, Mapping)
            and isinstance(data.get("parsed"), Mapping)
            and isinstance(data["parsed"].get("info"), Mapping)
        ):
            parsed = data["parsed"]
            info = dict(parsed["info"])
            parsed_type = str(parsed.get("type"))
        else:
            info = {}
            parsed_type = ""
        return cls(
            account=account,
            owner=owner,
            parsed_type=parsed_type,
            parsed_info=info,
            raw_account_digest=_digest({"account": account, "value": value}),
        )

    @model_validator(mode="after")
    def _bound(self) -> Self:
        if not isinstance(self.account, str) or not self.account:
            raise ValueError("account evidence requires an address")
        if not isinstance(self.owner, str) or not self.owner:
            raise ValueError("account evidence requires an owner")
        if not isinstance(self.raw_account_digest, str) or len(self.raw_account_digest) != 64:
            raise ValueError("account evidence requires a digest")
        return self


class PumpCanonicalIdentityRecord(BaseModel):
    """One source coordinate's independent semantic identity disposition."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    source_coordinate_digest: str
    signature: str
    slot: int
    transaction_index: int
    instruction_index: int
    inner_instruction_index: int | None = None
    method: str
    decoder_regime: str
    mint: str | None = None
    token_program: str | None = None
    bonding_curve: str | None = None
    associated_bonding_curve: str | None = None
    mint_authority: str | None = None
    disposition: PumpIdentityDisposition
    raw_account_digest: str | None = None
    identity_digest: str = ""
    _factory_digest: str | None = PrivateAttr(default=None)

    @classmethod
    def model_construct(cls, _fields_set: set[str] | None = None, **values: Any) -> Self:
        raise TypeError("PumpCanonicalIdentityRecord.model_construct is not a public construction boundary")

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        if self._factory_digest != self.identity_digest:
            raise ValueError("identity record lacks controlled construction evidence")
        rebuilt = type(self).model_validate(
            self.model_dump(mode="python") if update is None else {**self.model_dump(mode="python"), **update},
            context=_IDENTITY_FACTORY,
        )
        if rebuilt.identity_digest != self._factory_digest:
            raise ValueError("identity record factory binding mismatch")
        rebuilt._factory_digest = self._factory_digest
        return rebuilt

    @model_validator(mode="after")
    def _bound(self, info: ValidationInfo) -> Self:
        if info.context is not _IDENTITY_FACTORY:
            raise ValueError("identity records require controlled construction")
        if self.method not in {"create", "create_v2"}:
            raise ValueError("identity record method is invalid")
        if self.decoder_regime != _DECODER_REGIME:
            raise ValueError("identity record decoder regime is invalid")
        if self.disposition is PumpIdentityDisposition.VERIFIED_PUMP_CREATE:
            for value in (
                self.mint,
                self.token_program,
                self.bonding_curve,
                self.associated_bonding_curve,
            ):
                if not isinstance(value, str) or not value:
                    raise ValueError("verified identity requires full semantic fields")
            if self.raw_account_digest is None:
                raise ValueError("verified identity requires account evidence binding")
        payload = self.model_dump(mode="json", exclude={"identity_digest"})
        digest = _digest(payload)
        if self.identity_digest and self.identity_digest != digest:
            raise ValueError("identity record digest does not bind content")
        object.__setattr__(self, "identity_digest", digest)
        return self


class PumpCanonicalIdentityManifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    version: str
    source_manifest_digest: str
    records: tuple[PumpCanonicalIdentityRecord, ...]
    manifest_digest: str = ""
    _factory_digest: str | None = PrivateAttr(default=None)

    @classmethod
    def model_construct(cls, _fields_set: set[str] | None = None, **values: Any) -> Self:
        raise TypeError("PumpCanonicalIdentityManifest.model_construct is not a public construction boundary")

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        if self._factory_digest != self.manifest_digest:
            raise ValueError("identity manifest lacks controlled construction evidence")
        rebuilt = type(self).model_validate(
            self.model_dump(mode="python") if update is None else {**self.model_dump(mode="python"), **update},
            context=_IDENTITY_FACTORY,
        )
        rebuilt._factory_digest = self._factory_digest
        return rebuilt

    @model_validator(mode="after")
    def _bound(self, info: ValidationInfo) -> Self:
        if info.context is not _IDENTITY_FACTORY:
            raise ValueError("identity manifest requires controlled construction")
        if self.version != _IDENTITY_MANIFEST_VERSION:
            raise ValueError("unsupported identity manifest version")
        if not self.records or len({item.source_coordinate_digest for item in self.records}) != len(self.records):
            raise ValueError("identity manifest requires unique non-empty records")
        payload = self.model_dump(mode="json", exclude={"manifest_digest"})
        digest = _digest(payload)
        if self.manifest_digest and self.manifest_digest != digest:
            raise ValueError("identity manifest digest does not bind content")
        object.__setattr__(self, "manifest_digest", digest)
        return self


def token_program_for_instruction(
    decoder: PumpDecoderEvidence, fact: PumpDecodedInstructionFact, accounts: list[str]
) -> str:
    """Resolve the token program named account index for the decoded instruction kind."""
    if fact.instruction_kind not in {"create", "create_v2"}:
        raise ValueError("token program role only exists on launch instructions")
    names = next(
        (
            [account["name"] for account in instruction["accounts"]]
            for instruction in decoder.idl_content["instructions"]
            if instruction["name"] == fact.instruction_kind
        ),
        None,
    )
    if names is None or names.count("token_program") != 1:
        raise ValueError("decoder lacks a unique token_program role")
    index = names.index("token_program")
    if index >= len(accounts) or not isinstance(accounts[index], str) or not accounts[index]:
        raise ValueError("token program account is absent")
    return accounts[index]


def classify_pump_identity(
    *,
    source_coordinate_digest: str,
    signature: str,
    slot: int,
    transaction_index: int,
    fact: PumpDecodedInstructionFact,
    transaction: PumpRawTransactionEvidence,
    mint_info: Mapping[str, object],
    bonding_curve_info: Mapping[str, object],
    associated_info: Mapping[str, object],
    decoder: PumpDecoderEvidence,
) -> PumpCanonicalIdentityRecord:
    """Validate decoded roles against independent finalized account evidence."""
    accounts = transaction.message.get("instructions", [])[fact.instruction_index].get("accounts", [])
    base = {
        "source_coordinate_digest": source_coordinate_digest,
        "signature": signature,
        "slot": slot,
        "transaction_index": transaction_index,
        "instruction_index": fact.instruction_index,
        "inner_instruction_index": fact.inner_instruction_index,
        "method": fact.instruction_kind,
        "decoder_regime": _DECODER_REGIME,
    }

    def record(disposition: PumpIdentityDisposition, **extra: object) -> PumpCanonicalIdentityRecord:
        payload = {**base, **extra, "disposition": disposition}
        result = PumpCanonicalIdentityRecord.model_validate(payload, context=_IDENTITY_FACTORY)
        result._factory_digest = result.identity_digest
        return result

    if fact.instruction_kind not in {"create", "create_v2"}:
        return record(PumpIdentityDisposition.NOT_CREATE_OR_CREATE_V2)
    if fact.mint is None or fact.bonding_curve is None or fact.associated_bonding_curve is None:
        return record(PumpIdentityDisposition.AMBIGUOUS_ACCOUNT_ROLE)
    mint = fact.mint
    bonding_curve = fact.bonding_curve
    associated_bonding_curve = fact.associated_bonding_curve
    try:
        mint_evidence = PumpAccountInfoEvidence.from_get_account_info(account=mint, response=mint_info)
        bonding_evidence = PumpAccountInfoEvidence.from_get_account_info(
            account=bonding_curve, response=bonding_curve_info
        )
        associated_evidence = PumpAccountInfoEvidence.from_get_account_info(
            account=associated_bonding_curve, response=associated_info
        )
    except ValueError:
        return record(PumpIdentityDisposition.CANONICAL_RPC_UNAVAILABLE)

    try:
        token_program = token_program_for_instruction(decoder, fact, accounts)
    except ValueError:
        token_program = None

    mint_type = mint_evidence.parsed_type
    mint_owner = mint_evidence.owner
    if mint_type != "mint":
        return record(
            PumpIdentityDisposition.INVALID_MINT_ACCOUNT_TYPE,
            mint=mint,
            token_program=mint_owner,
            bonding_curve=bonding_curve,
            associated_bonding_curve=associated_bonding_curve,
            raw_account_digest=mint_evidence.raw_account_digest,
        )
    if mint_owner not in {TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID}:
        return record(
            PumpIdentityDisposition.UNSUPPORTED_TOKEN_PROGRAM,
            mint=mint,
            token_program=mint_owner,
            bonding_curve=bonding_curve,
            associated_bonding_curve=associated_bonding_curve,
            raw_account_digest=mint_evidence.raw_account_digest,
        )
    if token_program is not None and token_program != mint_owner:
        return record(
            PumpIdentityDisposition.AMBIGUOUS_ACCOUNT_ROLE,
            mint=mint,
            token_program=mint_owner,
            bonding_curve=bonding_curve,
            associated_bonding_curve=associated_bonding_curve,
            raw_account_digest=mint_evidence.raw_account_digest,
        )
    if bonding_evidence.owner != PUMP_PROGRAM_ADDRESS:
        return record(
            PumpIdentityDisposition.INVALID_BONDING_CURVE,
            mint=mint,
            token_program=mint_owner,
            bonding_curve=bonding_curve,
            associated_bonding_curve=associated_bonding_curve,
            mint_authority=mint_evidence.parsed_info.get("mintAuthority"),
            raw_account_digest=mint_evidence.raw_account_digest,
        )
    mint_authority = mint_evidence.parsed_info.get("mintAuthority")
    if isinstance(mint_authority, str) and mint_authority != bonding_curve:
        return record(
            PumpIdentityDisposition.INVALID_MINT_ROLE,
            mint=mint,
            token_program=mint_owner,
            bonding_curve=bonding_curve,
            associated_bonding_curve=associated_bonding_curve,
            mint_authority=mint_authority,
            raw_account_digest=mint_evidence.raw_account_digest,
        )
    associated_type = associated_evidence.parsed_type
    associated_owner = associated_evidence.owner
    associated_mint = associated_evidence.parsed_info.get("mint")
    associated_token_owner = associated_evidence.parsed_info.get("owner")
    if (
        associated_type != "account"
        or associated_owner != mint_owner
        or associated_mint != mint
        or associated_token_owner != bonding_curve
    ):
        return record(
            PumpIdentityDisposition.INVALID_ASSOCIATED_BONDING_CURVE,
            mint=mint,
            token_program=mint_owner,
            bonding_curve=bonding_curve,
            associated_bonding_curve=associated_bonding_curve,
            mint_authority=mint_authority if isinstance(mint_authority, str) else None,
            raw_account_digest=mint_evidence.raw_account_digest,
        )
    return record(
        PumpIdentityDisposition.VERIFIED_PUMP_CREATE,
        mint=mint,
        token_program=mint_owner,
        bonding_curve=bonding_curve,
        associated_bonding_curve=associated_bonding_curve,
        mint_authority=mint_authority if isinstance(mint_authority, str) else None,
        raw_account_digest=mint_evidence.raw_account_digest,
    )


def write_pump_corpus_v2_identity_manifest(
    root: Path, *, source_manifest_digest: str, records: tuple[PumpCanonicalIdentityRecord, ...]
) -> Path:
    """Create-only durable canonical identity manifest; refuses overwrite."""
    verified = tuple(
        PumpCanonicalIdentityRecord.model_validate(item.model_dump(mode="python"), context=_IDENTITY_FACTORY)
        for item in records
    )
    payload = {
        "version": _IDENTITY_MANIFEST_VERSION,
        "source_manifest_digest": source_manifest_digest,
        "records": [item.model_dump(mode="json") for item in verified],
    }
    manifest = PumpCanonicalIdentityManifest.model_validate(payload, context=_IDENTITY_FACTORY)
    manifest._factory_digest = manifest.manifest_digest
    path = root / _IDENTITY_MANIFEST_FILE
    if path.exists():
        raise FileExistsError("canonical identity manifest already exists")
    return _create_only_json(path, manifest.model_dump(mode="json"))


def recover_pump_corpus_v2_identity_manifest(root: Path) -> PumpCanonicalIdentityManifest:
    try:
        manifest = PumpCanonicalIdentityManifest.model_validate(
            json.loads((root / _IDENTITY_MANIFEST_FILE).read_text(encoding="utf-8")), context=_IDENTITY_FACTORY
        )
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("canonical identity manifest is unreadable") from error
    manifest._factory_digest = manifest.manifest_digest
    return manifest


def _create_only_json(path: Path, payload: dict[str, Any]) -> Path:
    import os

    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        handle.write("\n")
    return path


__all__ = [
    "PumpAccountInfoEvidence",
    "PumpCanonicalIdentityManifest",
    "PumpCanonicalIdentityRecord",
    "PumpIdentityDisposition",
    "TOKEN_2022_PROGRAM_ID",
    "TOKEN_PROGRAM_ID",
    "classify_pump_identity",
    "recover_pump_corpus_v2_identity_manifest",
    "token_program_for_instruction",
    "write_pump_corpus_v2_identity_manifest",
]
