"""Append-only in-memory scan ledger for Base Uniswap V3 log scans."""

from __future__ import annotations

from newcoin_trader.sources.base_uniswap_v3.models import (
    FAILURE_STATUSES,
    CapAmbiguityError,
    CapPolicy,
    ScanKind,
    ScanLedgerEntry,
    ScanStatus,
    require_non_bool_int,
)


class InMemoryScanLedger:
    """Append-only ledger. Existing entries are never mutated."""

    def __init__(self) -> None:
        self._entries: list[ScanLedgerEntry] = []

    @property
    def entries(self) -> tuple[ScanLedgerEntry, ...]:
        return tuple(self._entries)

    @staticmethod
    def detect_possible_truncation(*, response_count: object, configured_cap: object) -> bool:
        cap = require_non_bool_int(configured_cap, field_name="configured_cap", minimum=1)
        count = require_non_bool_int(response_count, field_name="response_count", minimum=0)
        return count >= cap

    def append_incomplete(
        self,
        *,
        scan_id: str,
        scan_kind: ScanKind,
        from_block: int,
        to_block: int,
        configured_cap: int,
        address: str | None = None,
        topic0: str | None = None,
        parent_scan_id: str | None = None,
        response_digest: str = "",
        cap_policy: CapPolicy = CapPolicy.REFUSE_ON_HIT,
        provider_endpoint: str = "",
        provider_version: str = "",
        attempt: int = 0,
        split_depth: int = 0,
        note: str | None = None,
    ) -> ScanLedgerEntry:
        entry = ScanLedgerEntry.model_validate(
            {
                "scan_id": scan_id,
                "parent_scan_id": parent_scan_id,
                "scan_kind": scan_kind,
                "status": ScanStatus.INCOMPLETE,
                "from_block": from_block,
                "to_block": to_block,
                "response_count": 0,
                "response_digest": response_digest,
                "configured_cap": configured_cap,
                "cap_policy": cap_policy,
                "possible_truncation": False,
                "address": address,
                "topic0": topic0,
                "provider_endpoint": provider_endpoint,
                "provider_version": provider_version,
                "attempt": attempt,
                "split_depth": split_depth,
                "note": note,
            }
        )
        self._entries.append(entry)
        return entry

    def mark_completed(
        self,
        *,
        scan_id: str,
        scan_kind: ScanKind,
        from_block: int,
        to_block: int,
        response_count: int,
        configured_cap: int,
        address: str | None = None,
        topic0: str | None = None,
        parent_scan_id: str | None = None,
        response_digest: str = "",
        cap_policy: CapPolicy = CapPolicy.REFUSE_ON_HIT,
        provider_endpoint: str = "",
        provider_version: str = "",
        attempt: int = 0,
        split_depth: int = 0,
        note: str | None = None,
    ) -> ScanLedgerEntry:
        truncated = self.detect_possible_truncation(
            response_count=response_count,
            configured_cap=configured_cap,
        )
        if truncated:
            failed = ScanLedgerEntry.model_validate(
                {
                    "scan_id": scan_id,
                    "parent_scan_id": parent_scan_id,
                    "scan_kind": scan_kind,
                    "status": ScanStatus.FAILED_CAP_AMBIGUITY,
                    "from_block": from_block,
                    "to_block": to_block,
                    "response_count": response_count,
                    "response_digest": response_digest,
                    "configured_cap": configured_cap,
                    "cap_policy": cap_policy,
                    "possible_truncation": True,
                    "address": address,
                    "topic0": topic0,
                    "provider_endpoint": provider_endpoint,
                    "provider_version": provider_version,
                    "attempt": attempt,
                    "split_depth": split_depth,
                    "note": note if note is not None else "cap-hitting response refused as completed",
                }
            )
            self._entries.append(failed)
            raise CapAmbiguityError(failed)

        status = ScanStatus.COMPLETED_EMPTY if response_count == 0 else ScanStatus.COMPLETED_NONEMPTY
        entry = ScanLedgerEntry.model_validate(
            {
                "scan_id": scan_id,
                "parent_scan_id": parent_scan_id,
                "scan_kind": scan_kind,
                "status": status,
                "from_block": from_block,
                "to_block": to_block,
                "response_count": response_count,
                "response_digest": response_digest,
                "configured_cap": configured_cap,
                "cap_policy": cap_policy,
                "possible_truncation": False,
                "address": address,
                "topic0": topic0,
                "provider_endpoint": provider_endpoint,
                "provider_version": provider_version,
                "attempt": attempt,
                "split_depth": split_depth,
                "note": note,
            }
        )
        self._entries.append(entry)
        return entry

    def mark_failed(
        self,
        *,
        scan_id: str,
        scan_kind: ScanKind,
        status: ScanStatus,
        from_block: int,
        to_block: int,
        response_count: int,
        configured_cap: int,
        address: str | None = None,
        topic0: str | None = None,
        parent_scan_id: str | None = None,
        response_digest: str = "",
        cap_policy: CapPolicy = CapPolicy.REFUSE_ON_HIT,
        provider_endpoint: str = "",
        provider_version: str = "",
        attempt: int = 0,
        split_depth: int = 0,
        note: str | None = None,
    ) -> ScanLedgerEntry:
        if status not in FAILURE_STATUSES:
            raise ValueError("mark_failed only permits FAILED_CAP_AMBIGUITY, FAILED_PROVIDER, or INCOMPLETE")
        possible_truncation = False
        if status is ScanStatus.FAILED_CAP_AMBIGUITY:
            possible_truncation = self.detect_possible_truncation(
                response_count=response_count,
                configured_cap=configured_cap,
            )
        entry = ScanLedgerEntry.model_validate(
            {
                "scan_id": scan_id,
                "parent_scan_id": parent_scan_id,
                "scan_kind": scan_kind,
                "status": status,
                "from_block": from_block,
                "to_block": to_block,
                "response_count": response_count,
                "response_digest": response_digest,
                "configured_cap": configured_cap,
                "cap_policy": cap_policy,
                "possible_truncation": possible_truncation,
                "address": address,
                "topic0": topic0,
                "provider_endpoint": provider_endpoint,
                "provider_version": provider_version,
                "attempt": attempt,
                "split_depth": split_depth,
                "note": note,
            }
        )
        self._entries.append(entry)
        return entry


__all__ = ["InMemoryScanLedger"]
