"""Phase 6 live-paper session/signal/position persistence (idempotent upserts)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from newcoin_trader.database.models import LivePaperPosition, LivePaperSession, LivePaperSignal
from newcoin_trader.domain.live_paper import LivePaperReport, LivePaperStatus


class LivePaperRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @staticmethod
    def merge_durable_state(prior: dict[str, Any] | None, report: LivePaperReport) -> dict[str, Any]:
        """Monotonic union of durable seen IDs; never wipe prior with an empty current report."""
        prior = dict(prior or {})
        extras_state = report.extras.get("durable_state") if isinstance(report.extras, dict) else None
        if isinstance(extras_state, dict):
            current_signals = set(str(x) for x in extras_state.get("seen_signals", ()))
            current_fills = set(str(x) for x in extras_state.get("seen_fills", ()))
            realized = str(extras_state.get("realized_pnl", report.portfolio.realized_pnl))
        else:
            current_signals = {s.signal_id for s in report.signals if s.status is LivePaperStatus.SIGNAL_ACCEPTED}
            current_fills = {f.fill_id for f in report.fills}
            realized = str(report.portfolio.realized_pnl)
        prior_signals = {str(x) for x in prior.get("seen_signals", ())}
        prior_fills = {str(x) for x in prior.get("seen_fills", ())}
        return {
            "seen_signals": sorted(prior_signals | current_signals),
            "seen_fills": sorted(prior_fills | current_fills),
            "realized_pnl": realized,
        }

    async def load_session_state(
        self,
        *,
        venue: str,
        rule_id: str,
        phase4_config_id: str,
        session_start: datetime,
    ) -> dict[str, Any]:
        row = await self._session.scalar(
            select(LivePaperSession).where(
                LivePaperSession.venue == venue,
                LivePaperSession.frozen_rule_id == rule_id,
                LivePaperSession.phase4_config_id == phase4_config_id,
                LivePaperSession.session_start == session_start,
            )
        )
        if row is None or not row.state_json:
            return {}
        return dict(row.state_json)

    async def persist_report(self, report: LivePaperReport) -> None:
        meta = report.meta
        prior_row = await self._session.scalar(
            select(LivePaperSession).where(LivePaperSession.session_id == meta.session_id)
        )
        prior_state = dict(prior_row.state_json) if prior_row is not None and prior_row.state_json else {}
        state = self.merge_durable_state(prior_state, report)
        sess_stmt = (
            insert(LivePaperSession)
            .values(
                session_id=meta.session_id,
                config_id=meta.config_id,
                venue=meta.venue,
                session_start=meta.session_start,
                session_end=meta.session_end,
                frozen_rule_id=meta.frozen_rule_id,
                phase4_config_id=meta.phase4_config_id,
                starting_cash=meta.starting_cash,
                realized_pnl=report.portfolio.realized_pnl,
                halted=str(meta.halted).lower(),
                state_json=state,
            )
            .on_conflict_do_update(
                constraint="uq_live_paper_sessions_session_id",
                set_={
                    "realized_pnl": report.portfolio.realized_pnl,
                    "halted": str(meta.halted).lower(),
                    "state_json": state,
                },
            )
        )
        await self._session.execute(sess_stmt)

        for signal in report.signals:
            sig_stmt = (
                insert(LivePaperSignal)
                .values(
                    session_id=signal.session_id,
                    signal_id=signal.signal_id,
                    event_id=signal.event_id,
                    rule_id=signal.rule_id,
                    decision_time=signal.decision_time,
                    status=signal.status.value,
                    reason=signal.reason.value if signal.reason else None,
                    provenance_json=dict(signal.provenance),
                )
                .on_conflict_do_nothing(constraint="uq_live_paper_signals_session_signal")
            )
            await self._session.execute(sig_stmt)

        for position in report.positions:
            pos_stmt = (
                insert(LivePaperPosition)
                .values(
                    session_id=position.session_id,
                    position_id=position.position_id,
                    signal_id=position.signal_id,
                    event_id=position.event_id,
                    token_address=position.token_address,
                    venue=position.venue.value,
                    lifecycle=position.lifecycle.value,
                    entry_notional=position.entry_notional,
                    entry_qty=position.entry_qty,
                    entry_price=position.entry_price,
                    exit_qty=position.exit_qty,
                    exit_price=position.exit_price,
                    realized_pnl=position.realized_pnl,
                    entry_time=position.entry_time,
                    exit_time=position.exit_time,
                    meta_json={
                        "label": position.label,
                        "remaining_qty": str(position.remaining_qty) if position.remaining_qty is not None else None,
                        "remaining_cost_basis": str(position.remaining_cost_basis)
                        if position.remaining_cost_basis is not None
                        else None,
                    },
                )
                .on_conflict_do_update(
                    constraint="uq_live_paper_positions_session_position",
                    set_={
                        "lifecycle": position.lifecycle.value,
                        "exit_qty": position.exit_qty,
                        "exit_price": position.exit_price,
                        "realized_pnl": position.realized_pnl,
                        "exit_time": position.exit_time,
                        "meta_json": {
                            "label": position.label,
                            "remaining_qty": str(position.remaining_qty)
                            if position.remaining_qty is not None
                            else None,
                            "remaining_cost_basis": str(position.remaining_cost_basis)
                            if position.remaining_cost_basis is not None
                            else None,
                        },
                    },
                )
            )
            await self._session.execute(pos_stmt)
        await self._session.flush()
