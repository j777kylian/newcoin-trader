"""Application/service layer for discovery and ingestion."""

from newcoin_trader.services.ingestion import CollectOnceResult, IngestionService, PollController

__all__ = ["CollectOnceResult", "IngestionService", "PollController"]
