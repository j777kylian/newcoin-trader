"""Paper-only execution package."""

from newcoin_trader.execution.gateway import ExecutionGateway
from newcoin_trader.execution.paper_broker import PaperBroker
from newcoin_trader.execution.safety import ensure_paper_mode

__all__ = ["ExecutionGateway", "PaperBroker", "ensure_paper_mode"]
