"""Research package."""

from newcoin_trader.research.event_study_engine import run_event_study
from newcoin_trader.research.executable_backtest_engine import run_executable_backtest
from newcoin_trader.research.feature_research_features import build_decision_feature_record
from newcoin_trader.research.live_paper_engine import process_live_paper_session
from newcoin_trader.research.pipeline import analyze_listing
from newcoin_trader.research.windows import DEFAULT_WINDOWS, without_lookahead

__all__ = [
    "DEFAULT_WINDOWS",
    "analyze_listing",
    "build_decision_feature_record",
    "process_live_paper_session",
    "run_event_study",
    "run_executable_backtest",
    "without_lookahead",
]
