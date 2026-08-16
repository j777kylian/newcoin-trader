"""Research package."""

from newcoin_trader.research.event_study_engine import run_event_study
from newcoin_trader.research.feature_research_features import build_decision_feature_record
from newcoin_trader.research.pipeline import analyze_listing
from newcoin_trader.research.windows import DEFAULT_WINDOWS, without_lookahead

__all__ = [
    "DEFAULT_WINDOWS",
    "analyze_listing",
    "build_decision_feature_record",
    "run_event_study",
    "without_lookahead",
]
