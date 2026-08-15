"""Shared pytest configuration. Tests never load a real .env file."""

from __future__ import annotations

import os

# Keep collector/database tests hermetic even if the host shell has secrets set.
os.environ.setdefault("EXECUTION_MODE", "paper")
os.environ.setdefault("BIRDEYE_API_KEY", "")
