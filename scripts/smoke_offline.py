#!/usr/bin/env python3
"""Offline smoke entrypoint. Fixtures only — no network, no database."""

from __future__ import annotations

import sys
from pathlib import Path

from newcoin_trader.demo import run_offline_smoke


def main() -> int:
    output = Path("artifacts")
    return run_offline_smoke(output_dir=output)


if __name__ == "__main__":
    sys.exit(main())
