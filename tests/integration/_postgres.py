"""Explicit database selection for PostgreSQL integration tests."""

from __future__ import annotations

import os


def get_test_database_url() -> str:
    """Return only the dedicated PostgreSQL integration-test database URL.

    Tests must never use ``DATABASE_URL`` because it may identify the
    application/research database.
    """
    return os.environ.get("NEWCOIN_TEST_DATABASE_URL", "")
