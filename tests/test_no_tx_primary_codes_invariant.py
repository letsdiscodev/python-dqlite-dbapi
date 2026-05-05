"""Pin: ``_NO_TX_PRIMARY_CODES`` holds only primary SQLite codes
(< 256).

The lookup site at ``connection.py`` masks the incoming code via
``primary_sqlite_code(...)`` before set membership. Adding an
extended code (e.g. a hypothetical ``SQLITE_ERROR_RETRY = 513``)
directly to the set would silently never match because
``primary_sqlite_code(513) == 1`` while the set holds 513.

The module-level ``assert`` enforces this at import time, but
``assert`` is stripped under ``python -O``. This test is the
ride-along enforcement so CI catches violations even when the
import-time assert is disabled.
"""

from dqlitedbapi.connection import _NO_TX_PRIMARY_CODES


def test_no_tx_primary_codes_are_all_primary() -> None:
    for code in _NO_TX_PRIMARY_CODES:
        assert 0 <= code < 256, code
