"""Pin: ``_NO_TX_PRIMARY_CODES`` holds only primary SQLite codes
(< 256).

The lookup site at ``connection.py`` masks the incoming code via
``primary_sqlite_code(...)`` before set membership. Adding an
extended code (e.g. a hypothetical ``SQLITE_ERROR_RETRY = 513``)
directly to the set would silently never match because
``primary_sqlite_code(513) == 1`` while the set holds 513.

The module-level ``if __debug__: assert`` is documentation-only
(both that guard AND a bare ``assert`` here would be stripped
under ``python -O``). This test is the runtime enforcement and
must survive ``-O`` stripping — use an explicit raise instead of
``assert`` so a future contributor running CI under ``-O`` sees
the same failure as a non-optimised run.
"""

from dqlitedbapi.connection import _NO_TX_PRIMARY_CODES


def test_no_tx_primary_codes_are_all_primary() -> None:
    """Use ``raise AssertionError`` (not ``assert``) so the check
    survives ``python -O`` stripping. A future contributor reverting
    to a bare ``assert`` would silently disable the invariant when
    pytest is run under ``-O``."""
    for code in _NO_TX_PRIMARY_CODES:
        if not (0 <= code < 256):
            raise AssertionError(
                f"_NO_TX_PRIMARY_CODES entry {code} is not a primary "
                f"code (must be 0-255). The mask & 0xFF lookup at "
                f"the call site depends on this invariant."
            )
