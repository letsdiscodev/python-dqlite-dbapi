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


def test_invariant_test_does_not_use_bare_assert() -> None:
    """Static-discipline pin against a future revert to bare
    ``assert``. The check above MUST use ``raise AssertionError`` so
    it survives ``python -O``; this test reads its own source and
    confirms the discipline is intact."""
    import pathlib
    import re

    src = pathlib.Path(__file__).read_text()
    # Match a bare ``assert 0 <= code`` at any indent — the previous
    # form. Must NOT appear in the source after this fix.
    assert re.search(r"^\s*assert\s+0\s*<=\s*code", src, re.MULTILINE) is None, (
        "test_no_tx_primary_codes_invariant.py uses a bare ``assert`` "
        "for the runtime invariant. Bare asserts strip under "
        "``python -O``; the runtime enforcement must use "
        "``raise AssertionError(...)`` so the invariant survives "
        "optimised CI runs."
    )
    # Positive: the explicit raise must be present.
    assert "raise AssertionError" in src
