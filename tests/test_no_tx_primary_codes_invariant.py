"""Pin: ``_NO_TX_PRIMARY_CODES`` holds only primary SQLite codes (< 256).

The lookup site masks via ``primary_sqlite_code(...)`` first, so an extended
code added to the set would never match. Enforced with ``raise`` (not
``assert``) so it survives ``python -O`` stripping.
"""

from dqlitedbapi.connection import _NO_TX_PRIMARY_CODES


def test_no_tx_primary_codes_are_all_primary() -> None:
    """Use ``raise`` (not ``assert``) so the invariant survives ``python -O``."""
    for code in _NO_TX_PRIMARY_CODES:
        if not (0 <= code < 256):
            raise AssertionError(
                f"_NO_TX_PRIMARY_CODES entry {code} is not a primary "
                f"code (must be 0-255). The mask & 0xFF lookup at "
                f"the call site depends on this invariant."
            )


def test_invariant_test_does_not_use_bare_assert() -> None:
    """Source pin: the invariant check above must use ``raise``, not bare ``assert``."""
    import pathlib
    import re

    src = pathlib.Path(__file__).read_text()
    if re.search(r"^\s*assert\s+0\s*<=\s*code", src, re.MULTILINE) is not None:
        raise AssertionError(
            "test_no_tx_primary_codes_invariant.py uses a bare ``assert`` "
            "for the runtime invariant. Bare asserts strip under "
            "``python -O``; the runtime enforcement must use "
            "``raise AssertionError(...)`` so the invariant survives "
            "optimised CI runs."
        )
    if "raise AssertionError" not in src:
        raise AssertionError(
            "test_no_tx_primary_codes_invariant.py must use "
            "``raise AssertionError`` for the runtime invariant check "
            "so the discipline survives ``python -O`` stripping."
        )
