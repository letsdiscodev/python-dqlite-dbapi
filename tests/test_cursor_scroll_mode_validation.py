"""Pin: ``Cursor.scroll(value, mode)`` and ``AsyncCursor.scroll`` validate
``mode`` per PEP 249 §6.1.1 before raising ``NotSupportedError``.

The previous implementation accepted any string for ``mode`` and
raised ``NotSupportedError`` unconditionally, so a caller typo
(``cur.scroll(5, "absolutely")``) was indistinguishable from a
correct ``cur.scroll(5, "absolute")`` call. PEP 249 §6.1.1 enumerates
the legal values as ``{"relative", "absolute"}``; a non-conforming
``mode`` is a caller-side bug and should surface as
``ProgrammingError`` (inside the ``dbapi.Error`` hierarchy) rather
than be masked.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import NotSupportedError, ProgrammingError


def _make_sync_cursor() -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._description = None
    cur._rows = []
    cur._row_index = 0
    cur._rowcount = -1
    cur._lastrowid = None
    cur._arraysize = 1
    cur.messages = []
    cur._connection = MagicMock()
    cur._connection._closed = False
    cur._connection._check_thread = lambda: None
    cur._connection.messages = []
    return cur


def test_scroll_with_legal_mode_relative_raises_not_supported() -> None:
    cur = _make_sync_cursor()
    with pytest.raises(NotSupportedError, match="not scrollable"):
        cur.scroll(0, "relative")


def test_scroll_with_legal_mode_absolute_raises_not_supported() -> None:
    cur = _make_sync_cursor()
    with pytest.raises(NotSupportedError, match="not scrollable"):
        cur.scroll(0, "absolute")


def test_scroll_with_invalid_mode_raises_programming_error_not_not_supported() -> None:
    """A caller typo must surface as ProgrammingError, not be masked
    by the NotSupportedError that a correct call also raises."""
    cur = _make_sync_cursor()
    with pytest.raises(ProgrammingError, match="scroll mode must be"):
        cur.scroll(5, "absolutely")  # typo


def test_scroll_with_invalid_mode_caps_dont_skirt_check() -> None:
    cur = _make_sync_cursor()
    with pytest.raises(ProgrammingError, match="scroll mode must be"):
        cur.scroll(5, "RELATIVE")  # case matters per PEP 249


def test_scroll_default_mode_is_relative() -> None:
    """Default ``mode`` arg is ``"relative"`` per PEP 249 §6.1.1."""
    cur = _make_sync_cursor()
    with pytest.raises(NotSupportedError, match="not scrollable"):
        cur.scroll(0)


# ---------------- value-type validation (sibling discipline)
#
# PEP 249 §6.1.1 documents ``value`` as an integer offset. The
# project-wide validator family (``arraysize.setter``,
# ``_reject_non_sequence_params``, ``setinputsizes``) treats a
# misshapen value as a caller-side bug surfaced as ``ProgrammingError``.
# Without a value-type check, ``cur.scroll("five", "relative")`` slips
# past the mode validator and is masked by the unconditional
# ``NotSupportedError`` — the same diagnostic a correct call produces.


def test_scroll_with_string_value_raises_programming_error() -> None:
    cur = _make_sync_cursor()
    with pytest.raises(ProgrammingError, match="scroll value"):
        cur.scroll("five", "relative")  # type: ignore[arg-type]


def test_scroll_with_none_value_raises_programming_error() -> None:
    cur = _make_sync_cursor()
    with pytest.raises(ProgrammingError, match="scroll value"):
        cur.scroll(None, "relative")  # type: ignore[arg-type]


def test_scroll_with_bool_value_raises_programming_error() -> None:
    """``bool`` is-a ``int`` in Python; explicit reject matches the
    project standard from ``arraysize.setter``."""
    cur = _make_sync_cursor()
    with pytest.raises(ProgrammingError, match="scroll value"):
        cur.scroll(True, "relative")


def test_scroll_with_float_value_raises_programming_error() -> None:
    cur = _make_sync_cursor()
    with pytest.raises(ProgrammingError, match="scroll value"):
        cur.scroll(1.5, "relative")  # type: ignore[arg-type]


def test_scroll_with_int_value_reaches_not_supported() -> None:
    """Sanity: a legal ``int`` value still reaches the unconditional
    ``NotSupportedError`` (no value-check false positive)."""
    cur = _make_sync_cursor()
    with pytest.raises(NotSupportedError, match="not scrollable"):
        cur.scroll(-3, "relative")


# ------------- async sibling pins (moved from test_audit_2026_05_coverage_gaps.py
# so the file lives up to its docstring claim that ``AsyncCursor.scroll`` is
# covered here). ``AsyncCursor.scroll`` is a plain ``def`` (the
# project-standard "raise-fast" stub idiom — see
# ``tests/test_async_cursor.py:test_async_cursor_scroll_is_sync``), so the
# tests below call it without ``await``.


async def test_async_scroll_bad_mode_raises_programming_error() -> None:
    from dqlitedbapi.aio import AsyncConnection, AsyncCursor

    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    with pytest.raises(ProgrammingError):
        cur.scroll(0, mode="bad-mode")


async def test_async_scroll_bad_value_raises_programming_error() -> None:
    """Sibling-validator symmetry: ``value`` must be an integer offset
    per PEP 249 §6.1.1. Without this check, ``cur.scroll("five",
    "relative")`` is masked by the unconditional ``NotSupportedError``.
    """
    from dqlitedbapi.aio import AsyncConnection, AsyncCursor

    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    with pytest.raises(ProgrammingError, match="scroll value"):
        cur.scroll("five", "relative")  # type: ignore[arg-type]


async def test_async_scroll_bool_value_raises_programming_error() -> None:
    """``bool`` is-a ``int``; explicit reject matches the project
    standard from ``arraysize.setter``."""
    from dqlitedbapi.aio import AsyncConnection, AsyncCursor

    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    with pytest.raises(ProgrammingError, match="scroll value"):
        cur.scroll(True, "relative")
