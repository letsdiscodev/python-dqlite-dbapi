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
