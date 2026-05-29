"""``Cursor.scroll`` / ``AsyncCursor.scroll`` validate ``mode`` per PEP 249 §6.1.1
before raising ``NotSupportedError``, so a typo'd mode surfaces as ``ProgrammingError``
rather than being masked by the unconditional ``NotSupportedError``.
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
    cur = _make_sync_cursor()
    with pytest.raises(ProgrammingError, match="scroll mode must be"):
        cur.scroll(5, "absolutely")


def test_scroll_with_invalid_mode_caps_dont_skirt_check() -> None:
    cur = _make_sync_cursor()
    with pytest.raises(ProgrammingError, match="scroll mode must be"):
        cur.scroll(5, "RELATIVE")  # case matters per PEP 249


def test_scroll_default_mode_is_relative() -> None:
    cur = _make_sync_cursor()
    with pytest.raises(NotSupportedError, match="not scrollable"):
        cur.scroll(0)


# value-type validation: a misshapen ``value`` must surface as ProgrammingError, not be
# masked by the unconditional NotSupportedError (matches the sibling-validator family).


def test_scroll_with_string_value_raises_programming_error() -> None:
    cur = _make_sync_cursor()
    with pytest.raises(ProgrammingError, match="scroll value"):
        cur.scroll("five", "relative")  # type: ignore[arg-type]


def test_scroll_with_none_value_raises_programming_error() -> None:
    cur = _make_sync_cursor()
    with pytest.raises(ProgrammingError, match="scroll value"):
        cur.scroll(None, "relative")  # type: ignore[arg-type]


def test_scroll_with_bool_value_raises_programming_error() -> None:
    """``bool`` is-a ``int``; explicit reject matches ``arraysize.setter``."""
    cur = _make_sync_cursor()
    with pytest.raises(ProgrammingError, match="scroll value"):
        cur.scroll(True, "relative")


def test_scroll_with_float_value_raises_programming_error() -> None:
    cur = _make_sync_cursor()
    with pytest.raises(ProgrammingError, match="scroll value"):
        cur.scroll(1.5, "relative")  # type: ignore[arg-type]


def test_scroll_with_int_value_reaches_not_supported() -> None:
    """Sanity: a legal ``int`` still reaches NotSupportedError (no value-check false positive)."""
    cur = _make_sync_cursor()
    with pytest.raises(NotSupportedError, match="not scrollable"):
        cur.scroll(-3, "relative")


# ``AsyncCursor.scroll`` is a plain ``def`` (raise-fast stub), so call it without ``await``.


async def test_async_scroll_bad_mode_raises_programming_error() -> None:
    from dqlitedbapi.aio import AsyncConnection, AsyncCursor

    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    with pytest.raises(ProgrammingError):
        cur.scroll(0, "bad-mode")


async def test_async_scroll_bad_value_raises_programming_error() -> None:
    from dqlitedbapi.aio import AsyncConnection, AsyncCursor

    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    with pytest.raises(ProgrammingError, match="scroll value"):
        cur.scroll("five", "relative")  # type: ignore[arg-type]


async def test_async_scroll_bool_value_raises_programming_error() -> None:
    from dqlitedbapi.aio import AsyncConnection, AsyncCursor

    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    with pytest.raises(ProgrammingError, match="scroll value"):
        cur.scroll(True, "relative")
