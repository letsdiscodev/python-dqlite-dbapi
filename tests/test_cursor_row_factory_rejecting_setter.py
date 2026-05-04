"""``Cursor.row_factory`` and ``AsyncCursor.row_factory`` are
read+writable Python-side hooks matching stdlib
``sqlite3.Cursor.row_factory``. Setter accepts callable or None;
non-callable input raises ``ProgrammingError`` (inside the PEP 249
``Error`` hierarchy).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import dqlitedbapi
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def _make_sync_cursor() -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._description = None
    cur._rows = []
    cur._row_index = 0
    cur._rowcount = -1
    cur._lastrowid = None
    cur._arraysize = 1
    cur._row_factory = None
    cur.messages = []
    cur._connection = MagicMock()
    return cur


def _make_async_cursor() -> AsyncCursor:
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._description = None
    cur._rows = []
    cur._row_index = 0
    cur._rowcount = -1
    cur._lastrowid = None
    cur._arraysize = 1
    cur._row_factory = None
    cur.messages = []
    cur._connection = MagicMock()
    return cur


def test_sync_cursor_row_factory_default_is_none() -> None:
    cur = _make_sync_cursor()
    assert cur.row_factory is None


def test_async_cursor_row_factory_default_is_none() -> None:
    cur = _make_async_cursor()
    assert cur.row_factory is None


def test_sync_cursor_row_factory_accepts_callable() -> None:
    """stdlib idiom: ``cur.row_factory = sqlite3.Row`` works."""
    import sqlite3

    cur = _make_sync_cursor()
    cur.row_factory = sqlite3.Row
    assert cur.row_factory is sqlite3.Row


def test_async_cursor_row_factory_accepts_callable() -> None:
    import sqlite3

    cur = _make_async_cursor()
    cur.row_factory = sqlite3.Row
    assert cur.row_factory is sqlite3.Row


def test_sync_cursor_row_factory_accepts_lambda() -> None:
    cur = _make_sync_cursor()
    factory = lambda c, r: dict(zip(["a", "b"], r, strict=True))  # noqa: E731
    cur.row_factory = factory
    assert cur.row_factory is factory


def test_sync_cursor_row_factory_rejects_non_callable() -> None:
    cur = _make_sync_cursor()
    with pytest.raises(dqlitedbapi.ProgrammingError, match="row_factory"):
        cur.row_factory = 42


def test_async_cursor_row_factory_rejects_non_callable() -> None:
    cur = _make_async_cursor()
    with pytest.raises(dqlitedbapi.ProgrammingError, match="row_factory"):
        cur.row_factory = "not callable"


def test_sync_cursor_row_factory_accepts_none_to_clear() -> None:
    cur = _make_sync_cursor()
    cur.row_factory = lambda c, r: list(r)  # set
    cur.row_factory = None  # clear
    assert cur.row_factory is None


def test_programming_error_is_dbapi_error_subclass() -> None:
    """Defence pin: ProgrammingError remains inside PEP 249 §7."""
    assert issubclass(dqlitedbapi.ProgrammingError, dqlitedbapi.Error)
