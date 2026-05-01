"""Pin: ``Cursor.row_factory`` and ``AsyncCursor.row_factory`` exist
as property+rejecting-setter pairs (mirroring the Connection-level
discipline) so a stdlib-style write attempt surfaces inside the
``dbapi.Error`` hierarchy rather than leaking ``AttributeError``
through ``__slots__``.

stdlib ``sqlite3.Cursor`` and aiosqlite both expose ``row_factory``
as a writable property. Cross-driver code that does
``cur.row_factory = sqlite3.Row`` is a documented stdlib idiom; we
reject it (dqlitedbapi only returns plain tuples per PEP 249) but
the rejection MUST go through ``NotSupportedError``, not bare
``AttributeError`` from the ``__slots__`` mechanism.
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
    cur.messages = []
    cur._connection = MagicMock()
    return cur


def test_sync_cursor_row_factory_read_returns_none() -> None:
    cur = _make_sync_cursor()
    assert cur.row_factory is None


def test_async_cursor_row_factory_read_returns_none() -> None:
    cur = _make_async_cursor()
    assert cur.row_factory is None


def test_sync_cursor_row_factory_assign_none_no_op() -> None:
    cur = _make_sync_cursor()
    cur.row_factory = None  # idempotent — sentinel for "default"


def test_async_cursor_row_factory_assign_none_no_op() -> None:
    cur = _make_async_cursor()
    cur.row_factory = None


def test_sync_cursor_row_factory_assign_callable_raises_not_supported() -> None:
    """A user attempting the stdlib idiom must hit ``NotSupportedError``
    (subclass of ``dbapi.Error``), not bare ``AttributeError`` from
    the ``__slots__`` mechanism."""
    import sqlite3

    cur = _make_sync_cursor()
    with pytest.raises(dqlitedbapi.NotSupportedError, match="row_factory"):
        cur.row_factory = sqlite3.Row


def test_async_cursor_row_factory_assign_callable_raises_not_supported() -> None:
    import sqlite3

    cur = _make_async_cursor()
    with pytest.raises(dqlitedbapi.NotSupportedError, match="row_factory"):
        cur.row_factory = sqlite3.Row


def test_sync_cursor_row_factory_assign_lambda_raises_not_supported() -> None:
    cur = _make_sync_cursor()
    with pytest.raises(dqlitedbapi.NotSupportedError, match="row_factory"):
        cur.row_factory = lambda c, r: dict(zip(["a", "b"], r, strict=True))


def test_not_supported_error_is_dbapi_error_subclass() -> None:
    """Defence pin: NotSupportedError must remain inside the
    PEP 249 §7 hierarchy."""
    assert issubclass(dqlitedbapi.NotSupportedError, dqlitedbapi.Error)
