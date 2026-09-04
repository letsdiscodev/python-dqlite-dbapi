"""``setinputsizes``/``setoutputsize`` short-circuit when cursor OR connection is closed."""

from __future__ import annotations

from unittest.mock import MagicMock

from dqlitedbapi.cursor import Cursor


def _build_cursor_with_closed_connection() -> Cursor:
    """Cursor with own _closed False but parent connection _closed True (TOCTOU window)."""
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._description = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_factory = None
    cur._rows = []
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []

    conn = MagicMock()
    conn._closed = True
    conn._check_thread = MagicMock()
    cur._connection = conn
    return cur


def test_setinputsizes_short_circuits_on_closed_connection() -> None:
    cur = _build_cursor_with_closed_connection()
    check_thread_mock = cur._connection._check_thread
    assert isinstance(check_thread_mock, MagicMock)
    cur.setinputsizes([1, 2, 3])
    check_thread_mock.assert_not_called()


def test_setoutputsize_short_circuits_on_closed_connection() -> None:
    cur = _build_cursor_with_closed_connection()
    check_thread_mock = cur._connection._check_thread
    assert isinstance(check_thread_mock, MagicMock)
    cur.setoutputsize(64)
    check_thread_mock.assert_not_called()


def test_setinputsizes_does_not_short_circuit_when_both_open() -> None:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._description = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_factory = None
    cur._rows = []
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    conn = MagicMock()
    conn._closed = False
    conn._check_thread = MagicMock()
    cur._connection = conn

    cur.setinputsizes([1])
    conn._check_thread.assert_called_once()


def test_setoutputsize_does_not_short_circuit_when_both_open() -> None:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._description = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_factory = None
    cur._rows = []
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    conn = MagicMock()
    conn._closed = False
    conn._check_thread = MagicMock()
    cur._connection = conn

    cur.setoutputsize(128)
    conn._check_thread.assert_called_once()
