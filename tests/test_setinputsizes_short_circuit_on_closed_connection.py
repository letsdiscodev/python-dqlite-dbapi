"""Pin: sync ``Cursor.setinputsizes`` and ``Cursor.setoutputsize``
short-circuit when EITHER cursor or parent connection is closed —
symmetric with the async sibling at ``aio/cursor.py:808-817`` and
``:830-832``.

Pre-fix, sync only short-circuited on closed cursor; if the cursor
escaped the cascade-close (a TOCTOU window the async cursor's
``cursor()`` factory explicitly guards) and the parent connection
was closed, sync called ``self._connection._check_thread()``
against a connection mid-tear-down — divergent from the async
sibling's "free to do nothing on closed" interpretation of PEP 249
§6.2.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dqlitedbapi.cursor import Cursor


def _build_cursor_with_closed_connection() -> Cursor:
    """Construct a Cursor whose own ``_closed`` is False but whose
    parent connection's ``_closed`` is True (the TOCTOU window).
    """
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
    # If _check_thread is called, it would not raise on a MagicMock,
    # but the test asserts it is NOT called.
    conn._check_thread = MagicMock()
    cur._connection = conn
    return cur


def test_setinputsizes_short_circuits_on_closed_connection() -> None:
    """When the parent connection is closed (cursor not yet cascade-
    closed), ``setinputsizes`` must no-op — NOT call
    ``_check_thread``.
    """
    cur = _build_cursor_with_closed_connection()
    check_thread_mock = cur._connection._check_thread
    assert isinstance(check_thread_mock, MagicMock)
    # Should be a silent no-op.
    cur.setinputsizes([1, 2, 3])
    # _check_thread must NOT have been called against the closed
    # connection.
    check_thread_mock.assert_not_called()


def test_setoutputsize_short_circuits_on_closed_connection() -> None:
    """Symmetric pin for ``setoutputsize``."""
    cur = _build_cursor_with_closed_connection()
    check_thread_mock = cur._connection._check_thread
    assert isinstance(check_thread_mock, MagicMock)
    cur.setoutputsize(64)
    check_thread_mock.assert_not_called()


def test_setinputsizes_does_not_short_circuit_when_both_open() -> None:
    """Negative pin: when neither cursor nor connection is closed,
    the original ``_check_thread`` call still runs (symmetric with
    pre-fix behaviour on the bound-loop happy path).
    """
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
    """Symmetric negative pin for setoutputsize."""
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
