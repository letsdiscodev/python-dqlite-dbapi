"""Pin that ``setinputsizes`` / ``setoutputsize`` clear ``self.messages``
and ``self._connection.messages`` per PEP 249 §6.1.1.

PEP 249 §6.1.1 enumerates the cursor methods that clear the
messages list and explicitly names ``setinputsizes`` /
``setoutputsize``. Every other method in the list already clears;
these two were the outliers until ISSUE-566 brought them in line.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dqlitedbapi.cursor import Cursor


def _make_cursor() -> Cursor:
    conn = MagicMock()
    conn.messages = []
    conn._check_thread = MagicMock()
    return Cursor(conn)


def test_setinputsizes_clears_cursor_messages() -> None:
    cur = _make_cursor()
    cur.messages.append((RuntimeError, "stale"))
    cur.setinputsizes([None])
    assert cur.messages == []


def test_setoutputsize_clears_cursor_messages() -> None:
    cur = _make_cursor()
    cur.messages.append((RuntimeError, "stale"))
    cur.setoutputsize(4096)
    assert cur.messages == []


def test_setinputsizes_does_not_clear_connection_messages() -> None:
    """PEP 249 §6.1.1 / §6.1.2 — Connection.messages and Cursor.messages
    are independent surfaces. Cursor methods must NOT clear the
    connection's list."""
    cur = _make_cursor()
    seed = (RuntimeError, "session-level diagnostic")
    cur._connection.messages.append(seed)
    cur.setinputsizes([None])
    assert cur._connection.messages == [seed]


def test_setoutputsize_does_not_clear_connection_messages() -> None:
    cur = _make_cursor()
    seed = (RuntimeError, "session-level diagnostic")
    cur._connection.messages.append(seed)
    cur.setoutputsize(4096)
    assert cur._connection.messages == [seed]
