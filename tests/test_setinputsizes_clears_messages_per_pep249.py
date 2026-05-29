"""``setinputsizes`` / ``setoutputsize`` clear cursor messages per PEP 249 §6.1.1."""

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
    cur.messages.append((RuntimeError, RuntimeError("stale")))
    cur.setinputsizes([None])
    assert cur.messages == []


def test_setoutputsize_clears_cursor_messages() -> None:
    cur = _make_cursor()
    cur.messages.append((RuntimeError, RuntimeError("stale")))
    cur.setoutputsize(4096)
    assert cur.messages == []


def test_setinputsizes_does_not_clear_connection_messages() -> None:
    """Cursor methods must not clear the connection's independent messages list."""
    cur = _make_cursor()
    seed = (RuntimeError, RuntimeError("session-level diagnostic"))
    cur._connection.messages.append(seed)
    cur.setinputsizes([None])
    assert cur._connection.messages == [seed]


def test_setoutputsize_does_not_clear_connection_messages() -> None:
    cur = _make_cursor()
    seed = (RuntimeError, RuntimeError("session-level diagnostic"))
    cur._connection.messages.append(seed)
    cur.setoutputsize(4096)
    assert cur._connection.messages == [seed]
