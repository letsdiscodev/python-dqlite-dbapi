"""Pin that setinputsizes / setoutputsize do NOT clear ``self.messages``.

PEP 249 §6.1.2 enumerates the cursor methods that clear the
messages list as the fetch + nextset surface. setinputsizes /
setoutputsize are no-ops in dqlite; a well-meaning "consistency"
patch that added ``del self.messages[:]`` to them would silently
wipe a warning list the caller pushed between execute() and
setinputsizes().
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dqlitedbapi.cursor import Cursor


def _make_cursor() -> Cursor:
    conn = MagicMock()
    conn.messages = []
    conn._check_thread = MagicMock()
    return Cursor(conn)


def test_setinputsizes_does_not_clear_cursor_messages() -> None:
    cur = _make_cursor()
    cur.messages.append((RuntimeError, "stale"))
    cur.setinputsizes([None])
    assert cur.messages == [(RuntimeError, "stale")]


def test_setoutputsize_does_not_clear_cursor_messages() -> None:
    cur = _make_cursor()
    cur.messages.append((RuntimeError, "stale"))
    cur.setoutputsize(4096)
    assert cur.messages == [(RuntimeError, "stale")]


def test_setinputsizes_does_not_clear_connection_messages() -> None:
    cur = _make_cursor()
    cur._connection.messages.append((RuntimeError, "stale"))
    cur.setinputsizes([None])
    assert cur._connection.messages == [(RuntimeError, "stale")]


def test_setoutputsize_does_not_clear_connection_messages() -> None:
    cur = _make_cursor()
    cur._connection.messages.append((RuntimeError, "stale"))
    cur.setoutputsize(4096)
    assert cur._connection.messages == [(RuntimeError, "stale")]
