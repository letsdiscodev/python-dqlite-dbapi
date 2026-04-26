"""Pins for small PEP 249 corners that lacked dedicated tests.

Each pin documents a contract that a future refactor could silently
break:

* ``_datetime_from_iso8601("")`` returns None — the docstring claims
  this and the ``RowsResponse`` decode path relies on it for legacy
  servers that emit empty text for NULL datetime cells. A change to
  raise on empty input would route a legitimate NULL through the
  DataError path.
* ``iter(closed_cursor)`` returns the cursor (PEP 234 protocol) but
  ``next()`` raises InterfaceError. The protocol defers the closed
  check to ``__next__`` so a stale ``for row in cursor:`` loop
  surfaces a clear PEP 249 error on the first read instead of
  silently iterating an empty buffer.
* ``Cursor.messages`` is cleared by every ``execute*`` /
  ``fetch*`` call per PEP 249 §6.1. The driver doesn't currently
  populate ``messages``, but the clear contract still has to hold —
  consumers (tracing middleware, structured-error tooling) populate
  it themselves between calls and rely on the clear to scope each
  observation to its own statement.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.exceptions import InterfaceError
from dqlitedbapi.types import _datetime_from_iso8601


def test_datetime_from_iso8601_empty_string_returns_none() -> None:
    assert _datetime_from_iso8601("") is None


def test_datetime_from_iso8601_normal_string_returns_datetime() -> None:
    """Negative pin: the empty-string short-circuit must not mask
    valid input."""
    out = _datetime_from_iso8601("2025-01-01T00:00:00")
    assert out is not None


def _make_closed_cursor() -> Any:
    """Build a Cursor that is detached from any real connection but in
    the ``_closed`` state — minimum surface to exercise the iteration
    protocol without spinning up a loop thread."""
    from dqlitedbapi.cursor import Cursor

    cur = Cursor.__new__(Cursor)
    cur._closed = True
    cur._description = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._rows = []
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    # ``fetchone`` calls ``_connection._check_thread`` BEFORE the
    # closed check; provide a no-op stub so the closed branch is the
    # one that fires.
    fake_conn = MagicMock()
    fake_conn._check_thread = MagicMock()
    cur._connection = fake_conn
    return cur


def test_iter_on_closed_cursor_returns_cursor_no_immediate_raise() -> None:
    """PEP 234: ``iter(obj)`` must return an iterator. Returning the
    cursor and deferring the closed check to ``__next__`` matches the
    PEP 234 contract; raising in ``__iter__`` would diverge."""
    cur = _make_closed_cursor()
    assert iter(cur) is cur


def test_next_on_closed_cursor_raises_interface_error() -> None:
    """The closed check fires from ``fetchone`` (called by
    ``__next__``). Pin the PEP 249 ``InterfaceError`` rather than a
    bare ``StopIteration`` — silent termination would let a ``for row
    in cursor:`` loop after ``cursor.close()`` quietly do nothing."""
    cur = _make_closed_cursor()
    with pytest.raises(InterfaceError):
        next(cur)


def test_for_loop_on_closed_cursor_raises_interface_error() -> None:
    """End-to-end: ``for row in closed_cursor`` must raise on the
    first yield attempt, not silently terminate."""
    cur = _make_closed_cursor()
    with pytest.raises(InterfaceError):
        for _ in cur:  # noqa: PIE810 - deliberate full iteration probe
            pass


def test_cursor_messages_cleared_on_execute_check() -> None:
    """PEP 249 §6.1: ``cursor.messages`` is cleared by any subsequent
    method call on the cursor. The driver never populates the list
    itself, but consumers (telemetry middleware) may; the clear must
    fire so each statement's observations are scoped correctly."""
    conn = Connection("localhost:9001")
    try:
        cur = conn.cursor()
        # Pre-seed messages — simulate a consumer between statements.
        seeded = (UserWarning, UserWarning("from previous call"))
        cur.messages.append(seeded)
        assert cur.messages == [seeded]

        # ``executemany`` with an empty seq is the cheapest call that
        # exercises the clear (no actual SQL roundtrip; the empty-batch
        # short-circuit lives BEFORE any wire I/O so the test does not
        # require a live cluster).
        cur.executemany("INSERT INTO t VALUES (?)", [])
        assert cur.messages == []
    finally:
        conn.close()
