"""Pins for small PEP 249 corners that lacked dedicated tests."""

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
    """The empty-string short-circuit must not mask valid input."""
    out = _datetime_from_iso8601("2025-01-01T00:00:00")
    assert out is not None


def _make_closed_cursor() -> Any:
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
    # fetchone calls _check_thread BEFORE the closed check; stub it so closed fires.
    fake_conn = MagicMock()
    fake_conn._check_thread = MagicMock()
    cur._connection = fake_conn
    return cur


def test_iter_on_closed_cursor_returns_cursor_no_immediate_raise() -> None:
    """PEP 234: iter() returns the cursor; the closed check is deferred to __next__."""
    cur = _make_closed_cursor()
    assert iter(cur) is cur


def test_next_on_closed_cursor_raises_interface_error() -> None:
    """Closed cursor raises InterfaceError, not bare StopIteration (no silent no-op)."""
    cur = _make_closed_cursor()
    with pytest.raises(InterfaceError):
        next(cur)


def test_for_loop_on_closed_cursor_raises_interface_error() -> None:
    """``for row in closed_cursor`` raises on the first yield, not silently terminate."""
    cur = _make_closed_cursor()
    with pytest.raises(InterfaceError):
        for _ in cur:  # noqa: PIE810 - deliberate full iteration probe
            pass


def test_cursor_messages_cleared_on_execute_check() -> None:
    """PEP 249 §6.1: any cursor method call clears cursor.messages."""
    conn = Connection("localhost:9001")
    try:
        cur = conn.cursor()
        seeded = (UserWarning, UserWarning("from previous call"))
        cur.messages.append(seeded)
        assert cur.messages == [seeded]

        # Empty-seq executemany short-circuits before wire I/O, so no live cluster needed.
        cur.executemany("INSERT INTO t VALUES (?)", [])
        assert cur.messages == []
    finally:
        conn.close()


def test_connection_cursor_returns_distinct_instance_each_call() -> None:
    """PEP 249 §6.1.1: Connection.cursor() returns a new Cursor per call."""
    conn = Connection("localhost:9001")
    try:
        c1 = conn.cursor()
        c2 = conn.cursor()
        assert c1 is not c2
        c1._rowcount = 99
        assert c2._rowcount == -1
    finally:
        conn.close()


def test_commit_clears_messages_before_closed_raise() -> None:
    """commit() on a closed connection clears messages before raising InterfaceError."""
    conn = Connection("localhost:9001")
    conn.close()
    seeded = (UserWarning, UserWarning("stale"))
    conn.messages.append(seeded)
    with pytest.raises(InterfaceError, match="closed"):
        conn.commit()
    assert conn.messages == []


def test_rollback_clears_messages_before_closed_raise() -> None:
    """rollback() clears messages before raising on a closed connection."""
    conn = Connection("localhost:9001")
    conn.close()
    seeded = (UserWarning, UserWarning("stale"))
    conn.messages.append(seeded)
    with pytest.raises(InterfaceError, match="closed"):
        conn.rollback()
    assert conn.messages == []


def test_execute_returns_cursor_for_chaining() -> None:
    """execute() returns the cursor for chaining (cur.execute(sql).fetchall())."""
    conn = Connection("localhost:9001")
    try:
        cur = conn.cursor()
        result = cur.executemany("INSERT INTO t VALUES (?)", [])
        assert result is cur
    finally:
        conn.close()
