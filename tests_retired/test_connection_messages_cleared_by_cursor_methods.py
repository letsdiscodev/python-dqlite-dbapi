"""Cursor methods leave Connection.messages untouched: PEP 249 §6.1.1/§6.1.2 make the
connection and cursor message surfaces independent."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi import NotSupportedError
from dqlitedbapi.cursor import Cursor


def _make_cursor() -> Cursor:
    conn = MagicMock()
    conn.messages = []
    conn._check_thread = MagicMock()
    cur = Cursor(conn)
    cur._rows = [("a",), ("b",)]
    cur._description = [("c", 3, None, None, None, None, None)]  # type: ignore[assignment]
    cur._arraysize = 1
    return cur


class TestConnectionMessagesNotClearedByCursorMethods:
    def test_fetchone_does_not_clear_connection_messages(self) -> None:
        cur = _make_cursor()
        seed = (RuntimeError, RuntimeError("session-level diagnostic"))
        cur._connection.messages.append(seed)
        cur.fetchone()
        assert cur._connection.messages == [seed]

    def test_fetchmany_does_not_clear_connection_messages(self) -> None:
        cur = _make_cursor()
        seed = (RuntimeError, RuntimeError("session-level diagnostic"))
        cur._connection.messages.append(seed)
        cur.fetchmany(1)
        assert cur._connection.messages == [seed]

    def test_fetchall_does_not_clear_connection_messages(self) -> None:
        cur = _make_cursor()
        seed = (RuntimeError, RuntimeError("session-level diagnostic"))
        cur._connection.messages.append(seed)
        cur.fetchall()
        assert cur._connection.messages == [seed]

    def test_nextset_does_not_clear_connection_messages_before_raising(self) -> None:
        cur = _make_cursor()
        seed = (RuntimeError, RuntimeError("session-level diagnostic"))
        cur._connection.messages.append(seed)
        with pytest.raises(NotSupportedError):
            cur.nextset()
        assert cur._connection.messages == [seed]
