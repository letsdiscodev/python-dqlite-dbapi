"""PEP 249 §6.1.1 — Connection.messages is cleared by the cursor
fetch methods (fetchone, fetchmany, fetchall) and by nextset.
"""

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


class TestConnectionMessagesCleared:
    def test_fetchone_clears_connection_messages(self) -> None:
        cur = _make_cursor()
        cur._connection.messages.append((RuntimeError, "stale"))
        cur.fetchone()
        assert cur._connection.messages == []

    def test_fetchmany_clears_connection_messages(self) -> None:
        cur = _make_cursor()
        cur._connection.messages.append((RuntimeError, "stale"))
        cur.fetchmany(1)
        assert cur._connection.messages == []

    def test_fetchall_clears_connection_messages(self) -> None:
        cur = _make_cursor()
        cur._connection.messages.append((RuntimeError, "stale"))
        cur.fetchall()
        assert cur._connection.messages == []

    def test_nextset_clears_connection_messages_before_raising(self) -> None:
        cur = _make_cursor()
        cur._connection.messages.append((RuntimeError, "stale"))
        with pytest.raises(NotSupportedError):
            cur.nextset()
        assert cur._connection.messages == []
