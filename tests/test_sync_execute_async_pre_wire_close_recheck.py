"""Sync _execute_async re-checks closed AFTER acquiring the connection, before the wire
(mirrors AsyncCursor._execute_unlocked): a close racing the connection-acquisition await
raises InterfaceError and issues no wire call, rather than a wasted round-trip + silent scrub.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import InterfaceError


def _make_sync_cursor() -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._rows = []
    cur._description = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_index = 0
    cur._arraysize = 1
    cur._row_factory = None
    cur.messages = []
    cur._completed_iterations = 0
    return cur


@pytest.mark.asyncio
async def test_close_during_connection_acquire_raises_before_wire(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cur = _make_sync_cursor()

    inner = MagicMock()
    wire_calls = {"n": 0}

    def _query_raw_typed(*_a: Any, **_k: Any) -> Any:
        wire_calls["n"] += 1
        return "fake_coro"

    inner.query_raw_typed = _query_raw_typed

    async def _get_async_connection() -> Any:
        cur._closed = True  # foreign-thread close lands during the connection-acquire await
        return inner

    parent_conn = MagicMock()
    parent_conn._get_async_connection = _get_async_connection
    cur._connection = parent_conn

    async def fake_call_client(_coro: Any) -> Any:  # pragma: no cover - must not be reached
        wire_calls["n"] += 1
        return (["c"], [1], [[1]], [[1]])

    from dqlitedbapi import cursor as cursor_mod

    monkeypatch.setattr(cursor_mod, "_call_client", fake_call_client)

    with pytest.raises(InterfaceError):
        await cur._execute_async("SELECT 1", None)
    assert wire_calls["n"] == 0, "no wire call may be issued once the cursor is closed pre-wire"
