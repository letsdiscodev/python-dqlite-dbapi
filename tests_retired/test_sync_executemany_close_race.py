"""Sync executemany aborts (raises InterfaceError) and stops issuing wire writes when a
foreign thread closes the cursor mid-batch (check_same_thread=False), mirroring async's
per-iteration close guard — rather than silently running every remaining autocommitting INSERT.
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
    conn = MagicMock()
    conn._max_total_rows = None
    conn._busy_timeout = 5.0
    cur._connection = conn
    return cur


@pytest.mark.asyncio
async def test_executemany_stops_on_mid_batch_close(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _make_sync_cursor()
    calls = {"n": 0}

    async def fake_execute_async(self_inner: Cursor, _op: str, _params: Any) -> None:
        # Each "iteration" is a wire write that autocommits. A foreign thread closes the
        # cursor after the first one lands.
        calls["n"] += 1
        if calls["n"] >= 1:
            self_inner._closed = True

    monkeypatch.setattr(Cursor, "_execute_async", fake_execute_async)

    with pytest.raises(InterfaceError):
        await cur._executemany_async("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])

    # Only the first iteration's write was issued; the close aborts the batch before the rest.
    assert calls["n"] == 1, (
        f"executemany must stop issuing writes after a mid-batch close; issued {calls['n']}"
    )
