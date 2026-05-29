"""``Connection.rollback()`` (sync + async) retries a transient SQLITE_BUSY on
ROLLBACK rather than surfacing it, matching ``commit()``."""

from __future__ import annotations

import asyncio
import os
import threading
from unittest.mock import AsyncMock, MagicMock, patch

from dqlitedbapi import Connection
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import OperationalError
from dqlitewire import SQLITE_BUSY


def test_sync_rollback_retries_on_transient_busy() -> None:
    """A ROLLBACK returning SQLITE_BUSY once then succeeding must be retried."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn.messages = []
    conn._busy_timeout = 5.0
    inner = MagicMock()
    inner._protocol = object()
    inner.in_transaction = True  # so rollback proceeds to the ROLLBACK
    conn._async_conn = inner
    conn._run_sync = lambda coro: asyncio.run(coro)

    calls = 0

    async def fake_rollback_async() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OperationalError("database is locked", code=SQLITE_BUSY)

    conn._rollback_async = fake_rollback_async

    with patch("dqlitedbapi._busy_retry.time.sleep"):
        conn.rollback()

    assert calls == 2, f"rollback() should retry the BUSY once then succeed; got {calls} attempt(s)"


async def test_async_rollback_retries_on_transient_busy() -> None:
    """Async sibling: retried inside the op_lock / timeout scope."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._transaction_owner = None
    conn._timeout = 5.0
    conn._busy_timeout = 5.0
    conn.messages = []
    inner = MagicMock()
    inner._protocol = object()
    inner.in_transaction = True

    calls = 0

    def execute_side_effect(_sql: str) -> object:
        async def _co() -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OperationalError("database is locked", code=SQLITE_BUSY)

        return _co()

    inner.execute = MagicMock(side_effect=execute_side_effect)
    conn._async_conn = inner

    op_lock = AsyncMock()
    op_lock.__aenter__ = AsyncMock(return_value=op_lock)
    op_lock.__aexit__ = AsyncMock(return_value=False)
    with patch.object(conn, "_ensure_locks", return_value=(None, op_lock)):
        await conn.rollback()

    assert calls == 2, (
        f"async rollback() should retry the BUSY once then succeed; got {calls} attempt(s)"
    )
