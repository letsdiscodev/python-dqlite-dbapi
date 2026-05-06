"""Pin: ``AsyncConnection.commit()`` and ``rollback()`` bound their
op_lock acquire by ``self._timeout`` and remap a ``TimeoutError`` to
``OperationalError``.

Without the bound, a sibling task parked on a slow ``reader.read()``
(especially under ``trust_server_heartbeat=True`` widening the
per-read deadline up to 300 s) blocks ``commit()`` for the full
per-read deadline. Under shutdown — SA ``engine.dispose()`` or app
SIGTERM with a budget — the call hangs.

Mirror the ``close()`` discipline at ``aio/connection.py:569-570``
which already wraps the op_lock acquire in
``asyncio.timeout(self._timeout)``. The sync sibling
``Connection.commit/rollback`` at ``connection.py:947`` already
acquires with ``timeout=self._timeout`` for the same reason.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import OperationalError


def _prime_alive_inner() -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._transaction_owner = None
    conn._timeout = 0.05  # tight so the test finishes fast
    conn.messages = []
    inner = MagicMock()
    inner._protocol = object()
    inner.in_transaction = True  # would route into the COMMIT path
    conn._async_conn = inner
    return conn


@pytest.mark.asyncio
async def test_commit_op_lock_acquire_bounded_by_timeout() -> None:
    conn = _prime_alive_inner()
    held_lock = asyncio.Lock()
    await held_lock.acquire()  # held by us; commit() must time out

    with (
        patch.object(conn, "_ensure_locks", return_value=(None, held_lock)),
        pytest.raises(OperationalError, match="commit op_lock"),
    ):
        await conn.commit()

    held_lock.release()


@pytest.mark.asyncio
async def test_rollback_op_lock_acquire_bounded_by_timeout() -> None:
    conn = _prime_alive_inner()
    held_lock = asyncio.Lock()
    await held_lock.acquire()

    with (
        patch.object(conn, "_ensure_locks", return_value=(None, held_lock)),
        pytest.raises(OperationalError, match="rollback op_lock"),
    ):
        await conn.rollback()

    held_lock.release()
