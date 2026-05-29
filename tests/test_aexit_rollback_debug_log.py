"""__aexit__ logs a DEBUG breadcrumb on rollback failure: the body exception still wins (PEP 249),
but the swallowed rollback error needs a trace to diagnose dangling server-side transactions."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import OperationalError


def _connection_with_failing_rollback(rollback_exc: BaseException) -> AsyncConnection:
    import os

    conn = AsyncConnection.__new__(AsyncConnection)
    conn._address = "localhost:19001"
    conn._database = "default"
    conn._timeout = 1.0
    conn._max_total_rows = None
    conn._max_continuation_frames = None
    conn._trust_server_heartbeat = False
    conn._async_conn = MagicMock()  # truthy so we take the try/except branch
    conn._closed = False
    conn._closed_flag = [False]
    conn._connect_lock = None
    conn._op_lock = None
    conn._loop_ref = None
    conn._cursors = MagicMock()
    conn._cursors.__iter__ = lambda self: iter([])
    conn._cursors.clear = MagicMock()
    conn._finalizer = None
    conn._creator_pid = os.getpid()
    conn.messages = []
    conn.commit = AsyncMock()
    conn.rollback = AsyncMock(side_effect=rollback_exc)
    conn.close = AsyncMock()
    return conn


def test_aexit_logs_rollback_failure(caplog: pytest.LogCaptureFixture) -> None:
    conn = _connection_with_failing_rollback(OperationalError("server gone"))

    async def run() -> None:
        with caplog.at_level(logging.DEBUG, logger="dqlitedbapi.aio.connection"):
            await conn.__aexit__(RuntimeError, RuntimeError("body-raised"), None)

    asyncio.run(run())

    matching = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG and "rollback failed" in r.getMessage()
    ]
    assert matching, f"expected DEBUG 'rollback failed' record; got {caplog.records!r}"
    assert matching[0].exc_info is not None
    assert isinstance(matching[0].exc_info[1], OperationalError)


def test_aexit_rollback_cancelled_error_propagates() -> None:
    """CancelledError is not Exception — it must NOT be swallowed."""
    conn = _connection_with_failing_rollback(asyncio.CancelledError())

    async def run() -> None:
        with pytest.raises(asyncio.CancelledError):
            await conn.__aexit__(RuntimeError, RuntimeError("body-raised"), None)

    asyncio.run(run())
