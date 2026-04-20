"""Pin the DEBUG-log breadcrumb on AsyncConnection.__aexit__ rollback failure.

PEP 249 body-wins ordering is unchanged — the body's exception still
propagates and the rollback exception is swallowed. But a silent
swallow leaves no trace of the failed rollback, which an operator
later needs to diagnose dangling server-side transactions (leader
flip mid-commit, transport timeout, etc.).
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import OperationalError


def _connection_with_failing_rollback(rollback_exc: BaseException) -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._address = "localhost:19001"
    conn._database = "default"
    conn._timeout = 1.0
    conn._max_total_rows = None
    conn._max_continuation_frames = None
    conn._trust_server_heartbeat = False
    conn._async_conn = MagicMock()  # truthy so we take the try/except branch
    conn._closed = False
    conn._connect_lock = None
    conn._op_lock = None
    conn._loop_ref = None
    conn.messages = []
    conn.commit = AsyncMock()  # type: ignore[method-assign]
    conn.rollback = AsyncMock(side_effect=rollback_exc)  # type: ignore[method-assign]
    conn.close = AsyncMock()  # type: ignore[method-assign]
    return conn


def test_aexit_logs_rollback_failure(caplog: pytest.LogCaptureFixture) -> None:
    conn = _connection_with_failing_rollback(OperationalError("server gone"))

    async def run() -> None:
        with caplog.at_level(logging.DEBUG, logger="dqlitedbapi.aio.connection"):
            # Exercise __aexit__ directly with a body-exception signature.
            await conn.__aexit__(RuntimeError, RuntimeError("body-raised"), None)

    asyncio.run(run())

    matching = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG and "rollback failed" in r.getMessage()
    ]
    assert matching, f"expected DEBUG 'rollback failed' record; got {caplog.records!r}"
    # exc_info should carry the suppressed OperationalError.
    assert matching[0].exc_info is not None
    assert isinstance(matching[0].exc_info[1], OperationalError)


def test_aexit_rollback_cancelled_error_propagates() -> None:
    """CancelledError is not Exception — it must NOT be swallowed."""
    conn = _connection_with_failing_rollback(asyncio.CancelledError())

    async def run() -> None:
        with pytest.raises(asyncio.CancelledError):
            await conn.__aexit__(RuntimeError, RuntimeError("body-raised"), None)

    asyncio.run(run())
