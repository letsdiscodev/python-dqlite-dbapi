"""__aexit__'s rollback-cancel arm: the cancel signal supplants the body exception class but the
body exception stays on __context__. SA's is_disconnect walks __cause__ only, so misses it."""

from __future__ import annotations

import asyncio
import logging
import os
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


def _connection_with_failing_rollback(failure: BaseException) -> AsyncConnection:
    """Bare AsyncConnection whose ``rollback()`` raises ``failure``."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._creator_pid = os.getpid()
    conn._loop_ref = None
    conn._address = "127.0.0.1:9999"
    conn._async_conn = MagicMock()
    conn.messages = []
    conn.rollback = AsyncMock(side_effect=failure)
    return conn


def test_aexit_rollback_cancel_after_body_class_supplants() -> None:
    conn = _connection_with_failing_rollback(asyncio.CancelledError())
    body_exc = RuntimeError("body-raised-this")

    async def run() -> BaseException:
        try:
            await conn.__aexit__(type(body_exc), body_exc, None)
        except asyncio.CancelledError as e:
            return e
        raise AssertionError("did not raise CancelledError")

    cancel = asyncio.run(run())
    assert isinstance(cancel, asyncio.CancelledError)
    # Cancel wins; body class is unreachable via __cause__ (SA is_disconnect walks __cause__ only).
    assert cancel.__cause__ is None


def test_aexit_rollback_cancel_breadcrumb_names_policy_choice(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The DEBUG breadcrumb must name the structured-concurrency policy / __context__."""
    caplog.set_level(logging.DEBUG, logger="dqlitedbapi.aio.connection")
    conn = _connection_with_failing_rollback(asyncio.CancelledError())

    async def run() -> None:
        with pytest.raises(asyncio.CancelledError):
            await conn.__aexit__(RuntimeError, RuntimeError("body-raised"), None)

    asyncio.run(run())
    matching = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG and "rollback interrupted by cancel/signal" in r.getMessage()
    ]
    assert matching, f"expected DEBUG breadcrumb; got {caplog.records!r}"
    msg = matching[0].getMessage()
    assert "structured-concurrency" in msg or "__context__" in msg
