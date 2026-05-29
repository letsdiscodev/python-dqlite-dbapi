"""__aexit__ clean-exit commit emits a DEBUG breadcrumb when interrupted by cancel/KI/SystemExit
— the only forensic trail for a dangling-server-side-transaction-after-cancel symptom."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import OperationalError


def _connection_with_commit(commit_side_effect: BaseException) -> AsyncConnection:
    import os

    conn = AsyncConnection.__new__(AsyncConnection)
    conn._address = "localhost:19001"
    conn._database = "default"
    conn._timeout = 1.0
    conn._max_total_rows = None
    conn._max_continuation_frames = None
    conn._trust_server_heartbeat = False
    conn._async_conn = MagicMock()  # truthy so __aexit__ does not early-return
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
    conn.commit = AsyncMock(side_effect=commit_side_effect)
    conn.rollback = AsyncMock()
    conn.close = AsyncMock()
    return conn


@pytest.mark.parametrize(
    "exc_cls",
    [asyncio.CancelledError, KeyboardInterrupt, SystemExit],
)
def test_aexit_clean_commit_cancel_logs_breadcrumb_and_re_raises(
    exc_cls: type[BaseException], caplog: pytest.LogCaptureFixture
) -> None:
    conn = _connection_with_commit(exc_cls("simulated"))

    async def run() -> None:
        with (
            caplog.at_level(logging.DEBUG, logger="dqlitedbapi.aio.connection"),
            pytest.raises(exc_cls),
        ):
            await conn.__aexit__(None, None, None)

    asyncio.run(run())

    breadcrumbs = [
        r
        for r in caplog.records
        if r.levelname == "DEBUG" and "commit interrupted by cancel/signal" in r.getMessage()
    ]
    assert breadcrumbs, (
        f"expected DEBUG breadcrumb on clean-exit commit interrupt for "
        f"{exc_cls.__name__}; got: "
        f"{[r.getMessage() for r in caplog.records if r.levelname == 'DEBUG']}"
    )
    assert breadcrumbs[0].exc_info is not None
    assert isinstance(breadcrumbs[0].exc_info[1], exc_cls)


def test_aexit_clean_commit_normal_exception_does_not_log_breadcrumb(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An ordinary exception during commit must NOT trigger the cancel/signal breadcrumb."""
    conn = _connection_with_commit(OperationalError("server-side"))

    async def run() -> None:
        with (
            caplog.at_level(logging.DEBUG, logger="dqlitedbapi.aio.connection"),
            pytest.raises(OperationalError),
        ):
            await conn.__aexit__(None, None, None)

    asyncio.run(run())

    breadcrumbs = [
        r
        for r in caplog.records
        if r.levelname == "DEBUG" and "commit interrupted by cancel/signal" in r.getMessage()
    ]
    assert not breadcrumbs, (
        f"non-cancel/signal exception must not trigger the breadcrumb; "
        f"got: {[r.getMessage() for r in breadcrumbs]}"
    )
