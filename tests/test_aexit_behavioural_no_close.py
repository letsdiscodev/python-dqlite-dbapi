"""Behavioural pin: __aexit__ does not close (diverges from aiosqlite / psycopg) and leaves
_async_conn bound for reuse, regardless of syntactic surface."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


def _seed_async_conn_for_aexit() -> AsyncConnection:
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._creator_pid = 0  # bypass fork-check via test-only seeding
    aconn._loop_ref = None
    aconn._connect_lock = None
    aconn._op_lock = None
    aconn._async_conn = MagicMock()
    aconn._async_conn.close = AsyncMock()
    aconn._async_conn.in_transaction = False
    aconn._transaction_owner = None
    aconn._address = "stub:0"
    aconn._timeout = 1.0
    aconn._close_timeout = 0.5
    aconn._closed_flag = [False]
    aconn._connected_flag = [True]
    aconn.messages = []
    return aconn


@pytest.mark.asyncio
async def test_aexit_does_not_call_self_close_behavioural() -> None:
    aconn = _seed_async_conn_for_aexit()
    close_mock = AsyncMock()
    commit_mock = AsyncMock()
    aconn.close = close_mock
    aconn.commit = commit_mock

    await aconn.__aexit__(None, None, None)

    close_mock.assert_not_called()
    inner = aconn._async_conn
    assert inner is not None
    inner.close.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_aexit_leaves_async_conn_bound_for_reusability() -> None:
    """Connection stays reusable after async-with exits: _async_conn must remain bound."""
    aconn = _seed_async_conn_for_aexit()
    commit_mock = AsyncMock()
    aconn.commit = commit_mock
    inner = aconn._async_conn

    await aconn.__aexit__(None, None, None)

    assert aconn._async_conn is inner, (
        f"__aexit__ should leave _async_conn bound for reusability; got {aconn._async_conn!r}"
    )
    assert not aconn._closed, (
        f"__aexit__ should leave _closed=False for reusability; got _closed={aconn._closed!r}"
    )
