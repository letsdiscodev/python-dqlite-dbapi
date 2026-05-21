"""Behavioural pin for ``AsyncConnection.__aexit__`` no-close
divergence from aiosqlite / psycopg.

The existing ``test_async_with_exit_does_not_close.py`` is source-
substring-only (matches on ``"await self.close()"`` /
``"self._async_conn.close()"`` literals). A regression that closes
via any other syntactic surface — helper invocation, attribute
drop, GC-triggered teardown via ``_async_conn = None`` — passes the
substring assertion while semantically closing.

This file drives the actual ``__aexit__`` body against a hand-seeded
``AsyncConnection`` with ``AsyncMock``-wrapped close paths and
asserts close was NOT invoked regardless of syntactic surface, and
that ``_async_conn`` remains bound for reusability per the
documented contract.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


def _seed_async_conn_for_aexit() -> AsyncConnection:
    """Hand-build an ``AsyncConnection`` for direct ``__aexit__``
    invocation. The body walks ``commit()`` / ``rollback()`` which
    we patch out to AsyncMock no-ops so the test is wire-free."""
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
    """Drive ``__aexit__`` on a real AsyncConnection with ``close``
    wrapped in AsyncMock. Assert close was NOT invoked regardless of
    the syntactic surface a future regression might use."""
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
    """The documented contract: the connection remains REUSABLE after
    ``async with`` exits — ``_async_conn`` must still be bound to
    the inner conn so a subsequent ``aconn.cursor()`` does not raise
    ``InterfaceError("Connection is closed")``. A regression that
    nulls ``_async_conn`` as a soft-close trick would fail this pin
    even though it'd pass the source-substring assertions in the
    sibling test file."""
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
