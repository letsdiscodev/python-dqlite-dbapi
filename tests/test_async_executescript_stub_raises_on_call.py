"""The executescript stubs are plain ``def`` (not ``async def``) so
``NotSupportedError`` fires on the call line.

If they were ``async def``, a forgotten ``await`` would be a silent no-op with
only a GC-time RuntimeWarning, defeating the diagnostic-leak prevention.
"""

from __future__ import annotations

import inspect

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import NotSupportedError


def test_async_connection_executescript_is_plain_def_not_coroutine() -> None:
    assert not inspect.iscoroutinefunction(AsyncConnection.executescript), (
        "AsyncConnection.executescript must be `def`, not `async def`, so the "
        "NotSupportedError fires on the call line — not deferred to await"
    )


def test_async_cursor_executescript_is_plain_def_not_coroutine() -> None:
    assert not inspect.iscoroutinefunction(AsyncCursor.executescript), (
        "AsyncCursor.executescript must be `def`, not `async def`, so the "
        "NotSupportedError fires on the call line — not deferred to await"
    )


def test_async_connection_executescript_call_raises_immediately() -> None:
    """An unawaited call must raise immediately, not return a warning-at-GC coroutine."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn.messages = []
    with pytest.raises(NotSupportedError, match="executescript"):
        aconn.executescript("SELECT 1; SELECT 2;")


def test_async_cursor_executescript_call_raises_immediately() -> None:
    from unittest.mock import MagicMock

    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur.messages = []
    cur._connection = MagicMock()
    cur._connection.messages = []
    cur._connection._check_loop_binding = MagicMock()
    with pytest.raises(NotSupportedError, match="executescript"):
        cur.executescript("SELECT 1;")
