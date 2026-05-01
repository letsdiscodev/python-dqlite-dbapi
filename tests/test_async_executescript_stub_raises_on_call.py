"""Pin: ``AsyncConnection.executescript`` and ``AsyncCursor.executescript``
are plain ``def`` (not ``async def``), so the unconditional
``NotSupportedError`` fires on the call line — matching the sync
sibling's diagnostic-leak prevention.

If the stubs were ``async def`` (the original shape), a caller who
forgot the ``await`` would observe a silent no-op with only a
GC-time ``RuntimeWarning("coroutine was never awaited")`` — defeating
the diagnostic-leak prevention this stub family was added for. Pin
the call-line raise so a future refactor that re-applies ``async
def`` to the body fails this test.
"""

from __future__ import annotations

import inspect

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import NotSupportedError


def test_async_connection_executescript_is_plain_def_not_coroutine() -> None:
    """The stub is plain ``def`` so the call line raises (not the
    deferred ``await`` line)."""
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
    """An unawaited call must raise NotSupportedError immediately,
    not return a coroutine that emits a warning at GC."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn.messages = []
    with pytest.raises(NotSupportedError, match="executescript"):
        aconn.executescript("SELECT 1; SELECT 2;")


def test_async_cursor_executescript_call_raises_immediately() -> None:
    """The cursor stub also raises on the call line."""
    from unittest.mock import MagicMock

    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur.messages = []
    cur._connection = MagicMock()
    cur._connection.messages = []
    cur._connection._check_loop_binding = MagicMock()
    with pytest.raises(NotSupportedError, match="executescript"):
        cur.executescript("SELECT 1;")
