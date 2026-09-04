"""Pin: PEP 249 §6.1.1 message-clearing holds on the closed-cursor and
cross-loop rejection paths of the async secondary methods (``setinputsizes``
/ ``setoutputsize`` / ``callproc`` / ``nextset`` / ``scroll``) — cleared
before ``_check_closed()`` / ``_ensure_locks()`` raise."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from typing import Any

import pytest

from dqlitedbapi import ProgrammingError
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor

_WARNING_STALE_CURSOR: tuple[type[Exception], Exception] = (Warning, Warning("stale-cursor"))
_WARNING_STALE_CONN: tuple[type[Exception], Exception] = (Warning, Warning("stale-conn"))


def _seed(cur: Any) -> None:
    """Seed cursor- and connection-level ``messages`` so we can observe the clear."""
    cur.messages.append(_WARNING_STALE_CURSOR)
    cur._connection.messages.append(_WARNING_STALE_CONN)


def _expect_messages_cleared_after_closed_call(invoke: Callable[[Any], None], cur: Any) -> None:
    """Run ``invoke`` on a CLOSED cursor: expect InterfaceError AND cleared messages."""
    from dqlitedbapi import InterfaceError

    cur._closed = True
    with pytest.raises(InterfaceError, match="closed"):
        invoke(cur)
    assert list(cur.messages) == [], "Cursor.messages must be cleared before _check_closed raises"
    # Connection.messages is an independent surface; cursor methods must not clear it.
    assert list(cur._connection.messages) == [_WARNING_STALE_CONN]


def _drive_other_loop(invoke_async: Callable[[], Any]) -> list[BaseException]:
    """Run ``invoke_async`` in a fresh ``asyncio.run`` on a background thread
    (loop differs from the pytest-asyncio loop). Return any exceptions caught."""
    errors: list[BaseException] = []

    def _runner() -> None:
        async def _inner() -> None:
            try:
                invoke_async()
            except BaseException as e:
                errors.append(e)

        asyncio.run(_inner())

    t = threading.Thread(target=_runner)
    t.start()
    t.join()
    return errors


async def test_setinputsizes_closed_cursor_clears_messages_first() -> None:
    """``setinputsizes`` on a closed cursor clears messages and does not raise."""
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    _seed(cur)
    cur._closed = True
    cur.setinputsizes([None])
    assert list(cur.messages) == []
    assert list(cur._connection.messages) == [_WARNING_STALE_CONN]


async def test_setoutputsize_closed_cursor_clears_messages_first() -> None:
    """Same as ``setinputsizes``."""
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    _seed(cur)
    cur._closed = True
    cur.setoutputsize(64)
    assert list(cur.messages) == []
    assert list(cur._connection.messages) == [_WARNING_STALE_CONN]


async def test_callproc_closed_cursor_clears_messages_first() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    _seed(cur)
    _expect_messages_cleared_after_closed_call(lambda c: c.callproc("p"), cur)


async def test_nextset_closed_cursor_clears_messages_first() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    _seed(cur)
    _expect_messages_cleared_after_closed_call(lambda c: c.nextset(), cur)


async def test_scroll_closed_cursor_clears_messages_first() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    _seed(cur)
    _expect_messages_cleared_after_closed_call(lambda c: c.scroll(1), cur)


async def test_callproc_cross_loop_clears_messages_first() -> None:
    """Cross-loop call clears messages before ``_ensure_locks()`` raises."""
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()
    _seed(cur)

    errors = _drive_other_loop(lambda: cur.callproc("p"))
    assert errors and isinstance(errors[0], ProgrammingError), (
        f"expected ProgrammingError from cross-loop call; got {errors!r}"
    )
    assert list(cur.messages) == []
    # Connection.messages is an independent surface; cursor methods must not clear it.
    assert list(cur._connection.messages) == [_WARNING_STALE_CONN]
