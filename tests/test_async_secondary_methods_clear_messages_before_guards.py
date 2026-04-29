"""Pin: PEP 249 §6.1.1's "messages cleared automatically by all standard
cursor method calls (prior to executing the call)" must hold even on
the closed-cursor and cross-loop rejection paths of the async cursor's
secondary methods (``setinputsizes`` / ``setoutputsize`` /
``callproc`` / ``nextset`` / ``scroll``).

The sync side already clears ``messages`` before invoking
``_check_thread()`` (pinned by
``test_secondary_methods_clear_messages_before_thread_check``). The
async side must clear before invoking ``_check_closed()`` and
``_ensure_locks()`` for symmetric PEP 249 conformance.

Severity is low (``messages`` is empty in practice today) but the
ordering symmetry locks the contract for any future code path that
begins populating ``messages``.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from typing import Any

import pytest

from dqlitedbapi import ProgrammingError
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


def _seed(cur: Any) -> None:
    """Seed both cursor- and connection-level ``messages`` lists so
    we can observe the clear."""
    cur.messages.append((Warning, "stale-cursor"))
    cur._connection.messages.append((Warning, "stale-conn"))


def _expect_messages_cleared_after_closed_call(invoke: Callable[[Any], None], cur: Any) -> None:
    """Run ``invoke(cur)`` on a CLOSED cursor; expect
    ``InterfaceError`` per PEP 249 §6.1.2 AND assert messages were
    cleared per §6.1.1."""
    from dqlitedbapi import InterfaceError

    cur._closed = True
    with pytest.raises(InterfaceError, match="closed"):
        invoke(cur)
    assert list(cur.messages) == [], "Cursor.messages must be cleared before _check_closed raises"
    assert list(cur._connection.messages) == [], (
        "Connection.messages must be cleared before _check_closed raises"
    )


def _drive_other_loop(invoke_async: Callable[[], Any]) -> list[BaseException]:
    """Run ``invoke_async`` inside a fresh ``asyncio.run`` on a
    background thread so its loop differs from the outer pytest-asyncio
    loop. Return any exceptions caught."""
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


@pytest.mark.asyncio
async def test_setinputsizes_closed_cursor_clears_messages_first() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    _seed(cur)
    _expect_messages_cleared_after_closed_call(lambda c: c.setinputsizes([None]), cur)


@pytest.mark.asyncio
async def test_setoutputsize_closed_cursor_clears_messages_first() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    _seed(cur)
    _expect_messages_cleared_after_closed_call(lambda c: c.setoutputsize(64), cur)


@pytest.mark.asyncio
async def test_callproc_closed_cursor_clears_messages_first() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    _seed(cur)
    _expect_messages_cleared_after_closed_call(lambda c: c.callproc("p"), cur)


@pytest.mark.asyncio
async def test_nextset_closed_cursor_clears_messages_first() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    _seed(cur)
    _expect_messages_cleared_after_closed_call(lambda c: c.nextset(), cur)


@pytest.mark.asyncio
async def test_scroll_closed_cursor_clears_messages_first() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    _seed(cur)
    _expect_messages_cleared_after_closed_call(lambda c: c.scroll(1), cur)


@pytest.mark.asyncio
async def test_callproc_cross_loop_clears_messages_first() -> None:
    """Cross-loop call must clear messages before
    ``_ensure_locks()`` raises ``ProgrammingError``. Symmetric with
    the sync side's _check_thread cross-thread test."""
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()
    _seed(cur)

    errors = _drive_other_loop(lambda: cur.callproc("p"))
    assert errors and isinstance(errors[0], ProgrammingError), (
        f"expected ProgrammingError from cross-loop call; got {errors!r}"
    )
    assert list(cur.messages) == []
    assert list(cur._connection.messages) == []
