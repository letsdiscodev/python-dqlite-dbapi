"""Pin: ``AsyncCursor.callproc`` / ``nextset`` / ``scroll`` route
through ``_ensure_locks()`` so a call from a different event loop
surfaces the loop-binding mismatch up front rather than reporting
``NotSupportedError`` (which leaves the caller thinking the cursor
is still loop-A bound).

Sibling consistency with ``setinputsizes`` / ``setoutputsize`` (see
``test_async_cursor_setinputsizes_loop_binding.py``). The sync side
already enforces thread-affinity for all five secondary methods via
``_check_thread()``; the async side's loop-binding check is the
parallel invariant.
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
async def test_callproc_rejects_cross_loop_call() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()
    errors = _drive_other_loop(lambda: cur.callproc("p"))
    assert errors, "expected ProgrammingError from the other loop's call"
    assert isinstance(errors[0], ProgrammingError), (
        f"expected ProgrammingError (loop-binding), got {type(errors[0]).__name__}"
    )


@pytest.mark.asyncio
async def test_nextset_rejects_cross_loop_call() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()
    errors = _drive_other_loop(lambda: cur.nextset())
    assert errors
    assert isinstance(errors[0], ProgrammingError)


@pytest.mark.asyncio
async def test_scroll_rejects_cross_loop_call() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()
    errors = _drive_other_loop(lambda: cur.scroll(1))
    assert errors
    assert isinstance(errors[0], ProgrammingError)


@pytest.mark.asyncio
async def test_callproc_same_loop_raises_not_supported() -> None:
    """Sanity: same-loop calls still raise ``NotSupportedError`` —
    the loop-binding check must not change well-formed behaviour."""
    from dqlitedbapi import NotSupportedError

    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()
    with pytest.raises(NotSupportedError):
        cur.callproc("p")
    with pytest.raises(NotSupportedError):
        cur.nextset()
    with pytest.raises(NotSupportedError):
        cur.scroll(1)
