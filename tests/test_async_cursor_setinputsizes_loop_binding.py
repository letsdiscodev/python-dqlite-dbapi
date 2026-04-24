"""``AsyncCursor.setinputsizes`` / ``setoutputsize`` now route through
``_ensure_locks()`` so a call from a different event loop surfaces the
loop-binding mismatch up front rather than silently succeeding.
"""

from __future__ import annotations

import asyncio

import pytest

from dqlitedbapi import ProgrammingError
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


@pytest.mark.asyncio
async def test_setinputsizes_rejects_cross_loop_call() -> None:
    """Bind the connection on this loop, then invoke the no-op sync
    method from a fresh ``asyncio.run`` — the loop-binding check must
    surface ``ProgrammingError``, matching the behaviour of every other
    cursor accessor."""
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    # Prime the binding on the outer loop.
    conn._ensure_locks()

    errors: list[BaseException] = []

    def _on_other_loop() -> None:
        async def _invoke() -> None:
            try:
                cur.setinputsizes([None])
            except BaseException as e:
                errors.append(e)

        asyncio.run(_invoke())

    # Run the other-loop call in a thread so its ``asyncio.run`` does
    # not interfere with the outer pytest-asyncio loop.
    import threading

    t = threading.Thread(target=_on_other_loop)
    t.start()
    t.join()
    assert errors, "expected a ProgrammingError from the other loop's call"
    assert isinstance(errors[0], ProgrammingError)


@pytest.mark.asyncio
async def test_setinputsizes_accepts_same_loop_call() -> None:
    """Sanity: the binding check must NOT reject a call from the same
    loop the connection was first used on."""
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()
    cur.setinputsizes([None])  # no raise
    cur.setoutputsize(100)
