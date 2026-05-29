"""setinputsizes/setoutputsize route through ``_ensure_locks()`` so a cross-loop
call surfaces the loop-binding mismatch instead of silently succeeding.
"""

from __future__ import annotations

import asyncio

from dqlitedbapi import ProgrammingError
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


async def test_setinputsizes_rejects_cross_loop_call() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()  # prime the binding on the outer loop

    errors: list[BaseException] = []

    def _on_other_loop() -> None:
        async def _invoke() -> None:
            try:
                cur.setinputsizes([None])
            except BaseException as e:
                errors.append(e)

        asyncio.run(_invoke())

    # Run in a thread so its asyncio.run does not interfere with the outer loop.
    import threading

    t = threading.Thread(target=_on_other_loop)
    t.start()
    t.join()
    assert errors, "expected a ProgrammingError from the other loop's call"
    assert isinstance(errors[0], ProgrammingError)


async def test_setinputsizes_accepts_same_loop_call() -> None:
    """Sanity: the binding check must not reject a same-loop call."""
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()
    cur.setinputsizes([None])  # no raise
    cur.setoutputsize(100)
