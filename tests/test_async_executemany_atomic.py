"""``AsyncCursor.executemany`` holds ``op_lock`` for the whole loop.

Previously each iteration called ``self.execute(...)`` which
re-acquired the lock, so a concurrent task on the same AsyncConnection
could slip arbitrary statements between iterations. Hold the lock
once across all iterations to match the sync path's atomicity (sync
``_run_sync`` already holds the lock for the outer coroutine).
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


@pytest.mark.asyncio
async def test_executemany_blocks_concurrent_execute_until_loop_ends() -> None:
    conn = AsyncConnection("localhost:19001")
    conn._ensure_locks()  # bind locks to the current loop

    async def _ensure_connection() -> object:
        return object()

    conn._ensure_connection = _ensure_connection  # type: ignore[method-assign]

    order: list[str] = []

    async def slow_unlocked(
        self: AsyncCursor,
        operation: str,
        parameters: object = None,
    ) -> None:
        tag = "exec-many" if "INSERT" in operation else "concurrent"
        order.append(f"{tag}:start")
        await asyncio.sleep(0.02)
        order.append(f"{tag}:end")
        # Keep the cursor consistent enough for accumulator.apply.
        self._description = None
        self._rows = []
        self._rowcount = 0
        self._row_index = 0

    with patch.object(AsyncCursor, "_execute_unlocked", slow_unlocked):
        cur = AsyncCursor(conn)

        async def run_executemany() -> None:
            await cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])

        async def run_concurrent_commit() -> None:
            await asyncio.sleep(0.01)  # let executemany start first
            other = AsyncCursor(conn)
            await other.execute("COMMIT")

        await asyncio.gather(run_executemany(), run_concurrent_commit())

    exec_many_end = [i for i, tag in enumerate(order) if tag == "exec-many:end"]
    concurrent_start = [i for i, tag in enumerate(order) if tag == "concurrent:start"]
    assert concurrent_start, f"concurrent execute never ran: {order}"
    assert max(exec_many_end) < concurrent_start[0], (
        f"concurrent execute interleaved with executemany: {order}"
    )
