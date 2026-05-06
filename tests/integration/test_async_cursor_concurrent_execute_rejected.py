"""Pin: concurrent ``await cur.execute(...)`` from different tasks
on a shared ``AsyncCursor`` raises ``InterfaceError`` instead of
silently clobbering result-set state.

The connection's op_lock serialises wire calls but not the cursor's
per-execute state mutations (``_description``, ``_rows``, ``_rowcount``).
Two concurrent execute() calls on the same cursor previously left
the latest writer's state on the cursor; both callers' fetchone()
saw whichever query ran second, silently corrupting results.
asyncpg raises ``InterfaceError("cursor is already executing")`` on
the same shape; this driver now matches.
"""

from __future__ import annotations

import asyncio

import pytest

import dqlitedbapi
from dqlitedbapi.aio import aconnect


@pytest.mark.asyncio
async def test_concurrent_execute_on_one_cursor_raises(cluster_address: str) -> None:
    conn = await aconnect(cluster_address, database="test_concurrent_cursor")
    try:
        cur = conn.cursor()

        # Two concurrent execute() on the SAME cursor — at least one
        # must raise InterfaceError. Without the guard, both completed
        # silently and the loser's state was overwritten.
        ready = asyncio.Event()

        async def first() -> object:
            try:
                ready.set()
                # Slow the wire RTT artificially via a query so the
                # second task has time to enter and hit the
                # executing-task guard.
                await cur.execute("SELECT 1")
                return await cur.fetchone()
            except Exception as e:
                return e

        async def second() -> object:
            await ready.wait()
            try:
                await cur.execute("SELECT 2")
                return await cur.fetchone()
            except Exception as e:
                return e

        results = await asyncio.gather(first(), second(), return_exceptions=True)
        # At least one of the two tasks must have hit the guard. Both
        # may also raise (if they collided exactly); accept either.
        raised = [
            r for r in results if isinstance(r, dqlitedbapi.InterfaceError)
        ]
        assert raised, (
            f"expected at least one InterfaceError on concurrent execute; got {results}"
        )
        assert any(
            "already executing" in str(r) for r in raised
        ), f"InterfaceError messages: {[str(r) for r in raised]}"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_sequential_execute_same_task_works(cluster_address: str) -> None:
    """Negative pin: sequential execute() calls from the same task
    do NOT trip the guard — clearing the slot in the finally branch
    ensures back-to-back execute is unaffected."""
    conn = await aconnect(cluster_address, database="test_concurrent_cursor")
    try:
        cur = conn.cursor()
        await cur.execute("SELECT 1")
        await cur.execute("SELECT 2")
        await cur.execute("SELECT 3")
    finally:
        await conn.close()
