"""Concurrent ``execute()`` from different tasks on one ``AsyncCursor`` raises ``InterfaceError``.

The op_lock serialises wire calls but not the cursor's per-execute state mutations, so without
this guard both callers' fetchone() saw whichever query ran second. Matches asyncpg.
"""

from __future__ import annotations

import asyncio

import dqlitedbapi
from dqlitedbapi.aio import aconnect


async def test_concurrent_execute_on_one_cursor_raises(cluster_address: str) -> None:
    conn = await aconnect(cluster_address, database="test_concurrent_cursor")
    try:
        cur = conn.cursor()

        ready = asyncio.Event()

        async def first() -> object:
            try:
                ready.set()
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
        raised = [r for r in results if isinstance(r, dqlitedbapi.InterfaceError)]
        assert raised, f"expected at least one InterfaceError on concurrent execute; got {results}"
        assert any("already executing" in str(r) for r in raised), (
            f"InterfaceError messages: {[str(r) for r in raised]}"
        )
    finally:
        await conn.close()


async def test_sequential_execute_same_task_works(cluster_address: str) -> None:
    """Sequential execute() from the same task does not trip the guard."""
    conn = await aconnect(cluster_address, database="test_concurrent_cursor")
    try:
        cur = conn.cursor()
        await cur.execute("SELECT 1")
        await cur.execute("SELECT 2")
        await cur.execute("SELECT 3")
    finally:
        await conn.close()
