"""Pin: ``AsyncCursor.__aenter__`` rejects entry from a foreign task while another task is
mid-execute on the same cursor (symmetric with the execute/executemany cross-task guard)."""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


async def test_aenter_rejects_when_cursor_executing_in_another_task() -> None:
    """Entering ``async with cur:`` while ``_executing_task`` is pinned to another task
    must raise ``InterfaceError``."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()

    # Stage the slot directly to simulate Task A parked mid-execute; __aenter__ only
    # consults the slot, so this avoids needing a live wire.
    other_task = asyncio.create_task(asyncio.sleep(60))
    try:
        cur._executing_task = other_task

        with pytest.raises(InterfaceError, match="executing in another task"):
            async with cur:
                pass
    finally:
        cur._executing_task = None
        other_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await other_task


async def test_aenter_same_task_with_slot_set_does_not_raise() -> None:
    """The guard is cross-task only: a slot pinned to the CURRENT task must NOT fire."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()

    cur_task = asyncio.current_task()
    cur._executing_task = cur_task
    try:
        async with cur:
            pass
    finally:
        cur._executing_task = None


async def test_aenter_unset_slot_admits_entry() -> None:
    """When no task holds the cursor (slot is None), ``async with cur:`` enters cleanly."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    assert cur._executing_task is None
    async with cur:
        pass
