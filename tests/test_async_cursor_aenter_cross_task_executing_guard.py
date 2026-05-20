"""Pin: ``AsyncCursor.__aenter__`` rejects entry from a foreign task
while another task is mid-execute on the same cursor.

Without the guard, a Task B entering ``async with cur:`` while Task A
is parked inside ``_execute_unlocked`` will silently succeed and then
close the cursor on ``__aexit__``. Task A's wire response hits the
post-await ``if self._closed: return`` short-circuit, dropping the
result; Task A's next ``fetchone()`` raises
``InterfaceError("Cursor is closed")`` — pointing at fetch rather than
at the cross-task misuse.

The fix is symmetric with the ``_executing_task`` cross-task guard
already enforced by ``execute`` / ``executemany``.
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


async def test_aenter_rejects_when_cursor_executing_in_another_task() -> None:
    """A foreign task entering ``async with cur:`` while ``_executing_task``
    is pinned to another task must raise ``InterfaceError``."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()

    # Simulate "Task A is parked mid-execute on this cursor" by pinning
    # the slot directly. The real flow sets the slot at the head of
    # ``execute`` inside the try-frame; the cross-task check on
    # ``__aenter__`` only consults the slot, so staging the slot
    # directly is sufficient and avoids dependence on a live wire.
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
    """If the slot is pinned to the CURRENT task (the documented
    same-task reentry case for ``execute`` -> ``__aenter__``), the
    guard must NOT fire — the rejection is cross-task only."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()

    cur_task = asyncio.current_task()
    cur._executing_task = cur_task
    try:
        # Must not raise — same task is the documented allowed case.
        async with cur:
            pass
    finally:
        # ``__aexit__`` calls close(); the cursor is closed now. Clear
        # the slot for completeness (close already scrubs but be
        # defensive).
        cur._executing_task = None


async def test_aenter_unset_slot_admits_entry() -> None:
    """When no task holds the cursor (slot is None), ``async with cur:``
    enters cleanly. Sanity check that the guard does not regress the
    common case."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    assert cur._executing_task is None
    async with cur:
        pass
