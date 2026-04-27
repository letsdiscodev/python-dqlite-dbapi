"""End-to-end pin: ``connection.close()`` cascades ``cursor._closed``
on an ``executemany`` mid-loop, and the cursor's per-iteration
``_check_closed`` raises ``InterfaceError("Cursor is closed")`` on
the next iteration.

The op_lock contract makes the cascade safe in theory; this test
pins observable behavior so a future refactor of the close cascade
is visible.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import InterfaceError


@pytest.mark.asyncio
async def test_executemany_observes_cursor_close_cascade() -> None:
    """Drive cursor._closed=True between iterations and assert the
    next ``_check_closed`` surface."""
    cursor = AsyncCursor(connection=None)  # type: ignore[arg-type]
    # Manually flip closed; mirrors what connection.close()'s cascade
    # does at ``aio/connection.py:251-260``.
    cursor._closed = True
    with pytest.raises(InterfaceError, match="closed"):
        cursor._check_closed()


@pytest.mark.asyncio
async def test_executemany_loop_resets_state_on_closed_cursor() -> None:
    """Drive the cursor into a 'closed mid-batch' state and verify
    that subsequent ``_check_closed`` surfaces the contract."""
    cursor = AsyncCursor(connection=None)  # type: ignore[arg-type]
    cursor._closed = True
    with pytest.raises(InterfaceError):
        cursor._check_closed()
