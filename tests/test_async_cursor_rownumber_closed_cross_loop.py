"""``AsyncCursor.rownumber`` returns None on a closed cursor regardless of loop.

The closed-state short-circuit must precede the loop-affinity check, else a
cross-loop read of a closed cursor raises instead of returning None.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor


def _make_cursor(closed: bool) -> AsyncCursor:
    """Build an AsyncCursor whose ``_check_loop_only`` raises (cross-loop access)."""
    cursor = AsyncCursor.__new__(AsyncCursor)
    cursor._closed = closed
    cursor._description = None
    cursor._row_index = 5
    cursor._rowcount = -1
    cursor._arraysize = 1
    cursor._lastrowid = None
    cursor._rows = []
    cursor._row_factory = None
    cursor._completed_iterations = 0
    cursor._executing_task = None
    cursor.messages = []

    def _check_loop_only() -> None:
        raise RuntimeError("event-loop mismatch: cursor bound to a different loop")

    conn = MagicMock()
    conn._check_loop_only = _check_loop_only
    cursor._connection = conn
    return cursor


@pytest.mark.asyncio
async def test_closed_cursor_rownumber_returns_none_even_cross_loop() -> None:
    cursor = _make_cursor(closed=True)
    assert cursor.rownumber is None


@pytest.mark.asyncio
async def test_live_cursor_rownumber_still_loop_checked() -> None:
    """Negative pin: a live cursor's rownumber still runs the loop check."""
    cursor = _make_cursor(closed=False)
    with pytest.raises(RuntimeError, match="event-loop mismatch"):
        _ = cursor.rownumber


_ = asyncio
