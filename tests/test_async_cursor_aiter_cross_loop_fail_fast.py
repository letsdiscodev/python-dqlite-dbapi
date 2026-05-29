"""``__aiter__``'s ``_check_parent_loop_only_lazy`` swallows ``ReferenceError`` from a GC'd parent
but propagates ``ProgrammingError`` from a live-parent cross-loop mismatch (fail-fast)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import ProgrammingError


@pytest.mark.asyncio
async def test_aiter_on_live_parent_cross_loop_raises_at_aiter() -> None:
    """``aiter(cur)`` must surface the cross-loop diagnostic up front, not in ``__anext__``."""
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur.messages = []

    conn = MagicMock(spec=AsyncConnection)
    conn._check_loop_only = MagicMock(
        side_effect=ProgrammingError("AsyncConnection was first used on a different event loop")
    )
    cur._connection = conn

    with pytest.raises(ProgrammingError, match="different event loop"):
        aiter(cur)


def test_lazy_helper_directly_re_raises_programming_error() -> None:
    """Only ``ReferenceError`` is swallowed; ``ProgrammingError`` must propagate."""
    cur = AsyncCursor.__new__(AsyncCursor)
    conn = MagicMock(spec=AsyncConnection)
    conn._check_loop_only = MagicMock(side_effect=ProgrammingError("cross-loop"))
    cur._connection = conn

    with pytest.raises(ProgrammingError, match="cross-loop"):
        cur._check_parent_loop_only_lazy()


def test_lazy_helper_swallows_reference_error_only() -> None:
    """``ReferenceError`` from a dead-proxy parent is swallowed (defer to ``__anext__``)."""
    cur = AsyncCursor.__new__(AsyncCursor)
    conn = MagicMock(spec=AsyncConnection)
    conn._check_loop_only = MagicMock(side_effect=ReferenceError("dead proxy"))
    cur._connection = conn

    cur._check_parent_loop_only_lazy()  # must not raise
