"""Pin: ``AsyncCursor.__aiter__``'s ``_check_parent_loop_only_lazy``
helper silently swallows ``ReferenceError`` from a GC'd parent BUT
propagates ``ProgrammingError`` from a live-parent cross-loop
mismatch — the deliberate async-divergence-from-sync fail-fast
documented at ``__aiter__``'s note block.

A regression that broadens the helper's
``except ReferenceError`` to ``except (ReferenceError,
ProgrammingError)`` — a "harmless" simplification claiming to
"match sync semantics" — would silently swallow the cross-loop
fail-fast and defer the misuse to the first ``__anext__`` await.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import ProgrammingError


@pytest.mark.asyncio
async def test_aiter_on_live_parent_cross_loop_raises_at_aiter() -> None:
    """``aiter(cur)`` must surface the cross-loop diagnostic up
    front; not defer to ``__anext__``. This is the divergence-from-
    sync that ``__aiter__``'s note explicitly specifies."""
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
    """Unit-level pin: only ``ReferenceError`` is swallowed. A
    ``ProgrammingError`` from ``_check_loop_only`` must propagate.
    A regression broadening the helper's ``except`` clause would
    fail this pin."""
    cur = AsyncCursor.__new__(AsyncCursor)
    conn = MagicMock(spec=AsyncConnection)
    conn._check_loop_only = MagicMock(side_effect=ProgrammingError("cross-loop"))
    cur._connection = conn

    with pytest.raises(ProgrammingError, match="cross-loop"):
        cur._check_parent_loop_only_lazy()


def test_lazy_helper_swallows_reference_error_only() -> None:
    """Positive twin: ``ReferenceError`` from a dead-proxy parent IS
    swallowed (defer to ``__anext__``), matching the documented
    stdlib-parity ``iter(closed_cur) is closed_cur`` contract."""
    cur = AsyncCursor.__new__(AsyncCursor)
    conn = MagicMock(spec=AsyncConnection)
    conn._check_loop_only = MagicMock(side_effect=ReferenceError("dead proxy"))
    cur._connection = conn

    # Must NOT raise.
    cur._check_parent_loop_only_lazy()
