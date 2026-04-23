"""``AsyncConnection.cursor()`` best-effort loop-binding check.

Sync ``Connection.cursor()`` calls ``_check_thread()``; the async
sibling had no parallel guard. A caller who bound the connection to
one event loop and then called ``cursor()`` from a different loop
got no diagnostic until the first await inside ``_ensure_locks``.

The async ``cursor()`` is sync by design (SQLAlchemy calls it from
sync context within its greenlet adapter), so a hard
``asyncio.get_running_loop()`` requirement would break SA. The
guard is best-effort: skip when no loop is running (SA greenlet
glue), raise ``ProgrammingError`` when a running loop differs from
the bound loop.
"""

from __future__ import annotations

import asyncio
import weakref

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import ProgrammingError


class TestCursorLoopBinding:
    def test_cursor_with_no_running_loop_allowed(self) -> None:
        """SA greenlet case: ``cursor()`` from sync context (no loop)
        skips the check cleanly.
        """
        conn = AsyncConnection("localhost:9001")
        # No running loop, no bound loop: trivially allowed.
        cur = conn.cursor()
        assert cur is not None

    def test_cursor_with_matching_loop_allowed(self) -> None:
        async def run() -> None:
            conn = AsyncConnection("localhost:9001")
            # Pretend we already bound to this loop.
            conn._loop_ref = weakref.ref(asyncio.get_running_loop())
            cur = conn.cursor()
            assert cur is not None

        asyncio.run(run())

    def test_cursor_with_mismatching_loop_rejected(self) -> None:
        conn = AsyncConnection("localhost:9001")
        # Keep a live reference to loop A so its weakref stays valid
        # across the call on loop B.
        loop_a = asyncio.new_event_loop()
        try:
            conn._loop_ref = weakref.ref(loop_a)

            async def use_on_different_loop() -> None:
                with pytest.raises(ProgrammingError, match="different event loop"):
                    conn.cursor()

            asyncio.run(use_on_different_loop())
        finally:
            loop_a.close()
