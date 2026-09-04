"""``AsyncConnection.cursor()`` best-effort loop-binding check.

cursor() is sync by design (SQLAlchemy calls it from sync greenlet context), so
the guard skips when no loop is running and only raises on a mismatched running loop.
"""

from __future__ import annotations

import asyncio
import weakref

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import ProgrammingError


class TestCursorLoopBinding:
    def test_cursor_with_no_running_loop_allowed(self) -> None:
        """SA greenlet case: cursor() from sync context (no loop) skips the check."""
        conn = AsyncConnection("localhost:9001")
        cur = conn.cursor()
        assert cur is not None

    def test_cursor_with_matching_loop_allowed(self) -> None:
        async def run() -> None:
            conn = AsyncConnection("localhost:9001")
            conn._loop_ref = weakref.ref(asyncio.get_running_loop())
            cur = conn.cursor()
            assert cur is not None

        asyncio.run(run())

    def test_cursor_with_mismatching_loop_rejected(self) -> None:
        conn = AsyncConnection("localhost:9001")
        # Keep loop A alive so its weakref stays valid across the call on loop B.
        loop_a = asyncio.new_event_loop()
        try:
            conn._loop_ref = weakref.ref(loop_a)

            async def use_on_different_loop() -> None:
                with pytest.raises(ProgrammingError, match="different event loop"):
                    conn.cursor()

            asyncio.run(use_on_different_loop())
        finally:
            loop_a.close()
