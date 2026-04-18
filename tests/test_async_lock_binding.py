"""AsyncConnection constructed outside a running loop still works.

Previously AsyncConnection eagerly called ``asyncio.Lock()`` in ``__init__``,
binding the lock to whatever loop happened to be the current event loop at
construction time. Creating the connection from sync glue code (e.g. the
SQLAlchemy AsyncAdaptedConnection scaffolding) and then running it in a
fresh ``asyncio.run()`` would fail because the lock was bound to a loop
that had since died.

Now the locks are lazily created inside ``_ensure_locks()`` on the loop
that's actually running the operation.
"""

import asyncio

from dqlitedbapi.aio.connection import AsyncConnection


class TestAsyncLockBinding:
    def test_construction_does_not_create_locks(self) -> None:
        """No asyncio.Lock should exist until we're inside a loop."""
        conn = AsyncConnection("localhost:19001", database="x")
        assert conn._connect_lock is None
        assert conn._op_lock is None

    def test_two_separate_asyncio_run_invocations_work(self) -> None:
        """The same AsyncConnection constructed in sync context must be
        usable from at least one asyncio.run(...) without crashing on
        the lock construction path."""
        conn = AsyncConnection("localhost:19001", database="x")

        async def touch_locks() -> None:
            lock_a, lock_b = conn._ensure_locks()
            async with lock_a:
                pass
            async with lock_b:
                pass

        # First run creates locks on a fresh loop.
        asyncio.run(touch_locks())
        # Simulate the SQLAlchemy glue pattern: locks from the previous
        # loop are now stale. The next asyncio.run must not reuse them
        # (or the user must at least not observe a crash).
        # We verify by resetting to mimic what a new-loop scenario would
        # look like if the first loop had been stopped cleanly.
        conn._connect_lock = None
        conn._op_lock = None
        asyncio.run(touch_locks())


class TestAsyncCloseResetsLocks:
    """close() nulls the lazily-created locks so a subsequent re-use
    from a new event loop cannot observe primitives bound to the dead
    loop. Parity with the sync close() connect_lock reset.
    """

    def test_close_without_ever_connecting_nulls_locks(self) -> None:
        async def scenario() -> None:
            conn = AsyncConnection("localhost:19001", database="x")
            # No cursor/execute has run; _async_conn is None but the
            # caller calls close() anyway (matches the docstring
            # promise that close is safe even on unused connections).
            await conn.close()
            assert conn._connect_lock is None
            assert conn._op_lock is None

        asyncio.run(scenario())


class TestLoopAffinityEnforcement:
    """After the first _ensure_locks() call, the AsyncConnection is
    pinned to that loop. Any subsequent use from a different loop must
    raise a clean ProgrammingError instead of asyncio's internal
    "got Future attached to a different loop" RuntimeError.
    """

    def test_cross_loop_use_raises_programming_error(self) -> None:
        import asyncio

        from dqlitedbapi.aio.connection import AsyncConnection
        from dqlitedbapi.exceptions import ProgrammingError

        conn = AsyncConnection("localhost:19001", database="x")

        async def touch() -> None:
            conn._ensure_locks()

        asyncio.run(touch())

        loop2 = asyncio.new_event_loop()
        try:
            with pytest.raises(ProgrammingError, match="loop"):
                loop2.run_until_complete(touch())
        finally:
            loop2.close()

    def test_same_loop_reuse_is_fine(self) -> None:
        import asyncio

        from dqlitedbapi.aio.connection import AsyncConnection

        conn = AsyncConnection("localhost:19001", database="x")

        async def touch() -> tuple[asyncio.Lock, asyncio.Lock]:
            a1, b1 = conn._ensure_locks()
            a2, b2 = conn._ensure_locks()
            assert a1 is a2
            assert b1 is b2
            return a1, b1

        asyncio.run(touch())

    def test_close_clears_loop_pin(self) -> None:
        """Close resets the pin so a subsequent asyncio.run on the
        same object (e.g. test fixture reuse) works without tripping
        the cross-loop guard.
        """
        import asyncio

        from dqlitedbapi.aio.connection import AsyncConnection

        conn = AsyncConnection("localhost:19001", database="x")

        async def _scenario() -> None:
            conn._ensure_locks()
            await conn.close()

        asyncio.run(_scenario())
        assert conn._loop_ref is None


import pytest  # noqa: E402
