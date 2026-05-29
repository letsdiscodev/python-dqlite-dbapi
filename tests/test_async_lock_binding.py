"""AsyncConnection constructed outside a running loop still works: locks are
lazily created inside ``_ensure_locks()`` on the running loop, not eagerly
bound to whatever loop was current at ``__init__`` time."""

import asyncio

from dqlitedbapi.aio.connection import AsyncConnection


class TestAsyncLockBinding:
    def test_construction_does_not_create_locks(self) -> None:
        """No asyncio.Lock exists until inside a loop."""
        conn = AsyncConnection("localhost:19001", database="x")
        assert conn._connect_lock is None
        assert conn._op_lock is None

    def test_two_separate_asyncio_run_invocations_work(self) -> None:
        """An AsyncConnection built in sync context is usable from asyncio.run()."""
        conn = AsyncConnection("localhost:19001", database="x")

        async def touch_locks() -> None:
            lock_a, lock_b = conn._ensure_locks()
            async with lock_a:
                pass
            async with lock_b:
                pass

        asyncio.run(touch_locks())
        # Stale locks from the dead first loop must not be reused on the next run.
        conn._connect_lock = None
        conn._op_lock = None
        asyncio.run(touch_locks())


class TestAsyncCloseResetsLocks:
    """close() nulls the lazy locks so reuse from a new loop can't observe
    primitives bound to the dead loop (parity with sync close())."""

    def test_close_without_ever_connecting_nulls_locks(self) -> None:
        async def scenario() -> None:
            conn = AsyncConnection("localhost:19001", database="x")
            # close() is safe even on an unused connection (_async_conn is None).
            await conn.close()
            assert conn._connect_lock is None
            assert conn._op_lock is None

        asyncio.run(scenario())


class TestLoopAffinityEnforcement:
    """After the first _ensure_locks(), the connection is pinned to that loop;
    use from another loop raises a clean ProgrammingError, not asyncio's
    internal "got Future attached to a different loop" RuntimeError."""

    def test_cross_loop_use_raises_programming_error(self) -> None:
        import asyncio

        from dqlitedbapi.aio.connection import AsyncConnection
        from dqlitedbapi.exceptions import InterfaceError, ProgrammingError

        conn = AsyncConnection("localhost:19001", database="x")

        async def touch() -> None:
            conn._ensure_locks()

        asyncio.run(touch())

        loop2 = asyncio.new_event_loop()
        try:
            # The bound loop was closed above; depending on GC timing the
            # diagnostic is InterfaceError (closed/GC'd loop) or
            # ProgrammingError (live different loop). Accept either.
            with pytest.raises((InterfaceError, ProgrammingError), match="loop"):
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
        """Close resets the pin so a later asyncio.run on the same object
        (e.g. fixture reuse) does not trip the cross-loop guard."""
        import asyncio

        from dqlitedbapi.aio.connection import AsyncConnection

        conn = AsyncConnection("localhost:19001", database="x")

        async def _scenario() -> None:
            conn._ensure_locks()
            await conn.close()

        asyncio.run(_scenario())
        assert conn._loop_ref is None


import pytest  # noqa: E402
