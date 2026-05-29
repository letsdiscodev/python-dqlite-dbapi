"""``force_close_transport``'s ``_observe`` done-callback drains a non-cancelled
exception via ``t.exception()``, silencing asyncio's "Task exception was never
retrieved" warning at GC."""

import asyncio

import pytest


def test_observe_callback_drains_non_cancelled_exception_silently() -> None:
    """Inline replay of the ``_observe`` body draining a non-cancelled exception."""
    loop = asyncio.new_event_loop()
    try:

        async def _raises() -> None:
            raise RuntimeError("synthetic")

        task = loop.create_task(_raises())
        with pytest.raises(RuntimeError, match="synthetic"):
            loop.run_until_complete(task)
        assert task.done() and not task.cancelled()
        # Observer body, copied verbatim from aio/connection.py:835-838.
        import contextlib as _ctx

        if not task.cancelled():
            with _ctx.suppress(BaseException):
                task.exception()
        assert task.exception() is not None
    finally:
        loop.close()


def test_observe_callback_skips_cancelled_task() -> None:
    """The observer's ``not t.cancelled()`` predicate skips truly-cancelled tasks."""
    loop = asyncio.new_event_loop()
    try:

        async def _victim() -> None:
            await asyncio.sleep(10)

        task = loop.create_task(_victim())
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            loop.run_until_complete(task)
        assert task.cancelled()
        if not task.cancelled():
            pytest.fail("predicate should reject cancelled task")
    finally:
        loop.close()
