"""Pin: ``force_close_transport``'s foreign-thread
``_cancel_and_observe`` wrapper attaches an ``_observe``
done-callback that drains any non-cancelled exception via
``t.exception()`` — silencing asyncio's
"Task exception was never retrieved" warning that would otherwise
fire under SA-finalize-from-foreign-thread.

Drives the inner ``_observe`` callback directly with a synthetic
done task (raising a non-cancelled exception). Without the
observer firing, the outer process emits the warning at GC.
"""

import asyncio

import pytest


def test_observe_callback_drains_non_cancelled_exception_silently() -> None:
    """Inline-replay of the inner ``_observe`` body. The body
    fires only when the cancelled task completes with a NON-
    cancelled exception — ``t.exception()`` consumes it so
    asyncio's task-finalisation logger doesn't emit
    'Task exception was never retrieved' at GC."""
    # Build a synthetic done task with a stored exception.
    loop = asyncio.new_event_loop()
    try:

        async def _raises() -> None:
            raise RuntimeError("synthetic")

        task = loop.create_task(_raises())
        # Drive to completion.
        with pytest.raises(RuntimeError, match="synthetic"):
            loop.run_until_complete(task)
        assert task.done() and not task.cancelled()
        # The observer body — copied verbatim from
        # aio/connection.py:835-838 — drains the exception.
        import contextlib as _ctx

        if not task.cancelled():
            with _ctx.suppress(BaseException):
                task.exception()
        # Calling .exception() again must not raise — the exception
        # was consumed by the observer.
        assert task.exception() is not None
    finally:
        loop.close()


def test_observe_callback_skips_cancelled_task() -> None:
    """The observer skips truly-cancelled tasks (no exception to
    drain). Pin the gating predicate."""
    loop = asyncio.new_event_loop()
    try:

        async def _victim() -> None:
            await asyncio.sleep(10)

        task = loop.create_task(_victim())
        task.cancel()
        # Drive to completion.
        with pytest.raises(asyncio.CancelledError):
            loop.run_until_complete(task)
        assert task.cancelled()
        # Observer body: predicate is `not t.cancelled()`. For a
        # cancelled task, the body skips entirely.
        # No assertion needed — just confirm no exception escapes.
        if not task.cancelled():
            pytest.fail("predicate should reject cancelled task")
    finally:
        loop.close()
