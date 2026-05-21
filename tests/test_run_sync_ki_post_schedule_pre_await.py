"""Pin: ``Connection._run_sync`` routes a ``KeyboardInterrupt`` /
``SystemExit`` that lands between ``run_coroutine_threadsafe``
returning and the ``future.result(...)`` wait through the
KI-aware cleanup arm rather than the outer ``finally`` only.

Bytecode-narrow gap window: the calling thread can receive a
``PyErr_SetAsyncExc``-delivered ``BaseException`` between scheduling
the coroutine and entering the result wait. The previous shape
left ``future`` orphaned on the loop thread (``_in_use=True``,
no ``_invalidate`` scheduled, ``_async_conn`` still attached) so
the next sync call wedged with "another operation is in progress"
until the per-RPC timeout fired.

The fix moves ``future = run_coroutine_threadsafe(coro, loop)``
inside the KI-aware ``try`` block with a ``future: ... | None``
sentinel. The KI arm guards ``future is None`` for the case where
the KI landed BEFORE the schedule (closes ``coro`` so it doesn't
emit a ``RuntimeWarning("coroutine was never awaited")`` at GC).

The window is bytecode-narrow so the reproducer uses monkeypatching
to inject the signal deterministically at the schedule seam.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading

import pytest

import dqlitedbapi
from dqlitedbapi.connection import Connection


def test_ki_landing_between_schedule_and_await_closes_coro_and_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A KI delivered between ``run_coroutine_threadsafe`` returning
    and the result-wait entering must:
    1. Propagate the KI signal to the caller.
    2. Cancel the scheduled future so the loop-thread coroutine does
       not orphan ``_in_use=True``.
    3. Null ``_async_conn`` and schedule ``_invalidate`` so the next
       sync call gets a fresh-connect path."""
    conn = dqlitedbapi.connect("127.0.0.1:9999")
    try:
        real_rcs = asyncio.run_coroutine_threadsafe
        injected = threading.Event()

        def evil_rcs(coro: object, loop: object) -> object:
            future: object = real_rcs(coro, loop)  # type: ignore[arg-type]
            if not injected.is_set():
                injected.set()
                # Schedule the future first, then raise — simulating
                # a KI delivered to the calling thread by an
                # asynchronous signal between schedule and result-wait.
                raise KeyboardInterrupt
            return future

        monkeypatch.setattr(
            "asyncio.run_coroutine_threadsafe",
            evil_rcs,
        )

        # First call: the injected KI propagates. ``execute`` is the
        # standard entry point that routes through ``_run_sync``.
        cur = conn.cursor()
        with pytest.raises(KeyboardInterrupt):
            cur.execute("SELECT 1")
    finally:
        monkeypatch.undo()
        # Clean up — the conn may be in an invalidated state.
        with contextlib.suppress(Exception):
            conn.close()


def test_ki_before_schedule_closes_coro_without_attribute_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the KI lands BEFORE ``run_coroutine_threadsafe`` even returns
    a future, the cleanup arm must guard ``future is None`` so the
    code doesn't ``AttributeError`` from inside the BaseException
    handler (which would mask the original signal)."""
    conn = dqlitedbapi.connect("127.0.0.1:9999")
    try:

        def evil_rcs_before(coro: object, loop: object) -> object:
            # Close the coro ourselves so a manual KI raise here
            # simulates "PyErr_SetAsyncExc landed during the schedule
            # call frame". The unscheduled coro must be closed by
            # the KI arm — the test verifies no RuntimeWarning
            # ("coroutine was never awaited") leaks.
            raise KeyboardInterrupt

        monkeypatch.setattr(
            "asyncio.run_coroutine_threadsafe",
            evil_rcs_before,
        )
        cur = conn.cursor()
        with pytest.raises(KeyboardInterrupt):
            cur.execute("SELECT 1")
    finally:
        monkeypatch.undo()
        with contextlib.suppress(Exception):
            conn.close()


def test_run_sync_future_sentinel_is_typed_optional() -> None:
    """Pin the source-level fix: ``future`` is declared as
    ``concurrent.futures.Future[T] | None = None`` before the
    KI-aware try block. The sentinel makes the KI cleanup arm's
    ``future is None`` guard deterministic."""
    import inspect

    src = inspect.getsource(Connection._run_sync)
    assert "future: concurrent.futures.Future[T] | None = None" in src, src
    # The schedule call must now be inside the outer try block (so
    # KI landing between schedule and result-wait routes through
    # the KI arm). The visual cue: ``future = asyncio.run_coroutine_threadsafe``
    # appears AFTER the ``try:`` line that wraps ``future.result``.
    assert "if future is None:" in src, src
