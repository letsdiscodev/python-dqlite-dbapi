"""Pin: ``AsyncConnection.force_close_transport`` re-snapshots
``inner._pending_drain`` so a concurrent ``_invalidate`` queued via
``loop.call_soon_threadsafe`` cannot orphan a freshly-created drain
task.

The race shape: ``force_close_transport`` runs from a foreign thread
(SA finalize, atexit, GC). Pre-fix, it snapshotted
``pending = getattr(inner, "_pending_drain", None)`` once near entry,
then later wrote ``inner._pending_drain = None``. Between those two
operations, a ``call_soon_threadsafe(_invalidate, ...)`` queued by the
loop-thread coroutine could fire and CREATE a fresh ``_pending_drain``
task. The cross-thread cancel only acted on the OLD task; the NEW
task was then nulled with no observer/cancel — orphaned, surfacing
as "Task was destroyed but it is pending" at GC.

The fix mirrors the in-thread re-snapshot loop in
``DqliteConnection._close_impl`` (cycle-27 R27_1 cap-and-fail-loud
discipline): repeat snapshot+null+cancel for up to 3 iterations; on
cap-exhaustion, log a WARNING and leave the residual task cancelled.
"""

from __future__ import annotations

import asyncio
import logging
import os
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


class _SeqPendingInner:
    """Inner-conn double whose ``_pending_drain`` returns a configured
    sequence of tasks on each read, simulating a concurrent
    ``_invalidate`` re-publishing a fresh task between our snapshot
    and our null-out.
    """

    def __init__(self, sequence: list[object]) -> None:
        self._seq_iter = iter(sequence)
        self._closed = False
        self._protocol = None

    @property
    def _pending_drain(self) -> object:
        try:
            return next(self._seq_iter)
        except StopIteration:
            return None

    @_pending_drain.setter
    def _pending_drain(self, value: object) -> None:
        # Null-out is a no-op for the sequence — the next read
        # automatically yields the next value in the sequence,
        # simulating the concurrent _invalidate side-effect.
        pass


def _build_aconn(inner: object) -> AsyncConnection:
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._creator_pid = os.getpid()
    aconn._loop_ref = None
    aconn._closed_flag = [False]
    aconn._async_conn = inner  # type: ignore[assignment]
    return aconn


def test_force_close_resnapshots_when_invalidate_creates_new_pending() -> None:
    """Simulate a concurrent ``_invalidate`` running between our
    snapshot and our null-out: each null is followed by a fresh task
    appearing in the slot. The re-snapshot loop must see the new
    tasks and cancel each (up to the cap).
    """
    closed_loop = asyncio.new_event_loop()
    closed_loop.close()

    initial_task = MagicMock()
    initial_task.done.return_value = False
    initial_task.get_loop.return_value = closed_loop
    fresh_tasks = [MagicMock() for _ in range(2)]
    for t in fresh_tasks:
        t.done.return_value = False
        t.get_loop.return_value = closed_loop

    # Sequence: initial → fresh[0] → fresh[1] → None (converges).
    inner = _SeqPendingInner([initial_task, *fresh_tasks, None])
    aconn = _build_aconn(inner)

    aconn.force_close_transport()

    # All three tasks (initial + 2 fresh) should have had cancel called.
    initial_task.cancel.assert_called_once()
    for t in fresh_tasks:
        t.cancel.assert_called_once()


def test_force_close_logs_warning_when_resnapshot_cap_exhausted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If ``_invalidate`` keeps creating fresh pending tasks each
    iteration (pathological feedback loop), the re-snapshot loop hits
    the cap (3) and a WARNING fires.
    """
    closed_loop = asyncio.new_event_loop()
    closed_loop.close()

    def _make_pending() -> MagicMock:
        t = MagicMock()
        t.done.return_value = False
        t.get_loop.return_value = closed_loop
        return t

    # 10 fresh tasks — never converges; the cap kicks in at 3.
    inner = _SeqPendingInner([_make_pending() for _ in range(10)])
    aconn = _build_aconn(inner)

    with caplog.at_level(logging.WARNING, logger="dqlitedbapi.aio.connection"):
        aconn.force_close_transport()

    warning_records = [
        r
        for r in caplog.records
        if r.levelname == "WARNING" and "re-snapshot iterations" in r.message
    ]
    assert warning_records, (
        f"expected WARNING about re-snapshot cap exhausted; "
        f"got {[r.message for r in caplog.records]}"
    )


def test_force_close_no_resnapshot_loop_iteration_when_pending_initially_none() -> None:
    """Negative pin: when there is no pending drain at entry, the
    loop body breaks on the first iteration. No exception, no
    warning, no cap-exhaustion path.
    """
    inner = _SeqPendingInner([None])
    aconn = _build_aconn(inner)
    aconn.force_close_transport()
    # No exception raised. _async_conn nulled at the tail.
    assert aconn._async_conn is None
