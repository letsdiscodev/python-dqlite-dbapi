"""Pin: ``force_close_transport`` re-snapshots ``inner._pending_drain`` so a
concurrent ``_invalidate`` (queued via ``call_soon_threadsafe`` on the loop
thread) cannot orphan a freshly-created drain task between snapshot and null-out.
Re-snapshot+cancel loops up to 3 iterations, then logs a WARNING on cap-exhaust.
"""

from __future__ import annotations

import asyncio
import logging
import os
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


class _SeqPendingInner:
    """Inner-conn double whose ``_pending_drain`` yields a configured sequence
    on each read, simulating a concurrent ``_invalidate`` re-publishing a task.
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
        # No-op: the next read yields the next sequence value, simulating
        # the concurrent _invalidate side-effect.
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
    """Each null is followed by a fresh task in the slot; the re-snapshot
    loop must see and cancel each (up to the cap)."""
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

    initial_task.cancel.assert_called_once()
    for t in fresh_tasks:
        t.cancel.assert_called_once()


def test_force_close_logs_warning_when_resnapshot_cap_exhausted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A pathological feedback loop hits the cap (3) and fires a WARNING."""
    closed_loop = asyncio.new_event_loop()
    closed_loop.close()

    def _make_pending() -> MagicMock:
        t = MagicMock()
        t.done.return_value = False
        t.get_loop.return_value = closed_loop
        return t

    # Never converges; the cap kicks in at 3.
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
    """Negative pin: no pending drain at entry breaks the loop on the first
    iteration — no exception, warning, or cap-exhaustion path."""
    inner = _SeqPendingInner([None])
    aconn = _build_aconn(inner)
    aconn.force_close_transport()
    assert aconn._async_conn is None
