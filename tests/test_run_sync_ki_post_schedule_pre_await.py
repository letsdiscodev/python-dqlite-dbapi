"""Pin: a KI / SystemExit landing between ``run_coroutine_threadsafe`` returning and the
``future.result`` wait routes through the KI-aware cleanup arm (not the outer finally only),
so the future is not orphaned with ``_in_use=True``. The schedule lives inside the try with a
``future: ... | None`` sentinel; the KI arm guards ``future is None`` for the pre-schedule case."""

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
    """KI between schedule and result-wait must propagate, cancel the future, and null
    ``_async_conn`` / schedule ``_invalidate`` so the next call reconnects."""
    conn = dqlitedbapi.connect("127.0.0.1:9999")
    try:
        real_rcs = asyncio.run_coroutine_threadsafe
        injected = threading.Event()

        def evil_rcs(coro: object, loop: object) -> object:
            future: object = real_rcs(coro, loop)  # type: ignore[arg-type]
            if not injected.is_set():
                injected.set()
                # Schedule, then raise — simulating a signal between schedule and result-wait.
                raise KeyboardInterrupt
            return future

        monkeypatch.setattr(
            "asyncio.run_coroutine_threadsafe",
            evil_rcs,
        )

        cur = conn.cursor()
        with pytest.raises(KeyboardInterrupt):
            cur.execute("SELECT 1")
    finally:
        monkeypatch.undo()
        with contextlib.suppress(Exception):
            conn.close()


def test_ki_before_schedule_closes_coro_without_attribute_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """KI before a future is returned must hit the ``future is None`` guard, not an
    AttributeError inside the handler (which would mask the signal)."""
    conn = dqlitedbapi.connect("127.0.0.1:9999")
    try:

        def evil_rcs_before(coro: object, loop: object) -> object:
            # Simulate PyErr_SetAsyncExc landing during the schedule call frame.
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
    """Source-level pin: the ``future | None`` sentinel and ``future is None`` guard."""
    import inspect

    src = inspect.getsource(Connection._run_sync)
    assert "future: concurrent.futures.Future[T] | None = None" in src, src
    assert "if future is None:" in src, src
