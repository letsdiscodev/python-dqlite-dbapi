"""Pin: ``Connection._run_sync`` KeyboardInterrupt / SystemExit
arm includes the same race-recovery branch as the timeout arm —
if the future completed successfully between ``Future.result(...)``
raising the signal and our cleanup, the cleanup MUST skip
``_invalidate`` so the connection stays reusable.

Without the race-recovery, a SIGINT arriving on the same
scheduling tick a successful insert resolved produces a spurious
reconnect on the next call.
"""

import concurrent.futures
import os
import threading
from unittest.mock import MagicMock, patch

import pytest

import dqlitedbapi


def _prime_connection() -> dqlitedbapi.Connection:
    conn = dqlitedbapi.Connection.__new__(dqlitedbapi.Connection)
    conn._timeout = 0.5
    conn._closed = False
    conn._async_conn = MagicMock()  # sentinel — must NOT be nulled on race-recovery
    conn._creator_pid = os.getpid()
    conn._op_lock = threading.RLock()  # type: ignore[assignment]
    return conn


def test_ki_arm_skips_invalidate_when_future_already_done() -> None:
    """The cleanup MUST skip _invalidate when the future is
    already done (raced to success against the signal) — pin
    that ``_async_conn`` is NOT nulled in that case."""
    conn = _prime_connection()
    sentinel_conn = conn._async_conn  # capture before _run_sync

    fake_future = MagicMock(spec=concurrent.futures.Future)
    # First Future.result raises KI; the future is concurrently done.
    fake_future.result = MagicMock(
        side_effect=[KeyboardInterrupt(), None]  # 2nd call: drain done-future
    )
    fake_future.cancel = MagicMock()
    fake_future.done = MagicMock(return_value=True)
    fake_future.cancelled = MagicMock(return_value=False)

    fake_loop = MagicMock()

    invalidate_calls: list[object] = []

    def call_soon_threadsafe(callback: object, *args: object) -> None:
        invalidate_calls.append((callback, args))

    fake_loop.call_soon_threadsafe = call_soon_threadsafe

    async def _victim() -> None:
        return None

    coro = _victim()
    try:
        with (
            patch.object(conn, "_ensure_loop", return_value=fake_loop),
            patch(
                "dqlitedbapi.connection.asyncio.run_coroutine_threadsafe",
                return_value=fake_future,
            ),
            pytest.raises(KeyboardInterrupt),
        ):
            conn._run_sync(coro)
    finally:
        coro.close()

    # Race-recovery branch: _async_conn is NOT nulled.
    assert conn._async_conn is sentinel_conn, (
        "race-recovery should preserve _async_conn for the next call"
    )
    # _invalidate was NOT scheduled (no call_soon_threadsafe to it).
    assert not invalidate_calls, (
        f"race-recovery should skip _invalidate scheduling; got: {invalidate_calls}"
    )


def test_ki_arm_invalidates_when_future_still_pending() -> None:
    """Positive control: the OPPOSITE branch — future is still
    pending when KI raises — must invalidate as before. This pins
    the existing wedge-cleanup behaviour."""
    conn = _prime_connection()

    fake_future = MagicMock(spec=concurrent.futures.Future)
    fake_future.result = MagicMock(
        side_effect=[
            KeyboardInterrupt(),
            concurrent.futures.TimeoutError(),  # bounded-wait absorbs
        ]
    )
    fake_future.cancel = MagicMock()
    fake_future.done = MagicMock(return_value=False)  # still pending
    fake_future.cancelled = MagicMock(return_value=False)

    fake_loop = MagicMock()
    invalidate_calls: list[object] = []
    fake_loop.call_soon_threadsafe = lambda cb, *args: invalidate_calls.append((cb, args))

    async def _victim() -> None:
        return None

    coro = _victim()
    try:
        with (
            patch.object(conn, "_ensure_loop", return_value=fake_loop),
            patch(
                "dqlitedbapi.connection.asyncio.run_coroutine_threadsafe",
                return_value=fake_future,
            ),
            pytest.raises(KeyboardInterrupt),
        ):
            conn._run_sync(coro)
    finally:
        coro.close()

    # Wedge-cleanup branch: _async_conn IS nulled.
    assert conn._async_conn is None, "wedge cleanup should null _async_conn"
    # _invalidate WAS scheduled.
    assert invalidate_calls, "wedge cleanup should schedule _invalidate"
