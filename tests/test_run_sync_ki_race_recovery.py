"""Pin: the KI / SystemExit arm has the timeout arm's race-recovery branch — if the future
completed successfully before cleanup, skip ``_invalidate`` so the connection stays reusable."""

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
    conn._async_conn = MagicMock()  # sentinel: must NOT be nulled on race-recovery
    conn._creator_pid = os.getpid()
    conn._op_lock = threading.RLock()  # type: ignore[assignment]
    return conn


def test_ki_arm_skips_invalidate_when_future_already_done() -> None:
    """When the future is already done (raced to success), skip _invalidate; keep _async_conn."""
    conn = _prime_connection()
    sentinel_conn = conn._async_conn

    fake_future = MagicMock(spec=concurrent.futures.Future)
    # First result() raises KI; second drains the concurrently-done future.
    fake_future.result = MagicMock(side_effect=[KeyboardInterrupt(), None])
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

    assert conn._async_conn is sentinel_conn, (
        "race-recovery should preserve _async_conn for the next call"
    )
    assert not invalidate_calls, (
        f"race-recovery should skip _invalidate scheduling; got: {invalidate_calls}"
    )


def test_ki_arm_invalidates_when_future_still_pending() -> None:
    """Positive control: future still pending when KI raises must invalidate as before."""
    conn = _prime_connection()

    fake_future = MagicMock(spec=concurrent.futures.Future)
    fake_future.result = MagicMock(
        side_effect=[
            KeyboardInterrupt(),
            concurrent.futures.TimeoutError(),  # bounded-wait absorbs this
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

    assert conn._async_conn is None, "wedge cleanup should null _async_conn"
    assert invalidate_calls, "wedge cleanup should schedule _invalidate"


# The drain ``future.result(timeout=0)`` uses an intentionally WIDE suppress(BaseException):
# the outer raise re-raises the user's KI, so any exception leaking from the drain would mask it.
import asyncio  # noqa: E402

import dqlitedbapi as _dbapi  # noqa: E402


@pytest.mark.parametrize(
    ("drain_exc", "exc_label"),
    [
        (_dbapi.OperationalError("coroutine resolved with wire error"), "Exception"),
        (asyncio.CancelledError("coroutine ran to cancellation"), "CancelledError"),
        (KeyboardInterrupt(), "KeyboardInterrupt"),
        (SystemExit(2), "SystemExit"),
    ],
    ids=["Exception", "CancelledError", "KeyboardInterrupt", "SystemExit"],
)
def test_ki_race_recovery_drain_absorbs_future_result_raise(
    drain_exc: BaseException,
    exc_label: str,
) -> None:
    """The drain must absorb any exception class (incl. BaseException) so the outer KI re-raise
    is not masked — a sweep flipping this to suppress(Exception) is the regression."""
    conn = _prime_connection()
    sentinel_conn = conn._async_conn

    fake_future = MagicMock(spec=concurrent.futures.Future)
    fake_future.result = MagicMock(side_effect=[KeyboardInterrupt(), drain_exc])
    fake_future.cancel = MagicMock()
    fake_future.done = MagicMock(return_value=True)
    fake_future.cancelled = MagicMock(return_value=False)

    fake_loop = MagicMock()
    fake_loop.call_soon_threadsafe = lambda cb, *args: None

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

    assert conn._async_conn is sentinel_conn, (
        f"{exc_label}: race-recovery should preserve _async_conn"
    )
