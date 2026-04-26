"""Pin: ``_run_sync`` cancels the future and invalidates the connection
when the calling thread receives ``KeyboardInterrupt`` (or
``SystemExit``) during ``Future.result``.

A long-running ``commit()`` / ``rollback()`` / ``execute()`` on a sync
``Connection`` blocks the calling thread on
``concurrent.futures.Future.result(timeout=...)``. If the user
``Ctrl-C``s while blocked, the calling thread receives
``KeyboardInterrupt``. Without explicit cleanup, the coroutine on the
background event-loop thread keeps running, eventually completes, and
the next sync call on the same connection races the residual
operation. Pin the cleanup contract: the future is cancelled, the
underlying async connection is invalidated, and the caller sees the
KI propagate.

Use a mocked ``Future.result`` so the test is deterministic without
relying on signal-delivery timing.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.exceptions import InterfaceError


def _make_with_loop_thread() -> Connection:
    """Build a sync ``Connection`` whose loop thread is up and whose
    underlying async connection is a mock — bypassing the real
    handshake."""
    conn = Connection("localhost:9001")
    # Spin up the event-loop thread by reading the lazy property.
    conn._ensure_loop()
    # Drop in a mock client connection so methods that touch
    # ``_async_conn`` don't try to handshake.
    fake = MagicMock()
    fake.execute = AsyncMock(return_value=(0, 0))
    fake.close = AsyncMock()
    fake._invalidate = MagicMock()
    fake._in_use = False
    fake._bound_loop = None
    conn._async_conn = fake
    return conn


def test_run_sync_propagates_keyboard_interrupt() -> None:
    conn = _make_with_loop_thread()
    try:
        with (
            patch(
                "concurrent.futures.Future.result",
                side_effect=KeyboardInterrupt,
            ),
            pytest.raises(KeyboardInterrupt),
        ):
            conn.commit()
    finally:
        conn._closed = True


def test_run_sync_keyboard_interrupt_invalidates_underlying_connection() -> None:
    """The new BaseException arm must schedule _invalidate on the loop
    thread so the wire stream is poisoned and the next sync call sees
    a clean PEP 249 error instead of "another operation is in progress".
    """
    conn = _make_with_loop_thread()
    try:
        invalidate_calls: list[Exception] = []
        original_invalidate = conn._async_conn._invalidate

        def capture_invalidate(*args: object, **kwargs: object) -> None:
            if args:
                invalidate_calls.append(args[0])  # type: ignore[arg-type]
            original_invalidate(*args, **kwargs)

        conn._async_conn._invalidate = capture_invalidate  # type: ignore[union-attr]

        with (
            patch(
                "concurrent.futures.Future.result",
                side_effect=KeyboardInterrupt,
            ),
            pytest.raises(KeyboardInterrupt),
        ):
            conn.commit()

        # Give the call_soon_threadsafe-scheduled invalidate a moment
        # to run on the loop thread.
        import time

        for _ in range(50):
            if invalidate_calls:
                break
            time.sleep(0.01)
        assert invalidate_calls, "expected _invalidate to be scheduled"
        assert isinstance(invalidate_calls[0], InterfaceError)
        assert "interrupted" in str(invalidate_calls[0]).lower()
    finally:
        conn._closed = True


def test_run_sync_propagates_system_exit() -> None:
    conn = _make_with_loop_thread()
    try:
        with (
            patch(
                "concurrent.futures.Future.result",
                side_effect=SystemExit,
            ),
            pytest.raises(SystemExit),
        ):
            conn.rollback()
    finally:
        conn._closed = True


def test_run_sync_after_keyboard_interrupt_keeps_connection_usable_or_raises_clean_error() -> None:
    """After a KI mid-call, the connection should either be invalidated
    (subsequent calls raise a clean PEP 249 error) or remain usable.
    What it must NOT do is silently corrupt internal state — pin that
    the next call doesn't hang or raise something unrelated to the
    PEP 249 hierarchy."""
    conn = _make_with_loop_thread()
    try:
        # First call: KI mid-Future.result.
        with (
            patch(
                "concurrent.futures.Future.result",
                side_effect=KeyboardInterrupt,
            ),
            pytest.raises(KeyboardInterrupt),
        ):
            conn.commit()

        # Second call: future.result is no longer patched. The
        # connection's internal state should be coherent — either it
        # raises a clean PEP 249 error indicating invalidation, or it
        # succeeds. It must NOT raise something outside the PEP 249
        # hierarchy or hang.
        try:
            conn.commit()
        except InterfaceError:
            # Acceptable: invalidation surfaced as InterfaceError.
            pass
        except Exception as exc:
            # Any other PEP 249 Error subclass is also acceptable.
            from dqlitedbapi.exceptions import Error as DbapiError

            assert isinstance(exc, DbapiError), (
                f"unexpected non-PEP-249 exception class after KI: {type(exc).__name__}"
            )
    finally:
        conn._closed = True
