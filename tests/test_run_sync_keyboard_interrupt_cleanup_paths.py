"""Pin: ``_run_sync``'s KI / SystemExit cleanup paths defend against
their own cleanup failing.

Two test gaps in the existing KI suite:

1. ``coro.close()`` on the never-scheduled coroutine MUST run so the
   coroutine frame is freed and CPython does not emit
   "coroutine was never awaited" ResourceWarning. The op-lock-acquire
   KI tests pin invalidation but never inspect the coroutine itself.

2. ``call_soon_threadsafe`` is wrapped in ``contextlib.suppress(
   RuntimeError)`` so a closed loop (concurrent ``engine.dispose()``)
   does not mask the KI. Pin that contract: when the loop is closing
   and call_soon_threadsafe raises RuntimeError, the KI still
   propagates to the caller.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dqlitedbapi import Connection


def _make_with_loop_thread() -> Connection:
    conn = Connection("localhost:9001")
    conn._ensure_loop()
    fake = MagicMock()
    fake.execute = AsyncMock(return_value=(0, 0))
    fake.close = AsyncMock()
    fake._invalidate = MagicMock()
    fake._in_use = False
    fake._bound_loop = None
    conn._async_conn = fake
    return conn


def test_op_lock_acquire_keyboard_interrupt_closes_unscheduled_coroutine() -> None:
    """The ``coro.close()`` call on the never-scheduled coroutine must
    run so the coroutine frame is freed. Inspect ``cr_frame`` and
    ``cr_running`` to verify the close landed."""
    conn = _make_with_loop_thread()
    try:
        # Capture the coroutine that _commit_async builds before
        # _run_sync runs. Patch _run_sync so we can intercept the coro.
        captured = {}

        async def stub_coro() -> None:
            return None

        coro = stub_coro()
        captured["coro"] = coro

        # Force the op-lock acquire to KI.
        fake_lock = MagicMock()
        fake_lock.acquire.side_effect = KeyboardInterrupt
        conn._op_lock = fake_lock

        with pytest.raises(KeyboardInterrupt):
            conn._run_sync(coro)

        # ``cr_frame`` becomes None after the coroutine is closed.
        assert getattr(coro, "cr_frame", None) is None, (
            "expected coro.close() to have freed the coroutine frame; cr_frame is still set"
        )
    finally:
        conn._closed = True


def test_op_lock_acquire_returns_false_closes_unscheduled_coroutine() -> None:
    """When ``_op_lock.acquire`` returns False (lock held elsewhere),
    the never-scheduled coroutine must also be closed before raising
    InterfaceError."""
    from dqlitedbapi.exceptions import InterfaceError

    conn = _make_with_loop_thread()
    try:

        async def stub_coro() -> None:
            return None

        coro = stub_coro()

        fake_lock = MagicMock()
        fake_lock.acquire.return_value = False
        conn._op_lock = fake_lock

        with pytest.raises(InterfaceError, match="another operation is in progress"):
            conn._run_sync(coro)

        assert getattr(coro, "cr_frame", None) is None, (
            "expected coro.close() to have freed the coroutine frame "
            "on the lock-not-acquired branch"
        )
    finally:
        conn._closed = True


def _patch_call_soon_threadsafe_for_invalidate(loop: Any) -> Any:
    """Patch ``loop.call_soon_threadsafe`` so that scheduling
    ``_invalidate`` raises RuntimeError, while every other use
    (``asyncio.run_coroutine_threadsafe``, etc.) passes through.
    """
    original = loop.call_soon_threadsafe

    def conditional(callback: Any, *args: Any, **kwargs: Any) -> Any:
        cb_name = getattr(callback, "__name__", "") or ""
        if "_invalidate" in cb_name or "_invalidate" in repr(callback):
            raise RuntimeError("Event loop is closed")
        return original(callback, *args, **kwargs)

    return patch.object(loop, "call_soon_threadsafe", side_effect=conditional)


def test_keyboard_interrupt_propagates_when_call_soon_threadsafe_raises_runtime_error() -> None:
    """The ``contextlib.suppress(RuntimeError)`` around the post-result
    ``call_soon_threadsafe`` invocation must absorb a "loop closed"
    RuntimeError so the KI still reaches the caller. Without
    suppression, the RuntimeError would supplant the KI and the user's
    Ctrl-C would be silently swallowed."""
    conn = _make_with_loop_thread()
    try:
        # ``conn._async_conn._in_use = True`` so the cleanup branch fires.
        conn._async_conn._in_use = True  # type: ignore[union-attr]

        original_loop = conn._loop
        assert original_loop is not None
        with (
            _patch_call_soon_threadsafe_for_invalidate(original_loop),
            patch(
                "concurrent.futures.Future.result",
                side_effect=KeyboardInterrupt,
            ),
            pytest.raises(KeyboardInterrupt),
        ):
            conn.commit()
    finally:
        conn._closed = True


def test_op_lock_acquire_keyboard_interrupt_propagates_when_call_soon_threadsafe_raises() -> None:
    """Same contract on the op-lock-acquire KI arm: a RuntimeError from
    ``call_soon_threadsafe`` (loop closed mid-Ctrl-C) must not mask the
    KI."""
    conn = _make_with_loop_thread()
    try:
        conn._async_conn._in_use = True  # type: ignore[union-attr]

        original_loop = conn._loop
        assert original_loop is not None
        with _patch_call_soon_threadsafe_for_invalidate(original_loop):
            fake_lock = MagicMock()
            fake_lock.acquire.side_effect = KeyboardInterrupt
            conn._op_lock = fake_lock

            with pytest.raises(KeyboardInterrupt):
                conn.commit()
    finally:
        conn._closed = True
