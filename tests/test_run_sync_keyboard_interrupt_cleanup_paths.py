"""Pin: ``_run_sync``'s KI / SystemExit cleanup paths defend against their own cleanup
failing — the never-scheduled coro is closed, and a closed-loop RuntimeError from
``call_soon_threadsafe`` is suppressed so it does not mask the KI."""

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
    """The never-scheduled coroutine must be closed so its frame is freed (cr_frame None)."""
    conn = _make_with_loop_thread()
    try:
        captured = {}

        async def stub_coro() -> None:
            return None

        coro = stub_coro()
        captured["coro"] = coro

        fake_lock = MagicMock()
        fake_lock.acquire.side_effect = KeyboardInterrupt
        conn._op_lock = fake_lock

        with pytest.raises(KeyboardInterrupt):
            conn._run_sync(coro)

        assert getattr(coro, "cr_frame", None) is None, (
            "expected coro.close() to have freed the coroutine frame; cr_frame is still set"
        )
    finally:
        conn._closed = True


def test_op_lock_acquire_returns_false_closes_unscheduled_coroutine() -> None:
    """When ``_op_lock.acquire`` returns False, the coro is closed before OperationalError."""
    from dqlitedbapi.exceptions import OperationalError

    conn = _make_with_loop_thread()
    try:

        async def stub_coro() -> None:
            return None

        coro = stub_coro()

        fake_lock = MagicMock()
        fake_lock.acquire.return_value = False
        conn._op_lock = fake_lock

        with pytest.raises(OperationalError, match="op_lock acquire timed out"):
            conn._run_sync(coro)

        assert getattr(coro, "cr_frame", None) is None, (
            "expected coro.close() to have freed the coroutine frame "
            "on the lock-not-acquired branch"
        )
    finally:
        conn._closed = True


def _patch_call_soon_threadsafe_for_invalidate(loop: Any) -> Any:
    """Make scheduling ``_invalidate`` raise RuntimeError; other call_soon_threadsafe uses pass."""
    original = loop.call_soon_threadsafe

    def conditional(callback: Any, *args: Any, **kwargs: Any) -> Any:
        cb_name = getattr(callback, "__name__", "") or ""
        if "_invalidate" in cb_name or "_invalidate" in repr(callback):
            raise RuntimeError("Event loop is closed")
        return original(callback, *args, **kwargs)

    return patch.object(loop, "call_soon_threadsafe", side_effect=conditional)


def test_keyboard_interrupt_propagates_when_call_soon_threadsafe_raises_runtime_error() -> None:
    """A "loop closed" RuntimeError from the post-result call_soon_threadsafe must be
    suppressed so the KI still reaches the caller."""
    conn = _make_with_loop_thread()
    try:
        conn._async_conn._in_use = True  # type: ignore[union-attr]  # make cleanup branch fire

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
    """Same contract on the op-lock-acquire KI arm: a call_soon_threadsafe RuntimeError
    must not mask the KI."""
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
