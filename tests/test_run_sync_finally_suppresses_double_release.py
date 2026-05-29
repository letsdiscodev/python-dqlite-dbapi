"""Pin: ``_run_sync``'s outer ``finally`` tolerates the same-thread close() bypass
having already released ``_op_lock`` — a double release() would replace the in-flight
KeyboardInterrupt with RuntimeError("release unlocked lock"), violating the PEP 249 surface.
"""

from __future__ import annotations

import threading
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


def test_run_sync_outer_finally_tolerates_already_released_op_lock() -> None:
    """Lock released out-of-band on the same thread before the outer finally must not raise."""
    conn = _make_with_loop_thread()
    try:

        def _bypass_release_then_raise_ki(*_args: object, **_kwargs: object) -> None:
            # Mirror the close() bypass: null the owner slot and release from the same thread.
            assert conn._op_lock_owner == threading.get_ident()
            conn._op_lock_owner = None
            conn._op_lock.release()
            raise KeyboardInterrupt(
                "synthetic signal landing after close() bypass released the lock"
            )

        with (
            patch(
                "concurrent.futures.Future.result",
                side_effect=_bypass_release_then_raise_ki,
            ),
            pytest.raises(KeyboardInterrupt),
        ):
            conn.commit()
    finally:
        conn._closed = True


def test_run_sync_outer_finally_no_runtimeerror_replaces_keyboard_interrupt() -> None:
    """Stricter form: the propagated exception is exactly KeyboardInterrupt, not RuntimeError."""
    conn = _make_with_loop_thread()
    try:

        def _bypass_release_then_raise_ki(*_args: object, **_kwargs: object) -> None:
            assert conn._op_lock_owner == threading.get_ident()
            conn._op_lock_owner = None
            conn._op_lock.release()
            raise KeyboardInterrupt("synthetic signal post-bypass")

        with patch(
            "concurrent.futures.Future.result",
            side_effect=_bypass_release_then_raise_ki,
        ):
            try:
                conn.commit()
            except BaseException as e:
                propagated: BaseException = e
            else:
                raise AssertionError("expected an exception to propagate")
        assert isinstance(propagated, KeyboardInterrupt), (
            f"outer-finally replaced the in-flight KeyboardInterrupt with "
            f"{type(propagated).__name__}: {propagated!r}. The outer-finally "
            f"release must be wrapped in contextlib.suppress(RuntimeError) so "
            f"the signal propagates cleanly to the PEP 249 caller."
        )
    finally:
        conn._closed = True
