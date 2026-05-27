"""Pin: ``Connection._run_sync``'s outer ``finally`` arm tolerates
the case where the same-thread ``close()`` bypass has already
released ``_op_lock``.

The bypass branch at ``Connection.close()`` (around line 2777)
runs when a signal handler invokes ``conn.close()`` while another
sync call is parked in ``Future.result(timeout=...)``. The bypass
nulls ``_op_lock_owner`` and releases the lock so the loop-thread
teardown does not deadlock waiting for the in-flight ``_run_sync``
to release.

When control returns to ``_run_sync`` (KI / SystemExit raised at
the interrupted ``Future.result``), the outer ``finally`` tries
to release the lock a second time. ``threading.Lock.release()``
on an unlocked lock raises ``RuntimeError("release unlocked
lock")``. Python's try/finally semantics REPLACE the propagating
``KeyboardInterrupt`` with that ``RuntimeError`` — a PEP 249 §7
surface violation: callers expect ``KeyboardInterrupt`` or a
``dbapi.Error`` subclass, never a raw ``RuntimeError`` from the
lock primitive.

The two sibling release sites on the same lock object —
acquire-time KI cleanup and the close() bypass itself — already
wrap their releases in ``contextlib.suppress(RuntimeError)`` for
the analogous "already released" case. The outer finally is the
last missing site.
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
    """Simulate the signal-handler bypass: the lock is released
    out-of-band on the same thread before the outer ``finally``
    runs. The finally must NOT raise ``RuntimeError("release
    unlocked lock")`` — that raise would replace the in-flight
    ``KeyboardInterrupt`` per Python's try/finally contract.
    """
    conn = _make_with_loop_thread()
    try:

        def _bypass_release_then_raise_ki(*_args: object, **_kwargs: object) -> None:
            # Mirror the close() bypass: null the owner slot and
            # release the lock from the same thread that is parked
            # inside ``Future.result``.
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
    """Stricter form of the above: assert the propagated exception
    is *exactly* ``KeyboardInterrupt`` and not the wrong-layer
    ``RuntimeError("release unlocked lock")``.

    Python's try/finally exception-replacement rule means that
    pre-fix, the outer finally's raw ``self._op_lock.release()``
    on the already-released lock raises ``RuntimeError``, and the
    runtime swallows the in-flight ``KeyboardInterrupt`` (it
    survives on ``__context__`` only). Pin the post-fix invariant.
    """
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
