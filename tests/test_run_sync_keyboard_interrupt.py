"""Pin: ``_run_sync`` cancels the future and invalidates the connection on
KeyboardInterrupt / SystemExit during ``Future.result``, then propagates the signal."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.exceptions import InterfaceError


def _make_with_loop_thread() -> Connection:
    """Sync Connection with the loop thread up and a mock async connection (no handshake)."""
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
    """The BaseException arm must schedule _invalidate so the next call sees a clean error."""
    conn = _make_with_loop_thread()
    try:
        invalidate_calls: list[Exception] = []
        original_invalidate = conn._async_conn._invalidate  # type: ignore[union-attr]

        def capture_invalidate(*args: object, **kwargs: object) -> None:
            if args:
                invalidate_calls.append(args[0])  # type: ignore[arg-type]
            original_invalidate(*args, **kwargs)  # type: ignore[arg-type]

        conn._async_conn._invalidate = capture_invalidate  # type: ignore[union-attr]

        with (
            patch(
                "concurrent.futures.Future.result",
                side_effect=KeyboardInterrupt,
            ),
            pytest.raises(KeyboardInterrupt),
        ):
            conn.commit()

        # Let the call_soon_threadsafe-scheduled invalidate run on the loop thread.
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


def test_run_sync_keyboard_interrupt_synchronously_nulls_async_conn() -> None:
    """The KI arm must null ``self._async_conn`` synchronously so the next op reconnects,
    rather than wedging on a stale ``_in_use=True`` until the loop read deadline fires."""
    conn = _make_with_loop_thread()
    try:
        before_inner = conn._async_conn
        assert before_inner is not None

        with (
            patch(
                "concurrent.futures.Future.result",
                side_effect=KeyboardInterrupt,
            ),
            pytest.raises(KeyboardInterrupt),
        ):
            conn.commit()

        assert conn._async_conn is None, (
            "KI arm must synchronously null self._async_conn so the "
            "next sync op gets a fresh-connect path; saw conn._async_conn "
            f"= {conn._async_conn!r}"
        )
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
    """After a KI mid-call, the next call must not hang or escape the PEP 249 hierarchy."""
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

        # Second call (unpatched): must succeed or raise a PEP 249 Error, never hang.
        try:
            conn.commit()
        except InterfaceError:
            pass
        except Exception as exc:
            from dqlitedbapi.exceptions import Error as DbapiError

            assert isinstance(exc, DbapiError), (
                f"unexpected non-PEP-249 exception class after KI: {type(exc).__name__}"
            )
    finally:
        conn._closed = True


def test_keyboard_interrupt_during_op_lock_acquire_invalidates_when_prior_op_in_flight() -> None:
    """KI escaping ``Lock.acquire`` (SIGINT-interruptible) with a prior op in-flight
    (``_in_use=True``) must schedule a defensive invalidation, else the conn wedges forever."""
    conn = _make_with_loop_thread()
    try:
        invalidate_calls: list[Exception] = []

        def capture_invalidate(*args: object, **kwargs: object) -> None:
            if args:
                invalidate_calls.append(args[0])  # type: ignore[arg-type]

        conn._async_conn._invalidate = capture_invalidate  # type: ignore[method-assign,union-attr,unused-ignore]
        conn._async_conn._in_use = True  # type: ignore[union-attr]

        # Lock objects are immutable C types — patch the instance attribute with a fake.
        fake_lock = MagicMock()
        fake_lock.acquire.side_effect = KeyboardInterrupt
        conn._op_lock = fake_lock

        with pytest.raises(KeyboardInterrupt):
            conn.commit()

        import time

        for _ in range(50):
            if invalidate_calls:
                break
            time.sleep(0.01)
        assert invalidate_calls, "expected _invalidate to be scheduled"
        assert isinstance(invalidate_calls[0], InterfaceError)
        assert "op-lock acquire" in str(invalidate_calls[0]).lower()
        # KI arm best-effort releases the lock under suppress(RuntimeError) to cover the
        # narrow gap where acquire returned True but KI landed before STORE_FAST.
        assert fake_lock.release.call_count == 1
    finally:
        conn._closed = True


def test_keyboard_interrupt_during_op_lock_acquire_nulls_async_conn_synchronously() -> None:
    """Pre-acquire KI arm must null ``self._async_conn`` synchronously like the post-acquire
    arm, else a signal-handler retry wedges on stale ``_in_use=True`` until the read deadline."""
    conn = _make_with_loop_thread()
    try:
        invalidate_calls: list[Exception] = []

        def capture_invalidate(*args: object, **kwargs: object) -> None:
            if args:
                invalidate_calls.append(args[0])  # type: ignore[arg-type]

        conn._async_conn._invalidate = capture_invalidate  # type: ignore[method-assign,union-attr,unused-ignore]
        conn._async_conn._in_use = True  # type: ignore[union-attr]

        fake_lock = MagicMock()
        fake_lock.acquire.side_effect = KeyboardInterrupt
        conn._op_lock = fake_lock

        with pytest.raises(KeyboardInterrupt):
            conn.commit()

        assert conn._async_conn is None, (
            "Pre-acquire KI arm must null self._async_conn "
            "synchronously — otherwise a retry from the signal "
            "handler wedges on stale _in_use=True until the loop "
            "coroutine yields."
        )

        # Invalidate is still scheduled so the wedged loop coroutine's wire stream gets reaped.
        import time

        for _ in range(50):
            if invalidate_calls:
                break
            time.sleep(0.01)
        assert invalidate_calls
        assert isinstance(invalidate_calls[0], InterfaceError)
    finally:
        conn._closed = True


def test_keyboard_interrupt_during_op_lock_acquire_no_op_when_idle() -> None:
    """Negative pin: KI during a quiet acquire (no prior op) must NOT schedule invalidation."""
    conn = _make_with_loop_thread()
    try:
        invalidate_calls: list[Exception] = []

        def capture_invalidate(*args: object, **kwargs: object) -> None:
            if args:
                invalidate_calls.append(args[0])  # type: ignore[arg-type]

        conn._async_conn._invalidate = capture_invalidate  # type: ignore[method-assign,union-attr,unused-ignore]
        conn._async_conn._in_use = False  # type: ignore[union-attr]

        fake_lock = MagicMock()
        fake_lock.acquire.side_effect = KeyboardInterrupt
        conn._op_lock = fake_lock

        with pytest.raises(KeyboardInterrupt):
            conn.commit()

        import time

        for _ in range(20):
            time.sleep(0.01)
        assert invalidate_calls == [], "expected NO _invalidate when prior op was not in flight"
    finally:
        conn._closed = True


def test_system_exit_during_op_lock_acquire_invalidates_when_prior_op_in_flight() -> None:
    """SystemExit takes the same path as KeyboardInterrupt."""
    conn = _make_with_loop_thread()
    try:
        invalidate_calls: list[Exception] = []

        def capture_invalidate(*args: object, **kwargs: object) -> None:
            if args:
                invalidate_calls.append(args[0])  # type: ignore[arg-type]

        conn._async_conn._invalidate = capture_invalidate  # type: ignore[method-assign,union-attr,unused-ignore]
        conn._async_conn._in_use = True  # type: ignore[union-attr]

        fake_lock = MagicMock()
        fake_lock.acquire.side_effect = SystemExit
        conn._op_lock = fake_lock

        with pytest.raises(SystemExit):
            conn.commit()

        import time

        for _ in range(50):
            if invalidate_calls:
                break
            time.sleep(0.01)
        assert invalidate_calls, "expected _invalidate to be scheduled"
        assert isinstance(invalidate_calls[0], InterfaceError)
    finally:
        conn._closed = True
