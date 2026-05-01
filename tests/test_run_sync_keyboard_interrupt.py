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


def test_run_sync_keyboard_interrupt_synchronously_nulls_async_conn() -> None:
    """The KI arm must null ``self._async_conn`` synchronously on
    the calling thread (a single GIL-atomic STORE_ATTR) so the next
    sync op sees a fresh-connect path regardless of whether the
    loop-thread coroutine has drained yet.

    Without this, a slow loop-thread read can keep ``_in_use=True``
    until the read deadline fires, wedging the next sync op with
    "another operation is in progress" for up to ``self._timeout``
    seconds.
    """
    conn = _make_with_loop_thread()
    try:
        # Capture the inner conn before KI lands.
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

        # The synchronous null-out happens on the calling thread
        # before KI is re-raised — observable immediately upon return
        # without any wait for the loop to drain.
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


def test_keyboard_interrupt_during_op_lock_acquire_invalidates_when_prior_op_in_flight() -> None:
    """``threading.Lock.acquire(timeout=...)`` is interruptible by SIGINT
    on CPython. A KI raised by the signal handler escapes ``acquire``
    BEFORE the in-block KI cleanup arm runs. If a prior in-flight call
    is wedged on the loop thread (``_in_use=True``), the connection is
    stuck for life unless we schedule an invalidation defensively.

    Pin the gated path: prior op in-flight → invalidation scheduled.
    """
    conn = _make_with_loop_thread()
    try:
        invalidate_calls: list[Exception] = []

        def capture_invalidate(*args: object, **kwargs: object) -> None:
            if args:
                invalidate_calls.append(args[0])  # type: ignore[arg-type]

        conn._async_conn._invalidate = capture_invalidate  # type: ignore[method-assign,union-attr,unused-ignore]
        # Simulate the prior-op-still-in-flight precondition.
        conn._async_conn._in_use = True  # type: ignore[union-attr]

        # Lock objects are immutable C types — patch the instance
        # attribute directly with a fake lock whose .acquire raises.
        fake_lock = MagicMock()
        fake_lock.acquire.side_effect = KeyboardInterrupt
        conn._op_lock = fake_lock

        with pytest.raises(KeyboardInterrupt):
            conn.commit()

        # Wait for the call_soon_threadsafe-scheduled invalidate to run.
        import time

        for _ in range(50):
            if invalidate_calls:
                break
            time.sleep(0.01)
        assert invalidate_calls, "expected _invalidate to be scheduled"
        assert isinstance(invalidate_calls[0], InterfaceError)
        assert "op-lock acquire" in str(invalidate_calls[0]).lower()
        # The KI cleanup arm now best-effort releases the lock under
        # ``contextlib.suppress(RuntimeError)`` to defend against the
        # bytecode-narrow gap where ``acquire`` returned True but the
        # KI landed before STORE_FAST. RuntimeError on an unlocked
        # ``threading.Lock`` is suppressed; the call is safe in both
        # the gap and the more-common "KI before acquire returned"
        # case. The mock's release just records the call without
        # raising, so we can pin its presence here.
        assert fake_lock.release.call_count == 1
    finally:
        conn._closed = True


def test_keyboard_interrupt_during_op_lock_acquire_nulls_async_conn_synchronously() -> None:
    """Pre-acquire KI arm must mirror the post-acquire arm's
    synchronous ``self._async_conn = None`` discipline so the
    next sync op gets a fresh-connect path even if the loop has
    not yet drained the scheduled ``_invalidate``.

    Without this, a retry from the signal handler reads a stale
    non-None ``self._async_conn`` whose ``_in_use=True`` is still
    latched (the loop's slow ``reader.read()`` has not yielded);
    the retry's ``_get_async_connection`` returns the dying conn,
    ``_check_in_use`` fires, and the call wedges with "another
    operation is in progress" until the read deadline.
    """
    conn = _make_with_loop_thread()
    try:
        # Capture invalidate so we can assert it was scheduled.
        invalidate_calls: list[Exception] = []

        def capture_invalidate(*args: object, **kwargs: object) -> None:
            if args:
                invalidate_calls.append(args[0])  # type: ignore[arg-type]

        conn._async_conn._invalidate = capture_invalidate  # type: ignore[method-assign,union-attr,unused-ignore]
        # Simulate prior-op-in-flight precondition (matches the
        # gating in the production code).
        conn._async_conn._in_use = True  # type: ignore[union-attr]

        fake_lock = MagicMock()
        fake_lock.acquire.side_effect = KeyboardInterrupt
        conn._op_lock = fake_lock

        with pytest.raises(KeyboardInterrupt):
            conn.commit()

        # Load-bearing assertion: synchronous null-out before the
        # KI propagates, mirroring the post-acquire arm's
        # ISSUE-785 discipline.
        assert conn._async_conn is None, (
            "Pre-acquire KI arm must null self._async_conn "
            "synchronously — otherwise a retry from the signal "
            "handler wedges on stale _in_use=True until the loop "
            "coroutine yields."
        )

        # And the invalidate was still scheduled (preserved
        # behavior — wedged loop coroutine needs the poison so
        # the wire stream gets reaped when it finally yields).
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
    """Negative pin: KI during a quiet acquire (no prior op wedged)
    must NOT schedule a gratuitous invalidation. Re-raise the KI
    cleanly; leave the connection usable for the next call."""
    conn = _make_with_loop_thread()
    try:
        invalidate_calls: list[Exception] = []

        def capture_invalidate(*args: object, **kwargs: object) -> None:
            if args:
                invalidate_calls.append(args[0])  # type: ignore[arg-type]

        conn._async_conn._invalidate = capture_invalidate  # type: ignore[method-assign,union-attr,unused-ignore]
        # Idle precondition: no prior op in flight.
        conn._async_conn._in_use = False  # type: ignore[union-attr]

        # Lock objects are immutable C types — patch the instance
        # attribute directly with a fake lock whose .acquire raises.
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
    """SystemExit takes the same path as KeyboardInterrupt — both are
    BaseException subclasses raised by signal handlers."""
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
