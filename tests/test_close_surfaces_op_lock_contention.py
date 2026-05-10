"""Pin: sync ``Connection.close()`` does NOT swallow op_lock-acquire-timeout.

The async sibling at ``aio/connection.py`` (close path) raises a
diagnostic when a sibling task holds the op_lock past the close
timeout — operators see a clear signal that the connection was
force-closed under contention.

The sync sibling previously wrapped the close-time ``_run_sync`` in
``contextlib.suppress(Exception)`` which silently swallowed the
``OperationalError("op_lock acquire timed out ...")`` raised by
``_run_sync`` under contention. Same operational signal, missing
on the sync surface.

Narrowing the suppression so ``OperationalError`` (the op_lock-
acquire-timeout) surfaces while genuine transport-class faults
during close are still swallowed (close() is best-effort and the
connection IS closed regardless of what the async drain reports).

In the production code paths there is no easy natural reproduction:

- ``close()`` from the creator thread mid-execute would deliberately
  short-circuit the bounded acquire (the SIGTERM-handler-friendly
  path) and never reach the suppression site.
- ``close()`` from a different thread is rejected up front by
  ``_check_thread``, before any op_lock contention can arise.

The narrowing is therefore primarily a discipline pin: future
refactors (e.g., a relaxed ``_check_thread`` for SA pool finalisation
or a SIGTERM-without-creator-thread shape) would expose the path.
Pin the discipline by patching ``_run_sync`` to raise a synthetic
``OperationalError`` and verifying it propagates past the close-
suppression block.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.exceptions import OperationalError


def _make_with_loop_thread() -> Connection:
    """Connection with a started loop and a faked ``_async_conn`` so
    the close path actually reaches the ``_run_sync`` site we want
    to exercise. The loop and inner-connection construction here
    mirrors the helper in
    ``tests/test_run_sync_keyboard_interrupt_cleanup_paths.py``."""
    conn = Connection("localhost:9001", timeout=0.5)
    conn._ensure_loop()
    fake = MagicMock()
    fake.execute = AsyncMock(return_value=(0, 0))
    fake.close = AsyncMock()
    fake._invalidate = MagicMock()
    fake._in_use = False
    fake._bound_loop = None
    conn._async_conn = fake
    return conn


def test_close_does_not_swallow_op_lock_acquire_timeout() -> None:
    """An ``OperationalError`` raised by ``_run_sync`` mid-close must
    propagate to the caller. Genuine transport-class ``Exception``s
    during close are still swallowed (best-effort)."""
    conn = _make_with_loop_thread()
    sentinel = OperationalError(
        "op_lock acquire timed out after 0.5s waiting for another operation "
        "(synthesised for test pin)"
    )

    # Patch _run_sync to raise our sentinel as if it had hit the
    # bounded-acquire timeout. The close() path's narrow handler must
    # let this propagate; a bare ``contextlib.suppress(Exception)``
    # would swallow it silently.
    with (
        patch.object(conn, "_run_sync", side_effect=sentinel),
        pytest.raises(OperationalError, match="op_lock acquire timed out"),
    ):
        conn.close()


def test_close_still_swallows_transport_class_exceptions() -> None:
    """A genuine transport-class ``Exception`` during the async close
    drain must still be swallowed — close() is best-effort and the
    loop-teardown ``finally`` block still reaps the underlying
    transport regardless. Without this, a transient OSError mid-close
    would propagate as a noisy stack trace from every ``conn.close()``
    in error-handling paths."""
    conn = _make_with_loop_thread()

    with patch.object(conn, "_run_sync", side_effect=OSError("write failed")):
        # No raise — the OSError is swallowed.
        conn.close()
    assert conn._closed
