"""Sync close() narrows its suppression so an op_lock-acquire-timeout OperationalError
surfaces (matching the async sibling's diagnostic) while transport-class faults are still
swallowed. Discipline pin: no natural reproduction today, so patch _run_sync to raise.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.exceptions import OperationalError


def _make_with_loop_thread() -> Connection:
    """Connection with a started loop and a faked _async_conn so close() reaches _run_sync."""
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
    """An OperationalError raised by _run_sync mid-close must propagate to the caller."""
    conn = _make_with_loop_thread()
    sentinel = OperationalError(
        "op_lock acquire timed out after 0.5s waiting for another operation "
        "(synthesised for test pin)"
    )

    with (
        patch.object(conn, "_run_sync", side_effect=sentinel),
        pytest.raises(OperationalError, match="op_lock acquire timed out"),
    ):
        conn.close()


def test_close_still_swallows_transport_class_exceptions() -> None:
    """A transport-class Exception during the async close drain is still swallowed —
    close() is best-effort and loop teardown reaps the transport regardless."""
    conn = _make_with_loop_thread()

    with patch.object(conn, "_run_sync", side_effect=OSError("write failed")):
        conn.close()
    assert conn._closed
