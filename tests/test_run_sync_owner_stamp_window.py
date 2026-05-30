"""Pin: a KeyboardInterrupt/SystemExit raised in the op-lock owner-stamp gap
must not leak ``_op_lock``.

The owner-stamp (``self._op_lock_owner = threading.get_ident()``) runs on the
acquired arm of ``_run_sync``. If a signal lands there while the lock is held,
the release path must still run; otherwise the lock is held by no thread and
every later ``_run_sync`` wedges on the bounded acquire.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dqlitedbapi import Connection


def _make_with_loop_thread() -> Connection:
    """Sync Connection with the loop thread up and a mock async connection."""
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


async def _noop() -> None:
    return None


def _ident_raising_in_stamp_gap(
    conn: Connection, exc: type[BaseException], state: dict[str, bool]
) -> Callable[[], int]:
    """A ``threading.get_ident`` replacement that raises once, only while
    ``_op_lock`` is held — i.e. at the owner-stamp, never at ``_check_thread``
    or ``close`` (which also call ``get_ident`` but with the lock unheld)."""
    real = threading.get_ident

    def fake() -> int:
        if not state["fired"] and conn._op_lock.locked():
            state["fired"] = True
            raise exc
        return real()

    return fake


@pytest.mark.parametrize("exc", [KeyboardInterrupt, SystemExit])
def test_run_sync_signal_in_owner_stamp_gap_releases_op_lock(
    exc: type[BaseException],
) -> None:
    conn = _make_with_loop_thread()
    coro = _noop()
    try:
        state = {"fired": False}
        with (
            patch(
                "dqlitedbapi.connection.threading.get_ident",
                side_effect=_ident_raising_in_stamp_gap(conn, exc, state),
            ),
            pytest.raises(exc),
        ):
            conn._run_sync(coro)

        assert state["fired"], "test did not exercise the owner-stamp gap"
        # The signal escaped before the body try was entered, but the lock was
        # held — it must be released, else the connection wedges for life.
        assert not conn._op_lock.locked()
        assert conn._op_lock.acquire(timeout=0) is True
        conn._op_lock.release()
        assert conn._op_lock_owner is None
    finally:
        coro.close()
        conn._closed = True
