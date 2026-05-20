"""Defence-in-depth: ``Connection._ensure_loop`` rejects callers from a
forked child even when ``_check_thread`` was bypassed.

Every PUBLIC method routes through ``_check_thread`` before reaching
``_run_sync`` / ``_ensure_loop``, so in normal operation the perimeter
guard fires. The hazard surface is a third-party subclass / refactor /
new internal caller that forgets the perimeter check and calls
``_run_sync`` directly — in a forked child, the inherited
``self._loop`` references the parent's BaseEventLoop (``is_closed()``
returns False because the Python attribute is inherited) and the
inherited ``self._thread`` does NOT have a running OS thread (only
the calling thread crosses ``fork()``). Without a guard at
``_ensure_loop``, the call enqueues a coroutine against a loop nobody
drains and hangs on ``Future.result(timeout=...)`` for the per-RPC
budget before raising a generic ``TimeoutError`` instead of the
canonical ``InterfaceError("Connection used after fork ...")``.

The fix mirrors the project-wide discipline of layering fork guards at
the lowest sensible point (see ``_at_fork_replace_resolve_leader_cache_lock``
and the per-public-method ``_check_thread`` perimeter).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import InterfaceError


def _patched():
    """Construct the standard mock context so a Connection can build
    its loop without touching the wire."""
    return (
        patch(
            "dqlitedbapi.connection._resolve_leader",
            new=AsyncMock(side_effect=lambda address, **_kw: address),
        ),
        patch("dqlitedbapi.connection.DqliteConnection"),
    )


class TestEnsureLoopPostForkGuard:
    def test_ensure_loop_raises_interface_error_in_simulated_fork_child(self) -> None:
        """``_ensure_loop`` must raise ``InterfaceError`` (matching the
        ``_check_thread`` shape) when the current pid differs from the
        connection's recorded creator pid — even when the perimeter
        ``_check_thread`` was bypassed."""
        conn = Connection("localhost:19001", timeout=10.0, close_timeout=0.5)
        try:
            # Spin the loop up in the simulated parent process so the
            # ``self._loop is not None and not self._loop.is_closed()``
            # early-return arm would otherwise return the (now
            # parent-owned) loop.
            resolve_patch, dqlite_patch = _patched()
            with resolve_patch, dqlite_patch as MockDqliteConn:
                instance = AsyncMock()
                instance.connect = AsyncMock()
                instance._protocol = AsyncMock()
                instance._in_use = False
                instance._pending_drain = None
                MockDqliteConn.return_value = instance
                conn._run_sync(conn._get_async_connection())

            # Simulate fork: bump the pid so ``get_current_pid()`` no
            # longer matches the captured ``self._creator_pid``.
            with (
                patch(
                    "dqlitedbapi.connection.get_current_pid",
                    return_value=conn._creator_pid + 1,
                ),
                pytest.raises(InterfaceError, match="Connection used after fork"),
            ):
                conn._ensure_loop()
        finally:
            # close() is also pid-guarded; running it here returns
            # quietly because we are back in the real (parent) process.
            conn.close()

    def test_ensure_loop_returns_loop_in_creator_process(self) -> None:
        """Negative pin: in the creator process, the pid guard is
        transparent and ``_ensure_loop`` returns the live loop."""
        conn = Connection("localhost:19001", timeout=10.0, close_timeout=0.5)
        try:
            resolve_patch, dqlite_patch = _patched()
            with resolve_patch, dqlite_patch as MockDqliteConn:
                instance = AsyncMock()
                instance.connect = AsyncMock()
                instance._protocol = AsyncMock()
                instance._in_use = False
                instance._pending_drain = None
                MockDqliteConn.return_value = instance
                conn._run_sync(conn._get_async_connection())

            loop = conn._ensure_loop()
            assert loop is conn._loop
            assert loop is not None
            assert not loop.is_closed()
        finally:
            conn.close()

    def test_ensure_loop_guard_message_matches_check_thread(self) -> None:
        """The diagnostic shape must match the perimeter ``_check_thread``
        wording so operators see a single fork-related error string
        regardless of which entry point detected the violation."""
        conn = Connection("localhost:19001", timeout=10.0, close_timeout=0.5)
        try:
            resolve_patch, dqlite_patch = _patched()
            with resolve_patch, dqlite_patch as MockDqliteConn:
                instance = AsyncMock()
                instance.connect = AsyncMock()
                instance._protocol = AsyncMock()
                instance._in_use = False
                instance._pending_drain = None
                MockDqliteConn.return_value = instance
                conn._run_sync(conn._get_async_connection())

            with (
                patch(
                    "dqlitedbapi.connection.get_current_pid",
                    return_value=conn._creator_pid + 1,
                ),
                pytest.raises(InterfaceError) as exc_info,
            ):
                conn._ensure_loop()
            msg = str(exc_info.value)
            assert "Connection used after fork" in msg
            assert "reconstruct from configuration" in msg
            assert f"pid {conn._creator_pid}" in msg
        finally:
            conn.close()
