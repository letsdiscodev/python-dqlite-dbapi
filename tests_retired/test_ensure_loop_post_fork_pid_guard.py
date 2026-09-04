"""Defence-in-depth: ``_ensure_loop`` rejects forked-child callers even when the
perimeter ``_check_thread`` was bypassed (e.g. a caller reaching ``_run_sync`` directly).

Without this, a forked child enqueues against the inherited parent loop that nobody
drains and hangs on ``Future.result(timeout=...)`` instead of raising InterfaceError.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import InterfaceError


def _patched():
    """Mock context so a Connection can build its loop without touching the wire."""
    return (
        patch(
            "dqlitedbapi.connection._resolve_leader",
            new=AsyncMock(side_effect=lambda address, **_kw: address),
        ),
        patch("dqlitedbapi.connection.DqliteConnection"),
    )


class TestEnsureLoopPostForkGuard:
    def test_ensure_loop_raises_interface_error_in_simulated_fork_child(self) -> None:
        """``_ensure_loop`` raises ``InterfaceError`` when the current pid differs from
        the recorded creator pid, even when ``_check_thread`` was bypassed."""
        conn = Connection("localhost:19001", timeout=10.0, close_timeout=0.5)
        try:
            # Spin the loop up in the simulated parent so the is_closed() early-return
            # arm would otherwise hand back the now parent-owned loop.
            resolve_patch, dqlite_patch = _patched()
            with resolve_patch, dqlite_patch as MockDqliteConn:
                instance = AsyncMock()
                instance.connect = AsyncMock()
                instance._protocol = AsyncMock()
                instance._in_use = False
                instance._pending_drain = None
                MockDqliteConn.return_value = instance
                conn._run_sync(conn._get_async_connection())

            # Simulate fork: bump the pid so it no longer matches _creator_pid.
            with (
                patch(
                    "dqlitedbapi.connection.get_current_pid",
                    return_value=conn._creator_pid + 1,
                ),
                pytest.raises(InterfaceError, match="Connection used after fork"),
            ):
                conn._ensure_loop()
        finally:
            conn.close()

    def test_ensure_loop_returns_loop_in_creator_process(self) -> None:
        """Negative pin: in the creator process ``_ensure_loop`` returns the live loop."""
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
        """The message must match the perimeter ``_check_thread`` wording so operators
        see one fork-related string regardless of which entry point caught it."""
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
