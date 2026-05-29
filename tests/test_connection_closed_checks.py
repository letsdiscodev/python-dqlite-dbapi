"""Pin the closed-state and lifecycle defensive branches in ``connection.py`` and
``aio/connection.py`` so a regression can't silently turn a closed-check into a no-op."""

from __future__ import annotations

import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi import connect
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


def _prime_async_connection(address: str = "localhost:19001") -> AsyncConnection:
    """Build an AsyncConnection with a mocked inner conn and primed locks
    (close() asserts the locks are bound when ``_async_conn`` is set)."""
    conn = AsyncConnection(address, database="x")
    inner = MagicMock()
    inner.close = AsyncMock()
    inner.execute = AsyncMock()
    conn._async_conn = inner
    conn._ensure_locks()
    return conn


class TestAsyncEnsureConnectionClosedCheck:
    async def test_connect_after_close_raises_interface_error(self) -> None:
        conn = _prime_async_connection()
        await conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            await conn.connect()


class TestAsyncCloseIsIdempotent:
    async def test_double_close_short_circuits(self) -> None:
        conn = _prime_async_connection()
        await conn.close()
        await conn.close()


class TestAsyncCommitRollbackLockRecheckRace:
    """``commit()``/``rollback()`` re-check ``_closed`` AFTER acquiring ``_op_lock``,
    so a concurrent close() winning the race surfaces as ``InterfaceError``."""

    @staticmethod
    def _flipping_lock(target: AsyncConnection) -> asyncio.Lock:
        """An asyncio.Lock that flips target._closed=True after acquiring."""

        class _FlipLock(asyncio.Lock):
            async def acquire(self) -> bool:  # type: ignore[override]
                result = await super().acquire()
                target._closed = True
                return result

        return _FlipLock()

    async def test_commit_recheck_under_lock_raises(self) -> None:
        conn = _prime_async_connection()
        conn._ensure_locks()
        conn._op_lock = self._flipping_lock(conn)
        with pytest.raises(InterfaceError, match="Connection is closed"):
            await conn.commit()

    async def test_rollback_recheck_under_lock_raises(self) -> None:
        conn = _prime_async_connection()
        conn._ensure_locks()
        conn._op_lock = self._flipping_lock(conn)
        with pytest.raises(InterfaceError, match="Connection is closed"):
            await conn.rollback()


class TestCursorNoRunningLoopBranch:
    """Sync ``cursor()`` from a thread with no running loop, on a loop-bound
    connection, swallows the ``get_running_loop`` RuntimeError and still creates it."""

    async def test_cursor_from_no_loop_thread_succeeds_when_bound(self) -> None:
        conn = _prime_async_connection()
        conn._ensure_locks()

        result: dict[str, object] = {}

        def _runner() -> None:
            try:
                cur = conn.cursor()
                result["cursor"] = cur
            except BaseException as e:  # noqa: BLE001
                result["err"] = e

        t = threading.Thread(target=_runner)
        t.start()
        t.join()

        assert "err" not in result, f"unexpected error: {result.get('err')!r}"
        assert "cursor" in result


class TestAsyncAddressProperty:
    def test_address_returns_configured(self) -> None:
        conn = AsyncConnection("localhost:19001", database="x")
        assert conn.address == "localhost:19001"


class TestAsyncAexitNeverConnected:
    async def test_aexit_short_circuits_when_async_conn_none(self) -> None:
        """``__aexit__`` early-returns when never-connected; connection stays reusable."""
        conn = AsyncConnection("localhost:19001", database="x")
        await conn.__aexit__(None, None, None)
        assert conn._closed is False


class TestSyncConnectWrapsClusterPolicyRejection:
    """``_build_and_connect`` maps ``ClusterPolicyError`` to ``InterfaceError`` so SA's
    ``is_disconnect`` doesn't retry-loop on a permanent config mismatch."""

    def test_policy_rejection_surfaces_as_interface_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def _raise_policy(*args: object, **kwargs: object) -> None:
            raise _client_exc.ClusterPolicyError("not allowed")

        # Short-circuit leader discovery so connect() reaches the post-find-leader arm.
        async def _identity_resolve(address: str, *, timeout: float, **_kw: object) -> str:
            return address

        monkeypatch.setattr(
            "dqlitedbapi.connection._resolve_leader", _identity_resolve, raising=True
        )
        monkeypatch.setattr("dqliteclient.DqliteConnection.connect", _raise_policy, raising=True)

        conn = connect("localhost:19001", timeout=2.0)
        try:
            with pytest.raises(InterfaceError, match="Cluster policy rejection;"):
                conn.connect()
        finally:
            conn.close()


class TestSyncClosedChecks:
    def test_get_async_connection_after_close_raises(self) -> None:
        conn = connect("localhost:19001", timeout=2.0)
        conn.close()
        cur = conn
        with pytest.raises(InterfaceError, match="Connection is closed"):
            cur.cursor()

    def test_connect_after_close_raises(self) -> None:
        conn = connect("localhost:19001", timeout=2.0)
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn.connect()

    def test_commit_after_close_raises(self) -> None:
        conn = connect("localhost:19001", timeout=2.0)
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn.commit()

    def test_rollback_after_close_raises(self) -> None:
        conn = connect("localhost:19001", timeout=2.0)
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn.rollback()

    def test_commit_async_raises_interface_error_when_async_conn_none(self) -> None:
        """``_async_conn is None`` without ``_closed`` raises ``InterfaceError``, not
        ``AttributeError`` (the old ``assert`` was stripped under ``python -O``)."""
        import asyncio

        conn = connect("localhost:19001", timeout=2.0)
        conn._async_conn = None
        with pytest.raises(InterfaceError, match="closed"):
            asyncio.run(conn._commit_async())
        conn.close()

    def test_rollback_async_raises_interface_error_when_async_conn_none(self) -> None:
        import asyncio

        conn = connect("localhost:19001", timeout=2.0)
        conn._async_conn = None
        with pytest.raises(InterfaceError, match="closed"):
            asyncio.run(conn._rollback_async())
        conn.close()


class TestSyncAddressProperty:
    def test_address_returns_configured(self) -> None:
        conn = connect("localhost:19001", timeout=2.0)
        try:
            assert conn.address == "localhost:19001"
        finally:
            conn.close()


class TestSyncGetAsyncConnectionDirect:
    """Hit ``_get_async_connection``'s closed-check directly, bypassing sync wrappers."""

    async def test_get_async_connection_closed_check(self) -> None:
        conn = connect("localhost:19001", timeout=2.0)
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            await conn._get_async_connection()
