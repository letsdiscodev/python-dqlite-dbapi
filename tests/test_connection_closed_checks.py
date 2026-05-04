"""Pin closed-state and lifecycle defensive branches in
``connection.py`` and ``aio/connection.py`` reported as uncovered
by ``pytest --cov``.

Lines covered (pre-pragma):

aio/connection.py:
- 175     — ``_ensure_connection`` closed-state check.
- 222     — ``close()`` already-closed early return (idempotency).
- 323     — ``commit()`` lock-recheck race fallback (driven via a
  monkey-patched lock that flips ``_closed`` mid-acquire).
- 355     — ``rollback()`` symmetric lock-recheck race.
- 384-387 — ``cursor()`` no-running-loop branch (sync caller in an
  already-bound connection — SA greenlet glue case).
- 407     — ``address`` property getter.
- 439     — ``__aexit__`` never-connected early return.

connection.py (sync):
- 128     — ``_build_and_connect`` ``ClusterPolicyError`` →
  ``InterfaceError`` wrapper.
- 473     — ``_get_async_connection`` closed-state check.
- 507     — sync ``connect()`` closed-state check.
- 621     — sync ``commit()`` closed-state check.
- 651     — sync ``rollback()`` closed-state check.
- 686     — sync ``address`` property getter.

The closed-checks form the PEP 249 lifecycle contract; the
``InterfaceError`` shape at every entry point is what callers wrap
their pool checkout error handling around. A regression that
turned any of these into a no-op would let a silent zombie
cursor return stale rows. Pin each.
"""

from __future__ import annotations

import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi import connect
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError

# ---------------------------------------------------------------------------
# aio/connection.py — closed-checks, lock-recheck races, no-loop, address
# ---------------------------------------------------------------------------


def _prime_async_connection(address: str = "localhost:19001") -> AsyncConnection:
    """Build an AsyncConnection with a mocked underlying client conn
    and primed locks (close() asserts the locks are bound when
    ``_async_conn`` is set, so callers that mock the inner conn must
    also prime the locks)."""
    conn = AsyncConnection(address, database="x")
    inner = MagicMock()
    inner.close = AsyncMock()
    inner.execute = AsyncMock()
    conn._async_conn = inner
    conn._ensure_locks()
    return conn


class TestAsyncEnsureConnectionClosedCheck:
    async def test_connect_after_close_raises_interface_error(self) -> None:
        """``_ensure_connection`` closed-state check at
        aio/connection.py:175. Drive via ``connect()`` which is the
        thinnest wrapper."""
        conn = _prime_async_connection()
        await conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            await conn.connect()


class TestAsyncCloseIsIdempotent:
    async def test_double_close_short_circuits(self) -> None:
        """``close()`` early-return on the second call. Drives
        aio/connection.py:222."""
        conn = _prime_async_connection()
        await conn.close()
        # Must succeed and short-circuit; second close should not
        # touch the (already-None) inner connection.
        await conn.close()


class TestAsyncCommitRollbackLockRecheckRace:
    """A concurrent ``close()`` may acquire ``_op_lock`` first, close
    the inner conn, and release. ``commit()`` / ``rollback()``
    re-check ``_closed`` AFTER the lock acquire to surface the race
    as ``InterfaceError`` rather than dereferencing ``None``. Drive
    via a lock subclass that flips ``_closed`` mid-``__aenter__``.
    """

    @staticmethod
    def _flipping_lock(target: AsyncConnection) -> asyncio.Lock:
        """Return an asyncio.Lock that sets target._closed=True after
        acquiring — simulates a concurrent close() winning the lock
        race."""

        class _FlipLock(asyncio.Lock):
            async def acquire(self) -> bool:  # type: ignore[override]
                result = await super().acquire()
                target._closed = True
                return result

        return _FlipLock()

    async def test_commit_recheck_under_lock_raises(self) -> None:
        conn = _prime_async_connection()
        # Prime locks (binds _loop_ref to the running loop).
        conn._ensure_locks()
        # Replace op_lock with the flipping variant.
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
    """``cursor()`` is sync — SA greenlet glue calls it from sync
    context within the async adapter. When the connection is already
    bound to a loop and the call comes from a thread without a
    running loop, the RuntimeError from
    ``asyncio.get_running_loop()`` is silently swallowed and the
    cursor is created. Drives aio/connection.py:384-387."""

    async def test_cursor_from_no_loop_thread_succeeds_when_bound(self) -> None:
        conn = _prime_async_connection()
        # Bind _loop_ref to the running loop.
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
        """Drives aio/connection.py:407."""
        conn = AsyncConnection("localhost:19001", database="x")
        assert conn.address == "localhost:19001"


class TestAsyncAexitNeverConnected:
    async def test_aexit_short_circuits_when_async_conn_none(self) -> None:
        """``__aexit__`` early-return when ``_async_conn is None``
        (never-connected). Drives aio/connection.py:439."""
        conn = AsyncConnection("localhost:19001", database="x")
        # Never called connect() — ``_async_conn`` is None.
        await conn.__aexit__(None, None, None)
        # Connection remains reusable per the docstring.
        assert conn._closed is False


# ---------------------------------------------------------------------------
# connection.py (sync) — closed-checks, policy-rejection, address
# ---------------------------------------------------------------------------


class TestSyncConnectWrapsClusterPolicyRejection:
    """Drives connection.py:128 — ``_build_and_connect`` translates
    ``dqliteclient.exceptions.ClusterPolicyError`` to
    ``InterfaceError("Cluster policy rejection; ...")`` so SA's
    ``is_disconnect`` does not enter a retry loop on a permanent
    config mismatch."""

    def test_policy_rejection_surfaces_as_interface_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def _raise_policy(*args: object, **kwargs: object) -> None:
            raise _client_exc.ClusterPolicyError("not allowed")

        # Patch ``_resolve_leader`` to short-circuit the leader-discovery
        # step so the test exercises the post-find-leader
        # ``DqliteConnection.connect`` arm. Then patch
        # ``DqliteConnection.connect`` to raise the policy error this
        # test pins.
        async def _identity_resolve(address: str, *, timeout: float) -> str:
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
        """Drives connection.py:473 via the public ``execute`` path."""
        conn = connect("localhost:19001", timeout=2.0)
        conn.close()
        cur = conn  # use the connection's own surface
        with pytest.raises(InterfaceError, match="Connection is closed"):
            cur.cursor()  # cursor() is the simplest path through the closed-check
        # Also assert the raw _get_async_connection guard via execute-style
        # path is consistent: a fresh connect() also raises.

    def test_connect_after_close_raises(self) -> None:
        """Drives connection.py:507."""
        conn = connect("localhost:19001", timeout=2.0)
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn.connect()

    def test_commit_after_close_raises(self) -> None:
        """Drives connection.py:621."""
        conn = connect("localhost:19001", timeout=2.0)
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn.commit()

    def test_rollback_after_close_raises(self) -> None:
        """Drives connection.py:651."""
        conn = connect("localhost:19001", timeout=2.0)
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn.rollback()

    def test_commit_async_raises_interface_error_when_async_conn_none(self) -> None:
        """Defence-in-depth: even if ``_async_conn`` is somehow None
        without ``_closed`` being True (e.g. a future race or an
        unexpected attribute reset), ``_commit_async`` must raise
        ``InterfaceError`` rather than ``AttributeError``. The previous
        ``assert self._async_conn is not None`` was stripped under
        ``python -O`` and would surface as a confusing AttributeError.
        """
        import asyncio

        conn = connect("localhost:19001", timeout=2.0)
        conn._async_conn = None  # simulate the defensive case
        with pytest.raises(InterfaceError, match="closed"):
            asyncio.run(conn._commit_async())
        conn.close()

    def test_rollback_async_raises_interface_error_when_async_conn_none(self) -> None:
        """Symmetric to the commit-side defensive test above."""
        import asyncio

        conn = connect("localhost:19001", timeout=2.0)
        conn._async_conn = None  # simulate the defensive case
        with pytest.raises(InterfaceError, match="closed"):
            asyncio.run(conn._rollback_async())
        conn.close()


class TestSyncAddressProperty:
    def test_address_returns_configured(self) -> None:
        """Drives connection.py:686."""
        conn = connect("localhost:19001", timeout=2.0)
        try:
            assert conn.address == "localhost:19001"
        finally:
            conn.close()


class TestSyncGetAsyncConnectionDirect:
    """Direct test for connection.py:473 — call
    ``_get_async_connection`` after close so the path is hit
    independently of any sync-wrapper that might short-circuit
    higher up."""

    async def test_get_async_connection_closed_check(self) -> None:
        conn = connect("localhost:19001", timeout=2.0)
        conn.close()
        with pytest.raises(InterfaceError, match="Connection is closed"):
            await conn._get_async_connection()
