"""Coverage gaps surfaced by the 2026-05 audit pass.

Five behavioural arms that are correct in the source but were not
pinned by the existing test suite — a regression that re-shapes any
of them would have shipped silently:

1. ``AsyncConnection.__aenter__`` cleanup-close DEBUG-log arm.
2. ``_build_and_connect`` ProtocolError / DataError /
   InterfaceError / DqliteError catch-all / OSError arms.
3. Connection.execute / executemany shortcut: cleanup-on-raise
   closes the freshly-opened cursor (sync executemany; async
   execute; async executemany).
4. Sync ``Connection.force_close_transport`` post-fork pid-mismatch
   arm (cursor cascade + finalizer detach without touching the
   inherited socket).
5. ``_run_sync`` KeyboardInterrupt cleanup: bounded-wait Exception
   debug-log arm.
"""

import asyncio
import logging
import os
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import dqliteclient.connection as _client_conn_mod
import dqliteclient.exceptions as _client_exc
import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.aio import connect as aio_connect
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import _build_and_connect
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import (
    DatabaseError,
    DataError,
    InterfaceError,
    OperationalError,
)

# ---------------------------------------------------------------
# 1. AsyncConnection.__aenter__ cleanup-close DEBUG-log arm
# ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_aenter_close_failure_debug_logged(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When ``__aenter__`` runs ``await self.connect()`` and connect
    raises, it best-effort closes; if THAT close also raises, the
    exception is DEBUG-logged with exc_info. The original connect
    error must continue propagating.

    Without this pin, a refactor that re-raises the close error
    would silently mask the real connect failure for operators."""

    async def boom_connect(self: AsyncConnection) -> None:
        raise OperationalError("simulated connect fail")

    async def boom_close(self: AsyncConnection) -> None:
        raise RuntimeError("simulated close fail")

    monkeypatch.setattr(AsyncConnection, "connect", boom_connect)
    monkeypatch.setattr(AsyncConnection, "close", boom_close)

    # ``aio.connect`` (NOT ``aconnect``) does NOT pre-connect — the
    # AsyncConnection is returned lazily and ``__aenter__`` is the
    # first place ``connect()`` runs. That's what we need to trigger
    # the cleanup-close path INSIDE ``__aenter__``.
    aconn = aio_connect("127.0.0.1:9999")
    with (
        caplog.at_level(logging.DEBUG, logger="dqlitedbapi.aio.connection"),
        pytest.raises(OperationalError, match="simulated connect fail"),
    ):
        async with aconn as _ctx:
            pytest.fail("body should not run")

    debug_records = [r for r in caplog.records if r.levelname == "DEBUG"]
    assert any(r.exc_info is not None for r in debug_records), (
        "expected a DEBUG record carrying exc_info from the cleanup-close failure"
    )


# ---------------------------------------------------------------
# 2. _build_and_connect connect-time classifier arms
# ---------------------------------------------------------------


def _build_classifier_cases() -> list[tuple[Exception, type[Exception], str]]:
    """Build the classifier-arm cases as a function so any
    dynamically-created subclass of ``_client_exc.DqliteError`` lives
    only inside the test module — not in the parametrize argvalues
    list, which pytest holds for the full session and which would
    otherwise leak into ``__subclasses__`` and trip the forward-compat
    test in ``test_audit_2026_05_coverage_gaps.py``."""
    return [
        (
            _client_exc.ProtocolError("decode bad"),
            OperationalError,
            "wire decode failed",
        ),
        (
            _client_exc.DataError("encode bad"),
            DataError,
            "encode bad",
        ),
        (
            _client_exc.InterfaceError("driver misuse"),
            InterfaceError,
            "driver misuse",
        ),
        # OSError escaping the client-layer wrap discipline.
        (
            ConnectionResetError("transport reset"),
            OperationalError,
            "Failed to connect",
        ),
    ]


@pytest.mark.parametrize(
    ("client_exc", "expected_dbapi_cls", "expected_substring"),
    _build_classifier_cases(),
)
@pytest.mark.asyncio
async def test_build_and_connect_classifier_arms(
    client_exc: Exception,
    expected_dbapi_cls: type[Exception],
    expected_substring: str,
) -> None:
    with (
        patch(
            "dqlitedbapi.connection._resolve_leader",
            new=AsyncMock(return_value="127.0.0.1:9001"),
        ),
        patch("dqlitedbapi.connection.DqliteConnection") as MockConn,
    ):
        instance = MagicMock()
        instance.connect = AsyncMock(side_effect=client_exc)
        MockConn.return_value = instance

        with pytest.raises(expected_dbapi_cls) as exc_info:
            await _build_and_connect(
                "127.0.0.1:9001",
                database="default",
                timeout=1.0,
                max_total_rows=None,
                max_continuation_frames=None,
                trust_server_heartbeat=False,
                close_timeout=0.5,
            )

    assert expected_substring in str(exc_info.value)


@pytest.mark.asyncio
async def test_build_and_connect_dqlite_error_catch_all_arm() -> None:
    """Pin the ``DqliteError`` catch-all arm: a future client-layer
    subclass not enumerated in the per-class arms must surface as
    ``DatabaseError`` with the canonical
    ``"unrecognized client error"`` prefix.

    The dynamic class is constructed and then deleted at end-of-test
    so it doesn't persist in ``__subclasses__`` and trip the
    forward-compat regression test that audits all DqliteError
    subclasses for explicit ``_call_client`` coverage."""
    fake_cls = type("FakeFutureDqliteError", (_client_exc.DqliteError,), {})
    try:
        with (
            patch(
                "dqlitedbapi.connection._resolve_leader",
                new=AsyncMock(return_value="127.0.0.1:9001"),
            ),
            patch("dqlitedbapi.connection.DqliteConnection") as MockConn,
        ):
            instance = MagicMock()
            instance.connect = AsyncMock(side_effect=fake_cls("future error"))
            MockConn.return_value = instance

            with pytest.raises(DatabaseError) as exc_info:
                await _build_and_connect(
                    "127.0.0.1:9001",
                    database="default",
                    timeout=1.0,
                    max_total_rows=None,
                    max_continuation_frames=None,
                    trust_server_heartbeat=False,
                    close_timeout=0.5,
                )

        assert "unrecognized client error" in str(exc_info.value)
    finally:
        # Drop the only strong reference so __subclasses__ reaps it
        # before the next test queries DqliteError.__subclasses__.
        del fake_cls
        import gc

        gc.collect()


# ---------------------------------------------------------------
# 3. Connection.execute / executemany shortcut cleanup-on-raise
# ---------------------------------------------------------------


def test_sync_executemany_shortcut_closes_cursor_on_raise() -> None:
    """``Connection.executemany`` opens a cursor; if cursor.executemany
    raises, the cursor must be closed before the exception
    propagates. Without the cleanup, partially-iterated state would
    leak with no caller able to clean it up (the cursor was never
    returned)."""
    conn = dqlitedbapi.Connection.__new__(dqlitedbapi.Connection)
    # Prime just enough state for ``Connection.executemany`` to reach
    # cur.executemany without going through the loop machinery.
    cursors_seen: list[Cursor] = []

    def fake_cursor() -> Cursor:
        cur = MagicMock(spec=Cursor)
        cur.executemany = MagicMock(side_effect=OperationalError("boom"))
        cur.close = MagicMock()
        cursors_seen.append(cur)
        return cur

    conn.cursor = fake_cursor  # type: ignore[assignment]
    conn.messages = []

    with pytest.raises(OperationalError, match="boom"):
        conn.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])

    assert len(cursors_seen) == 1
    close_mock = cursors_seen[0].close
    assert isinstance(close_mock, MagicMock)
    close_mock.assert_called_once()


@pytest.mark.asyncio
async def test_async_execute_shortcut_closes_cursor_on_raise() -> None:
    """Mirror of the sync test for ``AsyncConnection.execute``."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    cursors_seen: list[AsyncCursor] = []

    def fake_cursor() -> AsyncCursor:
        cur = MagicMock(spec=AsyncCursor)
        cur.execute = AsyncMock(side_effect=OperationalError("boom"))
        cur.close = AsyncMock()
        cursors_seen.append(cur)
        return cur

    aconn.cursor = fake_cursor  # type: ignore[assignment]
    aconn.messages = []

    with pytest.raises(OperationalError, match="boom"):
        await aconn.execute("SELECT 1")

    assert len(cursors_seen) == 1
    close_mock = cursors_seen[0].close
    assert isinstance(close_mock, AsyncMock)
    close_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_executemany_shortcut_closes_cursor_on_raise() -> None:
    """Mirror of the sync test for ``AsyncConnection.executemany``."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    cursors_seen: list[AsyncCursor] = []

    def fake_cursor() -> AsyncCursor:
        cur = MagicMock(spec=AsyncCursor)
        cur.executemany = AsyncMock(side_effect=OperationalError("boom"))
        cur.close = AsyncMock()
        cursors_seen.append(cur)
        return cur

    aconn.cursor = fake_cursor  # type: ignore[assignment]
    aconn.messages = []

    with pytest.raises(OperationalError, match="boom"):
        await aconn.executemany("INSERT INTO t VALUES (?)", [(1,)])

    assert len(cursors_seen) == 1
    close_mock = cursors_seen[0].close
    assert isinstance(close_mock, AsyncMock)
    close_mock.assert_awaited_once()


# ---------------------------------------------------------------
# 4. Sync force_close_transport post-fork pid-mismatch arm
# ---------------------------------------------------------------


def test_sync_force_close_transport_post_fork_short_circuits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the module-level ``_current_pid`` no longer matches the
    Connection's ``_creator_pid`` (i.e. we're now running in a
    forked child), ``force_close_transport`` must mark cursors
    closed and detach the finalizer without touching the inherited
    socket / loop. Without this branch, FIN goes out on a shared fd
    and silently terminates the parent's session."""
    import weakref

    conn = dqlitedbapi.Connection.__new__(dqlitedbapi.Connection)
    # Prime minimum state.
    conn._closed = False
    conn._closed_flag = [False]
    conn._creator_pid = os.getpid()
    conn._loop_lock = threading.Lock()
    conn._loop = None
    conn._thread = None
    conn._async_conn = None
    conn._cursors = weakref.WeakSet()
    conn._finalizer = MagicMock()
    conn._close_timeout = 0.5

    # Inject one cursor so the cascade arm has something to walk.
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._rows = [(1,)]
    cur._description = (("c", None, None, None, None, None, None),)
    cur._rowcount = 1
    cur._lastrowid = None
    cur._row_index = 0
    cur._connection = conn
    cur.messages = []
    conn._cursors.add(cur)

    # Simulate fork: bump _client_conn_mod._current_pid.
    monkeypatch.setattr(_client_conn_mod, "_current_pid", os.getpid() + 1)

    conn.force_close_transport()

    # The pid-mismatch arm marks cursors closed, detaches finalizer,
    # and returns without touching ``_loop_lock`` / writer.
    assert cur._closed is True
    assert cur._description is None
    assert cur._row_index == 0
    assert len(conn._cursors) == 0
    assert conn._finalizer is None
    assert conn._closed is True
    assert conn._closed_flag[0] is True


# ---------------------------------------------------------------
# 5. _run_sync KI cleanup bounded-wait Exception debug-log arm
# ---------------------------------------------------------------


def test_run_sync_ki_cleanup_exception_debug_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """KI lands during ``future.result(timeout=self._timeout)``; we
    cancel the future and bound-wait 1 s. If the cancelled coroutine
    raises a non-Cancelled / non-Timeout Exception (programming bug
    in cleanup), DEBUG-log it and continue to re-raise the original
    KI.

    Without coverage, a regression that re-raises the cleanup
    exception would mask the KI signal — catastrophic for the
    Ctrl-C-driven shutdown path.
    """
    import concurrent.futures

    conn = dqlitedbapi.Connection.__new__(dqlitedbapi.Connection)
    conn._timeout = 0.5
    conn._closed = False
    conn._async_conn = None
    conn._creator_pid = os.getpid()
    conn._op_lock = threading.RLock()  # type: ignore[assignment]

    # Construct a fake future that:
    # 1. raises KeyboardInterrupt on the first ``future.result(timeout=self._timeout)``
    # 2. raises a vanilla Exception on the bounded-wait ``future.result(timeout=1.0)``
    fake_future = MagicMock(spec=concurrent.futures.Future)
    fake_future.result = MagicMock(
        side_effect=[
            KeyboardInterrupt(),
            RuntimeError("simulated cleanup-bug error after cancel"),
        ]
    )
    fake_future.cancel = MagicMock()
    fake_future.done = MagicMock(return_value=False)
    fake_future.cancelled = MagicMock(return_value=False)

    fake_loop = MagicMock()
    fake_loop.call_soon_threadsafe = MagicMock()

    with (
        patch.object(conn, "_ensure_loop", return_value=fake_loop),
        patch(
            "dqlitedbapi.connection.asyncio.run_coroutine_threadsafe",
            return_value=fake_future,
        ),
        caplog.at_level(logging.DEBUG, logger="dqlitedbapi.connection"),
    ):

        async def victim() -> None:
            await asyncio.sleep(0)

        coro = victim()
        try:
            with pytest.raises(KeyboardInterrupt):
                conn._run_sync(coro)
        finally:
            # The fake future never actually consumed the coroutine —
            # close it explicitly so we don't trip the
            # ``coroutine was never awaited`` warning at gc.
            coro.close()

    debug_records = [r for r in caplog.records if r.levelname == "DEBUG"]
    assert any("KI/SystemExit cleanup" in r.message for r in debug_records), (
        "expected KI/SystemExit cleanup DEBUG record from the bounded-wait Exception arm"
    )
    fake_future.cancel.assert_called()


# ---------------------------------------------------------------
# Suppress ``coroutine ... was never awaited`` warnings from the
# stub coroutines that we deliberately route through MagicMock.
# ---------------------------------------------------------------
