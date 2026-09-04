"""Coverage gaps surfaced by the 2026-05 audit pass."""

import asyncio
import logging
import os
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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


async def test_aenter_close_failure_debug_logged(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If __aenter__'s best-effort close also raises, it's DEBUG-logged and the
    original connect error keeps propagating (must not be masked)."""

    async def boom_connect(self: AsyncConnection) -> None:
        raise OperationalError("simulated connect fail")

    async def boom_close(self: AsyncConnection) -> None:
        raise RuntimeError("simulated close fail")

    monkeypatch.setattr(AsyncConnection, "connect", boom_connect)
    monkeypatch.setattr(AsyncConnection, "close", boom_close)

    # aio.connect (NOT aconnect) does not pre-connect, so connect() first runs
    # inside __aenter__ — needed to trigger the cleanup-close path there.
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


def _build_classifier_cases() -> list[tuple[Exception, type[Exception], str]]:
    """Cases built in a function (not parametrize argvalues, which pytest holds
    session-long) so no DqliteError subclass leaks into the forward-compat sweep."""
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


async def test_build_and_connect_dqlite_error_catch_all_arm() -> None:
    """An un-enumerated DqliteError subclass surfaces as DatabaseError with the
    "unrecognized client error" prefix. The dynamic class is deleted at end so it
    doesn't persist in __subclasses__ and trip the forward-compat sweep."""
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
        # Drop the only strong reference so __subclasses__ reaps it.
        del fake_cls
        import gc

        gc.collect()


def test_sync_executemany_shortcut_closes_cursor_on_raise() -> None:
    """If cursor.executemany raises, the never-returned cursor must be closed
    before the exception propagates, else its state leaks unreachably."""
    conn = dqlitedbapi.Connection.__new__(dqlitedbapi.Connection)
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


async def test_async_execute_shortcut_closes_cursor_on_raise() -> None:
    """Mirror of the sync test for ``AsyncConnection.execute``."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    cursors_seen: list[AsyncCursor] = []

    def fake_cursor() -> AsyncCursor:
        cur = MagicMock(spec=AsyncCursor)
        cur.execute = AsyncMock(side_effect=OperationalError("boom"))
        # AsyncCursor.close is sync by design, so MagicMock (not AsyncMock).
        cur.close = MagicMock()
        cursors_seen.append(cur)
        return cur

    aconn.cursor = fake_cursor  # type: ignore[assignment]
    aconn.messages = []

    with pytest.raises(OperationalError, match="boom"):
        await aconn.execute("SELECT 1")

    assert len(cursors_seen) == 1
    close_mock = cursors_seen[0].close
    assert isinstance(close_mock, MagicMock)
    close_mock.assert_called_once()


async def test_async_executemany_shortcut_closes_cursor_on_raise() -> None:
    """Mirror of the sync test for ``AsyncConnection.executemany``."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    cursors_seen: list[AsyncCursor] = []

    def fake_cursor() -> AsyncCursor:
        cur = MagicMock(spec=AsyncCursor)
        cur.executemany = AsyncMock(side_effect=OperationalError("boom"))
        cur.close = MagicMock()
        cursors_seen.append(cur)
        return cur

    aconn.cursor = fake_cursor  # type: ignore[assignment]
    aconn.messages = []

    with pytest.raises(OperationalError, match="boom"):
        await aconn.executemany("INSERT INTO t VALUES (?)", [(1,)])

    assert len(cursors_seen) == 1
    close_mock = cursors_seen[0].close
    assert isinstance(close_mock, MagicMock)
    close_mock.assert_called_once()


def test_sync_force_close_transport_post_fork_short_circuits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """In a forked child (pid mismatch), force_close_transport marks cursors closed
    and detaches the finalizer without touching the inherited socket — else FIN on a
    shared fd kills the parent's session."""
    import weakref

    conn = dqlitedbapi.Connection.__new__(dqlitedbapi.Connection)
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

    # One cursor so the cascade arm has something to walk.
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

    # Simulate fork: bump the pid seen by the connection.
    _real_getpid = os.getpid
    monkeypatch.setattr("dqliteclient.connection.os.getpid", lambda: _real_getpid() + 1)

    conn.force_close_transport()

    assert cur._closed is True
    assert cur._description is None
    assert cur._row_index == 0
    assert len(conn._cursors) == 0
    assert conn._finalizer is None
    assert conn._closed is True
    assert conn._closed_flag[0] is True


def test_run_sync_ki_cleanup_exception_debug_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """KI during future.result: we cancel and bound-wait; an Exception in the
    cleanup is DEBUG-logged but the KI must still re-raise (else Ctrl-C is masked)."""
    import concurrent.futures

    conn = dqlitedbapi.Connection.__new__(dqlitedbapi.Connection)
    conn._timeout = 0.5
    conn._closed = False
    conn._async_conn = None
    conn._creator_pid = os.getpid()
    conn._op_lock = threading.RLock()  # type: ignore[assignment]

    # First result() raises KI; the bounded-wait result() raises a vanilla Exception.
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
            # The fake future never consumed the coroutine; close it to avoid
            # the "coroutine was never awaited" warning.
            coro.close()

    debug_records = [r for r in caplog.records if r.levelname == "DEBUG"]
    assert any("KI/SystemExit cleanup" in r.message for r in debug_records), (
        "expected KI/SystemExit cleanup DEBUG record from the bounded-wait Exception arm"
    )
    fake_future.cancel.assert_called()
