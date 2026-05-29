"""``check_same_thread=False`` gates off the cross-thread arm of
``_check_thread()`` (stdlib sqlite3 semantics). The fork check and
``close()`` stay strict; the async surface rejects the kwarg."""

from __future__ import annotations

import threading

import pytest

from dqlitedbapi import Connection, NotSupportedError
from dqlitedbapi.exceptions import InterfaceError, ProgrammingError


def _make_conn(**kwargs: object) -> Connection:
    """Construct a Connection without firing the wire."""
    return Connection("localhost:9999", **kwargs)  # type: ignore[arg-type]


def test_connect_accepts_check_same_thread_false() -> None:
    conn = _make_conn(check_same_thread=False)
    assert conn._check_same_thread is False


def test_connect_accepts_check_same_thread_true_explicit() -> None:
    conn = _make_conn(check_same_thread=True)
    assert conn._check_same_thread is True


def test_connect_default_check_same_thread_is_true() -> None:
    conn = _make_conn()
    assert conn._check_same_thread is True


def test_connect_rejects_check_same_thread_string() -> None:
    with pytest.raises(ProgrammingError, match="check_same_thread must be bool"):
        _make_conn(check_same_thread="false")


def test_connect_rejects_check_same_thread_int_one() -> None:
    # isinstance(1, bool) is False — strict-bool rejects.
    with pytest.raises(ProgrammingError, match="check_same_thread must be bool"):
        _make_conn(check_same_thread=1)


def test_connect_rejects_check_same_thread_int_zero() -> None:
    with pytest.raises(ProgrammingError, match="check_same_thread must be bool"):
        _make_conn(check_same_thread=0)


def test_connect_rejects_check_same_thread_none() -> None:
    with pytest.raises(ProgrammingError, match="check_same_thread must be bool"):
        _make_conn(check_same_thread=None)


def test_cross_thread_check_thread_passes_under_flag() -> None:
    conn = _make_conn(check_same_thread=False)
    conn._check_thread()  # same-thread sanity
    raised: list[BaseException] = []

    def worker() -> None:
        try:
            conn._check_thread()
        except BaseException as e:
            raised.append(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert raised == [], f"_check_thread raised under flag: {raised}"


def test_cross_thread_check_thread_default_raises() -> None:
    """Default (omitted) still raises on cross-thread call; guards
    against an accidental flip of the default."""
    conn = _make_conn()  # default True
    conn._check_thread()  # same-thread sanity
    raised: list[BaseException] = []

    def worker() -> None:
        try:
            conn._check_thread()
        except BaseException as e:
            raised.append(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert len(raised) == 1
    assert isinstance(raised[0], ProgrammingError)


def test_diagnostic_mentions_check_same_thread_in_raise_message() -> None:
    """Cross-thread error names the kwarg so operators see the workaround."""
    conn = _make_conn()  # default True
    raised: list[BaseException] = []

    def worker() -> None:
        try:
            conn._check_thread()
        except BaseException as e:
            raised.append(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert len(raised) == 1
    msg = str(raised[0])
    assert "check_same_thread=False" in msg, (
        "diagnostic must point at the workaround so operators "
        f"reading the traceback know what to do; got: {msg}"
    )


def test_fork_check_unaffected_by_check_same_thread_false() -> None:
    """The fork check is never relaxed by ``check_same_thread`` (it is
    about threads, not processes); cross-process use raises in both modes."""
    conn = _make_conn(check_same_thread=False)
    conn._creator_pid = -1  # simulate a forked child (pid != current)
    with pytest.raises(InterfaceError, match="used after fork"):
        conn._check_thread()


def test_close_still_strict_under_flag() -> None:
    """``close()`` stays strict under the flag: it tears down the daemon
    loop thread synchronously, a creator-thread-only operation.
    Foreign-thread teardown uses ``force_close_transport()``."""
    conn = _make_conn(check_same_thread=False)
    raised: list[BaseException] = []

    def worker_closes() -> None:
        try:
            conn.close()
        except BaseException as e:
            raised.append(e)

    t = threading.Thread(target=worker_closes)
    t.start()
    t.join()
    assert len(raised) == 1
    assert isinstance(raised[0], ProgrammingError)
    msg = str(raised[0])
    assert "close" in msg.lower()
    assert "force_close_transport" in msg, (
        "diagnostic must point at the foreign-thread teardown "
        f"workaround so operators reading the traceback know what "
        f"to use; got: {msg}"
    )


def test_close_from_creator_thread_succeeds_under_flag() -> None:
    """close() from the creator thread works; never-connected conn
    short-circuits at ``self._loop is None``."""
    conn = _make_conn(check_same_thread=False)
    conn.close()
    assert conn._closed is True


@pytest.mark.asyncio
async def test_aconnect_rejects_check_same_thread() -> None:
    """``aconnect()`` rejects the kwarg (async surface is loop-bound,
    not thread-bound) with a sync-only message."""
    from dqlitedbapi.aio import aconnect

    with pytest.raises(NotSupportedError) as exc_info:
        await aconnect("localhost:9999", check_same_thread=False)
    msg = str(exc_info.value)
    assert "check_same_thread" in msg
    assert "sync-only" in msg.lower() or "sync only" in msg.lower()


def test_aio_connect_sync_rejects_check_same_thread() -> None:
    """The lazy ``dqlitedbapi.aio.connect()`` also rejects the kwarg."""
    from dqlitedbapi.aio import connect as aio_connect

    with pytest.raises(NotSupportedError) as exc_info:
        aio_connect("localhost:9999", check_same_thread=False)
    msg = str(exc_info.value)
    assert "check_same_thread" in msg
    assert "sync-only" in msg.lower() or "sync only" in msg.lower()
