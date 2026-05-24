"""Pin: ``dqlitedbapi.connect()`` (and ``Connection.__init__``) accept
``check_same_thread: bool = True``; when ``False``, the cross-thread
arm of ``_check_thread()`` is gated off so the same Connection can
be used from multiple OS threads.

Matches stdlib ``sqlite3.connect(check_same_thread=False)`` semantics:

- Default ``True`` preserves the existing strict per-thread contract.
- ``False`` allows cross-thread method calls.
- The fork check is NEVER relaxed: ``check_same_thread`` is about
  threads, not processes.
- ``close()`` keeps the cross-thread check even under
  ``check_same_thread=False`` (it tears down the daemon loop thread,
  which requires the creator's identity). Foreign-thread teardown
  uses ``force_close_transport()`` per the existing documented
  pattern.
- ``aconnect()`` / the async surface rejects the kwarg with a
  sync-only message — the async surface is loop-bound, not
  thread-bound, by asyncio's structured-concurrency contract.

This file pins:
1. Acceptance + storage of the kwarg.
2. Cross-thread method behavior under the flag.
3. Strict-bool validation.
4. Default behavior unchanged (backward compat).
5. Fork check unaffected.
6. close() still strict.
7. aconnect() rejection with sync-only message.
"""

from __future__ import annotations

import threading

import pytest

from dqlitedbapi import Connection, NotSupportedError
from dqlitedbapi.exceptions import InterfaceError, ProgrammingError


def _make_conn(**kwargs: object) -> Connection:
    """Construct a Connection without firing the wire."""
    return Connection("localhost:9999", **kwargs)  # type: ignore[arg-type]


# ---- Acceptance + storage ----------------------------------------


def test_connect_accepts_check_same_thread_false() -> None:
    """``connect()`` accepts ``check_same_thread=False`` (no
    NotSupportedError); the Connection stores the value."""
    conn = _make_conn(check_same_thread=False)
    assert conn._check_same_thread is False


def test_connect_accepts_check_same_thread_true_explicit() -> None:
    """Explicit ``True`` works the same as the default."""
    conn = _make_conn(check_same_thread=True)
    assert conn._check_same_thread is True


def test_connect_default_check_same_thread_is_true() -> None:
    """Omitting the kwarg yields the default ``True`` (matches stdlib
    sqlite3.connect default; preserves backward compat)."""
    conn = _make_conn()
    assert conn._check_same_thread is True


# ---- Validation ---------------------------------------------------


def test_connect_rejects_check_same_thread_string() -> None:
    """Strict-bool: string ``"false"`` rejected. Mirrors the strict-
    bool discipline for other Connection kwargs (e.g. busy_timeout
    rejects bool); here the strict gate is the other direction."""
    with pytest.raises(ProgrammingError, match="check_same_thread must be bool"):
        _make_conn(check_same_thread="false")


def test_connect_rejects_check_same_thread_int_one() -> None:
    """``1`` is not ``True`` for this kwarg. ``isinstance(1, bool)``
    is False — strict-bool check rejects."""
    with pytest.raises(ProgrammingError, match="check_same_thread must be bool"):
        _make_conn(check_same_thread=1)


def test_connect_rejects_check_same_thread_int_zero() -> None:
    """Same for ``0`` vs ``False``."""
    with pytest.raises(ProgrammingError, match="check_same_thread must be bool"):
        _make_conn(check_same_thread=0)


def test_connect_rejects_check_same_thread_none() -> None:
    """``None`` rejected (not a bool)."""
    with pytest.raises(ProgrammingError, match="check_same_thread must be bool"):
        _make_conn(check_same_thread=None)


# ---- Cross-thread method behavior --------------------------------


def test_cross_thread_check_thread_passes_under_flag() -> None:
    """``_check_thread()`` on a Connection with
    ``check_same_thread=False`` returns cleanly from any thread."""
    conn = _make_conn(check_same_thread=False)
    # Sanity: same-thread works.
    conn._check_thread()
    # Cross-thread: a worker calls _check_thread; assert no raise.
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
    """Backward compat: default ``check_same_thread=True`` (omitted)
    still raises ``ProgrammingError`` on cross-thread call. This
    test exists to guard against an accidental flip of the
    default."""
    conn = _make_conn()  # default True
    # Sanity: same-thread works.
    conn._check_thread()
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
    """The cross-thread ``ProgrammingError`` message mentions the
    kwarg name so operators reading the traceback understand the
    workaround."""
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


# ---- Fork check stays unconditional ------------------------------


def test_fork_check_unaffected_by_check_same_thread_false() -> None:
    """The fork check is NEVER relaxed by ``check_same_thread``.
    ``check_same_thread`` is about threads, not processes; cross-
    process Connection use raises ``InterfaceError`` in BOTH modes.

    We simulate a forked child by mutating ``_creator_pid`` to a
    different value (we can't actually fork in a test process
    cleanly)."""
    conn = _make_conn(check_same_thread=False)
    # Simulate a child process by tampering with the creator pid.
    conn._creator_pid = -1  # any value != current pid
    with pytest.raises(InterfaceError, match="used after fork"):
        conn._check_thread()


# ---- close() stays strict ----------------------------------------


def test_close_still_strict_under_flag() -> None:
    """``Connection.close()`` is NOT relaxed by
    ``check_same_thread=False``: it tears down the daemon loop
    thread synchronously, a creator-thread-only operation.
    Foreign-thread teardown uses ``force_close_transport()`` (the
    documented foreign-thread path; SA's pool recycle uses it).

    Runtime test: construct a Connection with
    ``check_same_thread=False`` on the main thread, then call
    ``close()`` from a worker thread. The worker MUST raise
    ProgrammingError even though the cross-thread check is gated
    off everywhere else.
    """
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
    """Negative pin to #test_close_still_strict_under_flag: close()
    from the creator thread works (loop teardown is on the right
    thread). The connection never connected (no _ensure_loop) so
    the close path short-circuits at ``self._loop is None``."""
    conn = _make_conn(check_same_thread=False)
    conn.close()
    assert conn._closed is True


# ---- async sibling rejection -------------------------------------


@pytest.mark.asyncio
async def test_aconnect_rejects_check_same_thread() -> None:
    """``aconnect()`` rejects ``check_same_thread`` with a sync-only
    message — the async surface is loop-bound by asyncio's
    structured-concurrency contract; the kwarg has no equivalent."""
    from dqlitedbapi.aio import aconnect

    with pytest.raises(NotSupportedError) as exc_info:
        await aconnect("localhost:9999", check_same_thread=False)
    msg = str(exc_info.value)
    assert "check_same_thread" in msg
    assert "sync-only" in msg.lower() or "sync only" in msg.lower()


def test_aio_connect_sync_rejects_check_same_thread() -> None:
    """The lazy ``dqlitedbapi.aio.connect()`` (SA's sync-API-required
    entry point) also rejects the kwarg with the sync-only
    message."""
    from dqlitedbapi.aio import connect as aio_connect

    with pytest.raises(NotSupportedError) as exc_info:
        aio_connect("localhost:9999", check_same_thread=False)
    msg = str(exc_info.value)
    assert "check_same_thread" in msg
    assert "sync-only" in msg.lower() or "sync only" in msg.lower()
