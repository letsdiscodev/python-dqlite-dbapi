"""Pin in_transaction under check_same_thread: relaxed (cross-thread reads
work) when False, raises ProgrammingError cross-thread when True. The fork
check is never relaxed, and closed connections always return False."""

from __future__ import annotations

import threading

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.exceptions import InterfaceError, ProgrammingError


def _make_conn(**kwargs: object) -> Connection:
    return Connection("localhost:9999", **kwargs)  # type: ignore[arg-type]


def test_in_transaction_default_raises_cross_thread() -> None:
    """Default check_same_thread=True: cross-thread read raises
    ProgrammingError."""
    conn = _make_conn()  # default True

    class _MockInner:
        in_transaction = False

    conn._async_conn = _MockInner()  # type: ignore[assignment]
    raised: list[BaseException] = []

    def worker() -> None:
        try:
            _ = conn.in_transaction
        except BaseException as e:
            raised.append(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert len(raised) == 1
    assert isinstance(raised[0], ProgrammingError)


def test_in_transaction_relaxed_under_flag_returns_bool() -> None:
    """check_same_thread=False: cross-thread read returns the inner bool
    without raising."""
    conn = _make_conn(check_same_thread=False)

    class _MockInner:
        in_transaction = True

    conn._async_conn = _MockInner()  # type: ignore[assignment]
    result: list[bool | None] = [None]

    def worker() -> None:
        result[0] = conn.in_transaction

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert result[0] is True


def test_in_transaction_fork_check_unconditional_under_flag() -> None:
    """Even under check_same_thread=False, a forked child still raises
    InterfaceError; fork-safety isn't a parameter."""
    conn = _make_conn(check_same_thread=False)
    conn._creator_pid = -1  # simulate a child process; any value != current pid

    # Leave _closed=False and _async_conn non-None so the property reaches the
    # fork check rather than short-circuiting on closed/never-connected.
    class _MockInner:
        in_transaction = True

    conn._async_conn = _MockInner()  # type: ignore[assignment]

    with pytest.raises(InterfaceError, match="used after fork"):
        _ = conn.in_transaction


def test_in_transaction_closed_returns_false_under_flag() -> None:
    """Closed-state precedence: even under check_same_thread=False, a closed
    connection short-circuits to False without any fork/thread check."""
    conn = _make_conn(check_same_thread=False)
    conn._closed = True
    result: list[bool | None] = [None]

    def worker() -> None:
        result[0] = conn.in_transaction

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert result[0] is False


def test_in_transaction_same_thread_unaffected() -> None:
    """Same-thread read works under both flag values."""
    conn_default = _make_conn()
    conn_relaxed = _make_conn(check_same_thread=False)
    assert conn_default.in_transaction is False
    assert conn_relaxed.in_transaction is False
