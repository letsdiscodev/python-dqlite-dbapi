"""Pin: ``Connection.in_transaction`` property is relaxed under
``check_same_thread=False`` so cross-thread reads work, matching
stdlib ``sqlite3.Connection.in_transaction`` (a plain C-level
attribute read with no thread check).

The default ``check_same_thread=True`` keeps the affinity raise for
shipped-API compatibility; cross-thread readers that pre-date the
relaxation continue to see ``ProgrammingError`` (e.g. monitoring
threads that catch the raise as a "wrong-thread access detected"
signal).

The fork check is NEVER relaxed: even under
``check_same_thread=False``, reading ``in_transaction`` from a
forked child raises ``InterfaceError``.

Closed-state precedence is preserved: ``in_transaction`` on a
closed connection returns ``False`` (no thread check at all).
"""

from __future__ import annotations

import threading

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.exceptions import InterfaceError, ProgrammingError


def _make_conn(**kwargs: object) -> Connection:
    return Connection("localhost:9999", **kwargs)  # type: ignore[arg-type]


def test_in_transaction_default_raises_cross_thread() -> None:
    """Default ``check_same_thread=True``: cross-thread read raises
    ProgrammingError (preserves shipped-API compatibility).

    Inject a non-None ``_async_conn`` so the never-connected
    short-circuit doesn't fire — the property has to reach the
    thread check."""
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
    """``check_same_thread=False``: cross-thread read returns the
    bool from the inner without raising.

    Inject a non-None ``_async_conn`` whose ``in_transaction``
    returns True; without the flag the read would raise
    ``ProgrammingError`` from ``_check_thread``."""
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
    """Even under ``check_same_thread=False``, a forked child still
    raises ``InterfaceError``. The fork-safety isn't a parameter.

    Simulated by tampering with ``_creator_pid``."""
    conn = _make_conn(check_same_thread=False)
    # Simulate a child process; close-short-circuit MUST run BEFORE
    # this becomes observable, so we leave _closed=False.
    conn._creator_pid = -1  # any value != current pid

    # The closed-state precedence at the top of the property
    # short-circuits BEFORE the fork check; tearing closed=True
    # bypasses everything. We want the property to fire the fork
    # check, so leave _closed=False AND _async_conn=something-
    # non-None to defeat the never-connected short-circuit.
    # Inject a mock that returns True for in_transaction so the
    # property would otherwise return True.
    class _MockInner:
        in_transaction = True

    conn._async_conn = _MockInner()  # type: ignore[assignment]

    with pytest.raises(InterfaceError, match="used after fork"):
        _ = conn.in_transaction


def test_in_transaction_closed_returns_false_under_flag() -> None:
    """Closed-state precedence: even under
    ``check_same_thread=False``, reading ``in_transaction`` on a
    closed connection short-circuits to ``False`` without any
    fork/thread check. This is the same as the default-True
    behaviour; the relaxation should not change closed-state
    semantics."""
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
    """Backward compat: same-thread read works under both flag
    values. The relaxation only opens up cross-thread; same-thread
    behaviour is unchanged."""
    conn_default = _make_conn()
    conn_relaxed = _make_conn(check_same_thread=False)
    # Never-connected → both return False (the short-circuit at the
    # top of the property).
    assert conn_default.in_transaction is False
    assert conn_relaxed.in_transaction is False
