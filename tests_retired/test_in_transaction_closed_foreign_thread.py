"""Pin: in_transaction on a closed connection returns False from any thread.
The closed short-circuit must precede _check_thread so closed-state behaviour
is thread-independent (foreign-thread shutdown hooks must not see
ProgrammingError)."""

import os
import threading

import pytest

from dqlitedbapi.connection import Connection


def _make_closed_connection() -> Connection:
    conn = Connection.__new__(Connection)
    conn._closed = True
    conn._async_conn = None
    conn._creator_pid = os.getpid()
    conn._creator_thread = threading.get_ident()
    return conn


def test_in_transaction_returns_false_on_closed_conn_from_creator_thread() -> None:
    conn = _make_closed_connection()
    assert conn.in_transaction is False


def test_in_transaction_returns_false_on_closed_conn_from_foreign_thread() -> None:
    conn = _make_closed_connection()
    result: dict[str, object] = {}

    def reader() -> None:
        try:
            result["value"] = conn.in_transaction
        except BaseException as exc:
            result["error"] = exc

    t = threading.Thread(target=reader)
    t.start()
    t.join()

    assert "error" not in result, (
        "in_transaction on a closed connection must not raise from a "
        f"foreign thread; got {result.get('error')!r}. The closed "
        "short-circuit must run BEFORE _check_thread() so closed-state "
        "behaviour is thread-independent."
    )
    assert result.get("value") is False


def test_in_transaction_check_thread_still_runs_on_open_conn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Open-connection reads still run the thread-affinity check."""
    from dqlitedbapi.exceptions import ProgrammingError

    conn = Connection.__new__(Connection)
    conn._closed = False
    # Truthy so the closed branch is skipped; the read short-circuits at the
    # thread check before consulting conn.in_transaction.
    conn._async_conn = object()  # type: ignore[assignment]
    conn._creator_pid = os.getpid()
    conn._creator_thread = -1  # synthetic value no real thread holds

    def reader_value() -> object:
        return conn.in_transaction

    result: dict[str, object] = {}

    def runner() -> None:
        try:
            result["value"] = reader_value()
        except BaseException as exc:
            result["error"] = exc

    t = threading.Thread(target=runner)
    t.start()
    t.join()

    err = result.get("error")
    assert isinstance(err, ProgrammingError), (
        "Open-connection in_transaction read from a foreign thread "
        "must still raise ProgrammingError via _check_thread; got "
        f"{err!r}"
    )
