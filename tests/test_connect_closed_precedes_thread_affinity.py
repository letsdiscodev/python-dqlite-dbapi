"""Pin: ``Connection.connect()`` on a closed connection raises
``InterfaceError`` (closed-state) even when called from a non-creator
thread — closed-first precedence, mirroring every other public
method on the Connection class.

Previously ``connect()`` ordered ``_check_thread()`` BEFORE the
closed-state check, so a finalizer / atexit / pool-cleanup hook
running on a non-creator thread got the thread-affinity diagnostic
rather than the closed diagnostic. Stdlib sqlite3 and every other
public method on Connection (commit/rollback/cursor/transaction/
execute/executemany/...) raise closed-first regardless of thread.

The rationale comment at ``connection.py`` (``commit`` method) is
the canonical site: "closed-conn diagnostic is more salient than
thread-affinity". ``connect()`` was the lone deviation.
"""

from __future__ import annotations

import threading
from typing import Any

from dqlitedbapi import Connection
from dqlitedbapi.exceptions import InterfaceError, ProgrammingError


def test_connect_on_closed_from_other_thread_raises_interface_error() -> None:
    """A closed connection accessed from a non-creator thread via
    ``connect()`` must raise InterfaceError (closed-first), not
    ProgrammingError (thread-affinity)."""
    conn = Connection("localhost:9001", timeout=2.0)
    conn.close()

    result_holder: list[Any] = [None]

    def call_from_other_thread() -> None:
        try:
            conn.connect()
        except Exception as e:  # noqa: BLE001 -- pin captures whatever is raised
            result_holder[0] = e

    t = threading.Thread(target=call_from_other_thread)
    t.start()
    t.join(timeout=2.0)

    assert isinstance(result_holder[0], InterfaceError), (
        f"closed-first precedence broken: expected InterfaceError on "
        f"closed conn from non-creator thread, got "
        f"{type(result_holder[0]).__name__}: {result_holder[0]}"
    )
    assert "closed" in str(result_holder[0]).lower()


def test_connect_on_closed_from_creator_thread_raises_interface_error() -> None:
    """Sanity: closed-state check works from the creator thread too."""
    conn = Connection("localhost:9001", timeout=2.0)
    conn.close()
    try:
        conn.connect()
    except InterfaceError as e:
        assert "closed" in str(e).lower()
    else:
        raise AssertionError("expected InterfaceError on closed connect()")


def test_connect_from_other_thread_on_open_conn_still_raises_thread_affinity() -> None:
    """Negative pin: the closed-first re-ordering must NOT mask the
    thread-affinity check on an OPEN connection accessed from a
    non-creator thread. After the swap, both diagnostics still fire;
    only their relative order on closed conns changes."""
    conn = Connection("localhost:9001", timeout=2.0)
    try:
        result_holder: list[Any] = [None]

        def call_from_other_thread() -> None:
            try:
                conn.connect()
            except Exception as e:  # noqa: BLE001
                result_holder[0] = e

        t = threading.Thread(target=call_from_other_thread)
        t.start()
        t.join(timeout=2.0)

        assert isinstance(result_holder[0], ProgrammingError), (
            f"open conn from non-creator thread must still raise "
            f"ProgrammingError (thread-affinity); got "
            f"{type(result_holder[0]).__name__}: {result_holder[0]}"
        )
    finally:
        conn.close()
