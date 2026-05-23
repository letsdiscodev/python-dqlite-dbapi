"""Pin: ``Connection.connect()`` precedence: thread-affinity check
FIRST, then closed-state, then ``messages`` clear. Mirrors the
async sibling's discipline at ``aio/connection.py:1576-1582``.

Rationale: a foreign-thread caller must not mutate the owner
thread's ``messages`` list before the diagnostic fires. PEP 249
§6.1.1's "messages cleared by every standard method call" invariant
scopes to the owning caller; cross-thread misuse hitting the clear
first violates that.

Replaces the prior "closed-first precedence" pin -- the project
explicitly reversed the precedence to align with the async sibling.
"""

from __future__ import annotations

import threading
from typing import Any

from dqlitedbapi import Connection
from dqlitedbapi.exceptions import InterfaceError, ProgrammingError


def test_connect_on_closed_from_creator_thread_raises_interface_error() -> None:
    """On the creator thread, closed-state is the only failure mode
    and surfaces as InterfaceError."""
    conn = Connection("localhost:9001", timeout=2.0)
    conn.close()
    try:
        conn.connect()
    except InterfaceError as e:
        assert "closed" in str(e).lower()
    else:
        raise AssertionError("expected InterfaceError on closed connect()")


def test_connect_from_other_thread_raises_thread_affinity_first() -> None:
    """Foreign-thread caller surfaces the thread-affinity diagnostic
    (ProgrammingError) -- even when the connection is closed. The
    thread check runs BEFORE the closed-state check so a cross-thread
    caller cannot mutate the owner thread's ``messages`` list."""
    conn = Connection("localhost:9001", timeout=2.0)
    conn.close()

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
        f"thread-affinity precedence broken: expected ProgrammingError "
        f"from a foreign-thread caller (even on closed conn), got "
        f"{type(result_holder[0]).__name__}: {result_holder[0]}"
    )


def test_connect_from_other_thread_on_open_conn_raises_thread_affinity() -> None:
    """Negative pin: open conn from foreign thread still raises the
    thread-affinity diagnostic (no regression)."""
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
        assert isinstance(result_holder[0], ProgrammingError)
    finally:
        conn.close()
