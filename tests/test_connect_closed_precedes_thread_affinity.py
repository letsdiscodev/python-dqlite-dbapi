"""Pin: ``Connection.connect()`` precedence is thread-affinity, then closed-state,
then the ``messages`` clear, so a foreign-thread caller can't mutate the owner's
``messages`` list before the diagnostic fires. Mirrors the async sibling.
"""

from __future__ import annotations

import threading
from typing import Any

from dqlitedbapi import Connection
from dqlitedbapi.exceptions import InterfaceError, ProgrammingError


def test_connect_on_closed_from_creator_thread_raises_interface_error() -> None:
    """On the creator thread, closed-state surfaces as InterfaceError."""
    conn = Connection("localhost:9001", timeout=2.0)
    conn.close()
    try:
        conn.connect()
    except InterfaceError as e:
        assert "closed" in str(e).lower()
    else:
        raise AssertionError("expected InterfaceError on closed connect()")


def test_connect_from_other_thread_raises_thread_affinity_first() -> None:
    """Foreign-thread caller gets the thread-affinity ProgrammingError even when
    closed: the thread check runs before the closed-state check."""
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
    """Open conn from a foreign thread still raises the thread-affinity diagnostic."""
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
