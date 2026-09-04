"""PEP 249 idempotent close: a second close from any thread is a no-op. The
closed-state short-circuit must run BEFORE the cross-thread guard."""

from __future__ import annotations

import threading

from dqlitedbapi import Connection
from dqlitedbapi.exceptions import ProgrammingError


def test_second_close_from_other_thread_is_noop() -> None:
    conn = Connection("localhost:9001")
    conn.close()

    error: list[BaseException] = []

    def cross_thread_reclose() -> None:
        try:
            conn.close()
        except BaseException as exc:
            error.append(exc)

    t = threading.Thread(target=cross_thread_reclose)
    t.start()
    t.join(timeout=5.0)
    assert not error, f"unexpected exception from cross-thread reclose: {error[0]!r}"


def test_first_close_from_other_thread_still_raises() -> None:
    """The thread guard still applies to the FIRST close (tears down non-thread-safe
    loop primitives)."""
    conn = Connection("localhost:9001")
    error: list[BaseException] = []

    def cross_thread_first_close() -> None:
        try:
            conn.close()
        except BaseException as exc:
            error.append(exc)

    t = threading.Thread(target=cross_thread_first_close)
    t.start()
    t.join(timeout=5.0)
    # Mark closed so the fixture doesn't hang on the background loop thread.
    conn._closed = True
    assert len(error) == 1
    assert isinstance(error[0], ProgrammingError)


def test_repeated_close_on_same_thread_remains_idempotent() -> None:
    conn = Connection("localhost:9001")
    conn.close()
    conn.close()
    conn.close()
