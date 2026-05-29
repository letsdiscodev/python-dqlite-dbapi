"""Sync commit/rollback order ``_check_thread()`` first so cross-thread misuse cannot
mutate the owner thread's ``messages`` before the thread-affinity diagnostic fires."""

from __future__ import annotations

import threading

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import ProgrammingError


@pytest.mark.parametrize("op", ["commit", "rollback"])
def test_sync_op_on_closed_from_foreign_thread_raises_thread_affinity(op: str) -> None:
    """Foreign-thread commit/rollback on a closed conn raises ProgrammingError (thread
    check precedes closed check), not InterfaceError."""
    c = dqlitedbapi.connect("127.0.0.1:9999")
    c._closed = True
    c._closed_flag[0] = True

    captured: list[BaseException] = []

    def worker() -> None:
        try:
            getattr(c, op)()
        except BaseException as e:
            captured.append(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert len(captured) == 1
    assert isinstance(captured[0], ProgrammingError), (
        f"sync Connection.{op}() from a foreign thread (even on closed "
        f"conn) must raise ProgrammingError (thread-affinity precedence); "
        f"got {type(captured[0]).__name__}: {captured[0]}"
    )
