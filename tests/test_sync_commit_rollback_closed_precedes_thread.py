"""Pin: sync ``Connection.commit`` and ``Connection.rollback`` check
``_closed`` BEFORE ``_check_thread()``, mirroring the async siblings
and stdlib ``sqlite3``'s closed-first precedence. Cross-thread call
on a closed connection should surface ``InterfaceError("Connection
is closed")`` — the more salient diagnostic — not
``ProgrammingError("Connection objects ... must be used in same
thread...")``.
"""

from __future__ import annotations

import threading

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import InterfaceError


@pytest.mark.parametrize("op", ["commit", "rollback"])
def test_sync_op_on_closed_from_foreign_thread_raises_interface_error(op: str) -> None:
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
    assert isinstance(captured[0], InterfaceError), (
        f"sync Connection.{op}() on a closed connection from a foreign thread "
        f"must raise InterfaceError (closed-state precedence per stdlib + "
        f"async sibling); got {type(captured[0]).__name__}: {captured[0]}"
    )
