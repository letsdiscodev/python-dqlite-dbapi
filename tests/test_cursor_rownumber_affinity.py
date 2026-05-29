"""``Cursor.rownumber`` / ``AsyncCursor.rownumber`` enforce thread/loop affinity.

The getters read ``_row_index``, mutated by fetch on the creator thread/loop; a
foreign reader could observe a mid-mutation value.
"""

from __future__ import annotations

import threading

import dqlitedbapi
from dqlitedbapi.cursor import Cursor


def test_sync_rownumber_runs_thread_affinity_check() -> None:
    conn = dqlitedbapi.Connection("127.0.0.1:9999")
    try:
        cur = conn.cursor()
        captured: list[BaseException] = []

        def worker() -> None:
            try:
                _ = cur.rownumber
            except BaseException as e:  # noqa: BLE001
                captured.append(e)

        t = threading.Thread(target=worker)
        t.start()
        t.join(timeout=2.0)

        assert captured, "expected an exception from the foreign-thread call"
        assert isinstance(captured[0], dqlitedbapi.ProgrammingError), (
            f"expected ProgrammingError, got {type(captured[0]).__name__}: {captured[0]}"
        )
        assert "thread" in str(captured[0]).lower()
    finally:
        conn._closed = True


def test_sync_rownumber_same_thread_works() -> None:
    conn = dqlitedbapi.Connection("127.0.0.1:9999")
    try:
        cur = conn.cursor()
        assert cur.rownumber is None
    finally:
        conn._closed = True


def test_sync_rownumber_on_partial_init_does_not_crash() -> None:
    """Getter must not crash when ``_connection`` is unset."""
    cur = Cursor.__new__(Cursor)
    cur._description = None
    cur._row_index = 0
    assert cur.rownumber is None
