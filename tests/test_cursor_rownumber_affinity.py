"""Pin: ``Cursor.rownumber`` / ``AsyncCursor.rownumber`` enforce
thread / loop affinity, mirroring the ``in_transaction`` precedent.

Both getters read mutable cursor state (``_row_index``) that is
mutated by ``fetchone``/``fetchmany`` on the creator thread / bound
loop. A foreign-thread / foreign-loop reader could observe a
mid-mutation value (off-by-one race window).
"""

from __future__ import annotations

import threading

import dqlitedbapi
from dqlitedbapi.cursor import Cursor


def test_sync_rownumber_runs_thread_affinity_check() -> None:
    """Cross-thread ``cur.rownumber`` raises ``ProgrammingError``
    matching the connection-level ``in_transaction`` precedent."""
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
    """Positive control: same-thread read returns ``None`` (no
    result set active)."""
    conn = dqlitedbapi.Connection("127.0.0.1:9999")
    try:
        cur = conn.cursor()
        assert cur.rownumber is None
    finally:
        conn._closed = True


def test_sync_rownumber_on_partial_init_does_not_crash() -> None:
    """A hand-built ``Cursor.__new__`` fixture without a
    ``_connection`` slot must not crash the getter."""
    cur = Cursor.__new__(Cursor)
    cur._description = None
    cur._row_index = 0
    # No _connection set.
    assert cur.rownumber is None
