"""``Cursor.close()`` must be safe to call from a migrated thread.

A ``with conn.cursor() as c:`` body whose work runs in an executor /
``asyncio.to_thread`` may hand control back to ``__exit__`` on a
worker thread. Close deliberately omits the Connection thread-affinity
check so the cleanup does not raise ``ProgrammingError`` and mask the
body's original exception. Matches stdlib ``sqlite3.Cursor.close``.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

from dqlitedbapi.cursor import Cursor


def test_close_from_non_creator_thread_does_not_raise() -> None:
    conn = MagicMock()
    # Wire the thread-check to raise just like Connection._check_thread
    # would on a cross-thread call; close() must NOT invoke it.
    conn._check_thread.side_effect = AssertionError("close() must not call _check_thread")
    conn.messages = []
    cur = Cursor(conn)

    errors: list[BaseException] = []

    def run() -> None:
        try:
            cur.close()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    t = threading.Thread(target=run)
    t.start()
    t.join()
    assert errors == []
    assert cur._closed is True
    conn._check_thread.assert_not_called()


def test_close_is_idempotent() -> None:
    conn = MagicMock()
    conn.messages = []
    cur = Cursor(conn)
    cur.close()
    cur.close()  # must not raise
    assert cur._closed is True
