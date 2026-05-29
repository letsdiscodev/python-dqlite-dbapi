"""``Cursor.close()`` is safe from a migrated thread: it omits the
Connection thread-affinity check so cleanup can't raise and mask a body
exception (e.g. when ``__exit__`` runs on an executor worker thread).
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

from dqlitedbapi.cursor import Cursor


def test_close_from_non_creator_thread_does_not_raise() -> None:
    conn = MagicMock()
    # close() must not invoke _check_thread; make it raise if it does.
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
    cur.close()
    assert cur._closed is True
