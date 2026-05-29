"""Sync ``Connection.cursor()`` re-checks ``_closed`` after adding the Cursor to
``self._cursors`` and scrubs it if the flag flipped: ``force_close_transport`` (callable
from finalize/signal/pool threads with no lock) can land between the prelude check and
the WeakSet add, otherwise leaving a usable cursor on a dead Connection."""

from __future__ import annotations

import os
import threading
import weakref

from dqlitedbapi.connection import Connection


def _stub_connection() -> Connection:
    """Minimal Connection shape for the ``cursor()`` path: no ``_ensure_loop``, no wire I/O."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._cursors = weakref.WeakSet()
    conn.messages = []
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn._async_conn = None
    return conn


def test_cursor_post_add_recheck_scrubs_late_added_cursor() -> None:
    """Flip ``_closed`` mid-call (via ``_check_thread`` override) to simulate a
    sibling-thread close between the prelude check and the add; the post-add recheck
    must scrub the cursor and discard it from the WeakSet."""

    class _RaceConnection(Connection):
        def _check_thread(self) -> None:
            self._closed = True

    conn = _RaceConnection.__new__(_RaceConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._cursors = weakref.WeakSet()
    conn.messages = []
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn._async_conn = None

    cur = conn.cursor()

    assert cur._closed is True, (
        "post-add recheck did not flip cursor._closed; the freshly-"
        "built cursor escaped the cascade and is usable against a "
        "closed Connection — CWE-672-class hazard."
    )
    assert len(conn._cursors) == 0, (
        f"post-add recheck did not discard the late-added cursor — "
        f"_cursors postcondition (empty after close) is violated, "
        f"len={len(conn._cursors)}"
    )
    # No weakref-proxy swap: PEP 249 §6.5.1 requires ``cursor.connection is conn``
    # identity (and proxies are unhashable).
    assert cur._connection is conn, (
        "post-add recheck must preserve ``cursor.connection is conn`` "
        "identity per PEP 249 §6.5.1; observed swap or replacement."
    )
    assert cur._rows == []
    assert cur._description is None
    assert cur._rowcount == -1
    assert cur._lastrowid is None
    assert cur._row_index == 0
    assert hash(cur._connection) is not None


def test_cursor_post_add_recheck_noop_in_normal_path() -> None:
    """When ``_closed`` does not flip mid-call, cursor() returns a normally-tracked cursor."""
    conn = _stub_connection()
    cur = conn.cursor()
    assert cur._closed is False
    assert cur in conn._cursors
    assert not isinstance(cur._connection, weakref.ProxyTypes)


def test_cursor_post_add_recheck_mirrors_async_sibling_shape() -> None:
    """Source pin: sync ``cursor()`` has the scrub+discard re-check arm but, unlike the
    async sibling, no ``weakref.proxy`` swap (to keep PEP 249 §6.5.1 identity)."""
    import inspect

    src = inspect.getsource(Connection.cursor)
    assert "if self._closed:" in src
    assert "self._cursors.discard(cur)" in src
    for marker in ("cur._closed = True", "cur._rows = []", "cur._description = None"):
        assert marker in src, (
            f"sync cursor() post-add recheck no longer sets {marker!r}; "
            f"sibling-parity with aio/connection.py drifted."
        )
    # The proxy swap must NOT be present on this arm (PEP 249 §6.5.1 identity).
    assert (
        "weakref.proxy(cur._connection)"
        not in src.split("self._cursors.discard(cur)")[0].split("if self._closed:")[-1]
    ), (
        "sync cursor() post-add recheck must NOT swap cur._connection "
        "to a weakref.proxy; the scrub + discard already protects the "
        "cursor and the swap breaks PEP 249 §6.5.1 identity."
    )
