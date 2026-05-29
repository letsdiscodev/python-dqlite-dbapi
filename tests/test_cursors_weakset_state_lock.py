"""``Connection._cursors`` WeakSet add/discard/snapshot are guarded by ``_state_lock``
under ``check_same_thread=False``.

``weakref.WeakSet`` is not documented thread-safe; CPython's GIL hides this today but
PEP 703 free-threading turns the WeakSet mutations into real races, so lock the sites now.
"""

from __future__ import annotations

import threading
import time

from dqlitedbapi import Connection


def _make_conn(**kwargs: object) -> Connection:
    return Connection("localhost:9999", **kwargs)  # type: ignore[arg-type]


def test_concurrent_cursor_creation_under_flag_no_lost_entries() -> None:
    """N*M concurrent ``conn.cursor()`` calls all land in ``_cursors`` (no lost entries)."""
    conn = _make_conn(check_same_thread=False)
    n_threads = 10
    n_per_thread = 50
    cursors: list[object] = []
    cursors_lock = threading.Lock()

    def worker() -> None:
        for _ in range(n_per_thread):
            cur = conn.cursor()
            with cursors_lock:
                cursors.append(cur)  # hold a ref so GC doesn't reap

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(conn._cursors) == n_threads * n_per_thread


def test_state_lock_used_in_cursor_add_source_pin() -> None:
    """Source pin: ``Connection.cursor()`` uses ``_state_lock`` around ``_cursors.add``."""
    import inspect
    import textwrap

    import dqlitedbapi.connection as conn_mod

    src = textwrap.dedent(inspect.getsource(conn_mod.Connection.cursor))
    assert "_state_lock" in src
    assert "self._cursors.add" in src


def test_state_lock_used_in_cascade_cursors_source_pin() -> None:
    """Source pin: ``_cascade_cursors()`` snapshots and clears ``_cursors`` under
    ``_state_lock``; the intermediate iteration runs outside the lock."""
    import inspect
    import textwrap

    import dqlitedbapi.connection as conn_mod

    src = textwrap.dedent(inspect.getsource(conn_mod.Connection._cascade_cursors))
    assert "_state_lock" in src
    assert "list(self._cursors)" in src
    assert "self._cursors.clear()" in src


def test_concurrent_cursor_creation_and_cascade_close_no_torn_state() -> None:
    """Cascade-close while N threads spawn cursors: no exceptions, no torn state."""
    conn = _make_conn(check_same_thread=False)
    cursors: list[object] = []
    cursors_lock = threading.Lock()
    stop = threading.Event()

    def worker() -> None:
        while not stop.is_set():
            cur = conn.cursor()
            with cursors_lock:
                cursors.append(cur)
            time.sleep(0.001)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    time.sleep(0.02)
    # Cascade from main thread while workers are still adding.
    conn._cascade_cursors()
    stop.set()
    for t in threads:
        t.join()

    # No count assertion: workers may add after the cascade runs.


def test_default_unchanged_cursor_creation_single_thread() -> None:
    """Backward compat: default ``check_same_thread=True`` path unchanged."""
    conn = _make_conn()
    c1 = conn.cursor()
    c2 = conn.cursor()
    c3 = conn.cursor()
    assert len(conn._cursors) == 3
    # Hold refs so GC doesn't reap.
    assert (c1, c2, c3) is not None
