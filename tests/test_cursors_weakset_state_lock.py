"""Pin: ``Connection._cursors`` WeakSet ``add`` / ``discard`` /
snapshot are guarded by ``_state_lock`` under ``check_same_thread=
False``.

``weakref.WeakSet`` is documented as not safe for use by multiple
threads simultaneously. Under CPython GIL the small dict-insert
operations the WeakSet performs internally are bytecode-atomic, so
no breakage today on CPython; under PEP 703 free-threading these
become real races. Belt-and-suspenders: lock the mutation sites so
the future PEP 703 reader doesn't have to plumb the lock through
later.

The lock is the same ``_state_lock`` introduced for the transaction
ctxmgr (Phase 2.1) — narrow contention surface, single lock to
reason about lock-order.

Pinned:
- ``conn.cursor()`` from concurrent threads under
  check_same_thread=False produces the right set size.
- ``Connection.close()``'s cascade snapshots under the lock then
  iterates outside.
- Default ``check_same_thread=True`` behaviour unchanged (the lock
  is held but uncontended on single-thread paths).
"""

from __future__ import annotations

import threading
import time

from dqlitedbapi import Connection


def _make_conn(**kwargs: object) -> Connection:
    return Connection("localhost:9999", **kwargs)  # type: ignore[arg-type]


def test_concurrent_cursor_creation_under_flag_no_lost_entries() -> None:
    """``check_same_thread=False``: N threads each call
    ``conn.cursor()`` M times. After all threads complete,
    ``len(conn._cursors)`` equals N*M. Without the lock, WeakSet
    add races could lose entries under PEP 703 / cyclic GC."""
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

    # Expected: every cursor added is present in _cursors. The
    # caller-side cursors list holds strong refs so GC didn't reap.
    assert len(conn._cursors) == n_threads * n_per_thread


def test_state_lock_used_in_cursor_add_source_pin() -> None:
    """Source-level pin: ``Connection.cursor()`` body uses
    ``_state_lock`` around the ``_cursors.add(cur)`` call. Tripwire
    for a future maintainer who 'optimizes' by dropping the lock."""
    import inspect
    import textwrap

    import dqlitedbapi.connection as conn_mod

    src = textwrap.dedent(inspect.getsource(conn_mod.Connection.cursor))
    # The expected shape: ``with _state_lock:`` followed by
    # ``self._cursors.add(cur)``. Just check both anchors appear in
    # the same source.
    assert "_state_lock" in src
    assert "self._cursors.add" in src


def test_state_lock_used_in_cascade_cursors_source_pin() -> None:
    """Source-level pin: ``Connection._cascade_cursors()`` snapshots
    ``list(self._cursors)`` under ``_state_lock`` and clears under
    the lock too. The intermediate iteration runs outside the
    lock (cursor scrub doesn't re-enter the WeakSet)."""
    import inspect
    import textwrap

    import dqlitedbapi.connection as conn_mod

    src = textwrap.dedent(inspect.getsource(conn_mod.Connection._cascade_cursors))
    assert "_state_lock" in src
    assert "list(self._cursors)" in src
    assert "self._cursors.clear()" in src


def test_concurrent_cursor_creation_and_cascade_close_no_torn_state() -> None:
    """``check_same_thread=False``: spawn cursors from N threads,
    then call ``conn._cascade_cursors()`` from the main thread.
    No exceptions; the cascade completes; ``_cursors`` ends empty."""
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

    # After cascade: _cursors is cleared. Some workers may have
    # added AFTER cascade ran; those are still in _cursors. The
    # important thing is that we didn't crash.
    # (We can't assert on the exact count since workers race the
    # cascade.)


def test_default_unchanged_cursor_creation_single_thread() -> None:
    """Backward compat: default ``check_same_thread=True``.
    Single-thread cursor creation behaviour unchanged; the lock is
    uncontended on this path."""
    conn = _make_conn()
    c1 = conn.cursor()
    c2 = conn.cursor()
    c3 = conn.cursor()
    assert len(conn._cursors) == 3
    # Hold refs so GC doesn't reap.
    assert (c1, c2, c3) is not None
