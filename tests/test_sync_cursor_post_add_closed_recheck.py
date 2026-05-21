"""Pin: sync ``Connection.cursor()`` re-checks ``_closed`` AFTER
adding the new ``Cursor`` to ``self._cursors`` and runs the cascade
scrub if the flag flipped between the prelude check and the WeakSet
add. Mirrors the async sibling's post-add re-check at
``aio/connection.py``.

The defect this pins guards against: ``force_close_transport`` is
documented as callable from finalize threads / signal handlers /
SA-pool reclaim threads (no ``_check_thread`` / no ``_op_lock``
acquire). The cascade snapshot ``list(self._cursors)`` can run on a
sibling thread between this method's prelude check and the WeakSet
add — the freshly-built cursor skips the cascade scrub and is
returned to the caller with ``_closed=False`` and a strong ref to
the now-dead Connection.

The pin tests the deterministic interleaving by directly mutating
``_closed`` between the prelude and the add. The race-stress is left
as a CI exercise (probabilistic; the deterministic mock-based pin
is what protects the discipline.)
"""

from __future__ import annotations

import os
import threading
import weakref

from dqlitedbapi.connection import Connection


def _stub_connection() -> Connection:
    """Minimal ``Connection`` shape sufficient for the ``cursor()``
    path: no ``_ensure_loop`` invocation, no wire I/O."""
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
    """Directly drive the interleaving by flipping ``_closed`` to
    ``True`` from a sibling thread between the prelude check and
    the add. Simulated via subclass override of ``_check_thread``
    that flips the flag mid-call — the post-add recheck must then
    scrub the cursor and discard it from the WeakSet."""

    class _RaceConnection(Connection):
        """Subclass that flips ``_closed`` to True during
        ``_check_thread``, simulating a sibling-thread
        ``force_close_transport`` landing between the prelude
        ``if self._closed:`` check and the WeakSet add."""

        def _check_thread(self) -> None:
            # Flip ``_closed`` to mimic the cross-thread race
            # window the post-add recheck guards.
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

    # Post-add recheck must have fired:
    # (a) the cursor's ``_closed`` is True (cascade scrub applied);
    assert cur._closed is True, (
        "post-add recheck did not flip cursor._closed; the freshly-"
        "built cursor escaped the cascade and is usable against a "
        "closed Connection — CWE-672-class hazard."
    )
    # (b) ``_cursors`` is empty (entry discarded so the cascade
    # postcondition holds);
    assert len(conn._cursors) == 0, (
        f"post-add recheck did not discard the late-added cursor — "
        f"_cursors postcondition (empty after close) is violated, "
        f"len={len(conn._cursors)}"
    )
    # (c) ``_connection`` is a weakref proxy (no strong ref pinning
    # the dead Connection's loop/thread state alive).
    assert isinstance(cur._connection, weakref.ProxyTypes), (
        "post-add recheck did not swap ``cur._connection`` to a "
        "weakref proxy; the leaked cursor strong-pins the dead "
        "Connection's loop / thread / async wrapper."
    )
    # (d) cascade-scrub fields are zeroed.
    assert cur._rows == []
    assert cur._description is None
    assert cur._rowcount == -1
    assert cur._lastrowid is None
    assert cur._row_index == 0


def test_cursor_post_add_recheck_noop_in_normal_path() -> None:
    """Negative pin: when ``_closed`` does NOT flip mid-call, the
    post-add recheck arm is not taken and ``cursor()`` returns a
    normally-tracked, open cursor."""
    conn = _stub_connection()
    cur = conn.cursor()
    assert cur._closed is False
    assert cur in conn._cursors
    # No weakref-proxy swap.
    assert not isinstance(cur._connection, weakref.ProxyTypes)


def test_cursor_post_add_recheck_mirrors_async_sibling_shape() -> None:
    """Source-level pin: the sync ``cursor()`` body contains the same
    discipline as the async sibling — the re-check arm references
    ``_closed``, the scrubbed fields, and the ``weakref.proxy`` swap
    that the async sibling uses. Anchors the sibling-parity claim so
    a future divergence surfaces at review time, not in production."""
    import inspect

    src = inspect.getsource(Connection.cursor)
    # The defining markers of the post-add recheck arm.
    assert "if self._closed:" in src
    assert "self._cursors.discard(cur)" in src
    assert "weakref.proxy(cur._connection)" in src
    # The scrub fields the async sibling sets must all be present.
    for marker in ("cur._closed = True", "cur._rows = []", "cur._description = None"):
        assert marker in src, (
            f"sync cursor() post-add recheck no longer sets {marker!r}; "
            f"sibling-parity with aio/connection.py drifted."
        )
