"""Pin: sync ``Cursor._next_row_unlocked`` snapshots ``_rows`` /
``_row_index`` / ``_row_factory`` to locals before the bounds check
so a foreign-thread ``force_close_transport`` cascade that rewrites
``self._rows = []`` between the bounds check and the indexed read
cannot turn the indexed read into a bare ``IndexError`` that escapes
the ``dbapi.Error`` hierarchy.

The local-snapshot is preferable to a try/except wrap: it makes the
read atomic w.r.t. the cascade (the snapshot is consistent; either
we deliver the pre-cascade row or we observe the post-cascade empty
list). The next ``fetchone`` after a delivered stale row observes
``_closed`` via the prelude check and raises ``InterfaceError``
cleanly.

We pin two surfaces:

1. A direct AST-level pin that ``_next_row_unlocked`` reads
   ``self._rows`` exactly once (a regression to two reads brings
   back the TOCTOU race).
2. A behavioural pin that simulates the cascade racing the bounds
   check via a ``Barrier``-synchronised sibling thread.
"""

from __future__ import annotations

import ast
import inspect
import textwrap
import threading
from typing import Any
from unittest.mock import MagicMock

from dqlitedbapi.cursor import Cursor


def _make_cursor(rows: list[tuple[Any, ...]]) -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._description = (("col", 4, None, None, None, None, None),)
    cur._rowcount = len(rows)
    cur._lastrowid = None
    cur._row_factory = None
    cur._rows = list(rows)
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    cur._connection = MagicMock()
    cur._connection._closed = False
    cur._connection._check_thread = lambda: None
    return cur


def test_next_row_unlocked_snapshots_self_rows_to_local() -> None:
    """AST-level pin: ``_next_row_unlocked`` must read ``self._rows``
    exactly once, never twice (the second read is the TOCTOU race
    partner with ``_cascade_cursors``' ``self._rows = []`` write).
    """
    src = textwrap.dedent(inspect.getsource(Cursor._next_row_unlocked))
    tree = ast.parse(src)
    self_rows_reads = 0
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and node.attr == "_rows"
            and isinstance(node.value, ast.Name)
            and node.value.id == "self"
            and isinstance(node.ctx, ast.Load)
        ):
            self_rows_reads += 1
    assert self_rows_reads == 1, (
        f"_next_row_unlocked reads self._rows {self_rows_reads} times; "
        "must snapshot to a local exactly once to avoid the cascade-race "
        "IndexError"
    )


def test_next_row_unlocked_race_with_cascade_empty_rows() -> None:
    """Behavioural pin: simulate the cascade writing
    ``self._rows = []`` between the bounds check and the indexed read.

    Before the fix: bare ``IndexError`` escapes.
    After the fix: snapshot delivers the row cleanly (the cascade's
    effect is observed on the NEXT call via ``_check_closed``).
    """
    cur = _make_cursor([(1,), (2,), (3,)])

    # Drive the cursor manually through the path: snapshot happens
    # first, then if the cascade fires the snapshot still points at
    # the pre-cascade list. After delivering the row the test then
    # simulates the cascade-completed state by reading the next row
    # AFTER the cascade swap.
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []
    delivered: list[Any] = []

    def reader() -> None:
        try:
            # Sync with the canceller before we enter the helper; the
            # helper runs without yielding the GIL between snapshot
            # and indexed read, so the snapshot semantics are what
            # protects us against the cascade landing first.
            barrier.wait()
            row = cur._next_row_unlocked()
            delivered.append(row)
        except BaseException as exc:  # noqa: BLE001 — pin behaviour
            errors.append(exc)

    def canceller() -> None:
        barrier.wait()
        # Cascade-equivalent: scrub rows + index from the sibling
        # thread. Even with a sub-microsecond reader head start, the
        # snapshot guarantees the indexed read does not race.
        cur._rows = []
        cur._row_index = 0

    t_read = threading.Thread(target=reader)
    t_cancel = threading.Thread(target=canceller)
    t_read.start()
    t_cancel.start()
    t_read.join()
    t_cancel.join()

    # The reader either delivered the first row (snapshot won) or
    # observed the post-cascade empty list and returned None. Both
    # are clean outcomes; neither raises IndexError.
    assert not errors, f"reader raised: {errors!r}"
    assert delivered == [(1,)] or delivered == [None]
