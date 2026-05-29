"""_next_row_unlocked snapshots _rows/_row_index/_row_factory to locals
before the bounds check so a foreign-thread force_close cascade rewriting
self._rows = [] between check and indexed read cannot leak a bare
IndexError. Pinned both via an AST check (reads self._rows exactly once)
and a Barrier-synchronised race."""

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
    """_next_row_unlocked must read self._rows exactly once; a second read
    is the TOCTOU partner of the cascade's self._rows = [] write."""
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
    """Simulate the cascade writing self._rows = [] between the bounds
    check and the indexed read; the snapshot must keep the read clean."""
    cur = _make_cursor([(1,), (2,), (3,)])

    barrier = threading.Barrier(2)
    errors: list[BaseException] = []
    delivered: list[Any] = []

    def reader() -> None:
        try:
            barrier.wait()
            row = cur._next_row_unlocked()
            delivered.append(row)
        except BaseException as exc:  # noqa: BLE001 — pin behaviour
            errors.append(exc)

    def canceller() -> None:
        barrier.wait()
        # Cascade-equivalent: scrub rows + index from the sibling thread.
        cur._rows = []
        cur._row_index = 0

    t_read = threading.Thread(target=reader)
    t_cancel = threading.Thread(target=canceller)
    t_read.start()
    t_cancel.start()
    t_read.join()
    t_cancel.join()

    # Reader delivered the first row (snapshot won) or saw the empty list
    # and returned None — both clean, neither raises IndexError.
    assert not errors, f"reader raised: {errors!r}"
    assert delivered == [(1,)] or delivered == [None]
