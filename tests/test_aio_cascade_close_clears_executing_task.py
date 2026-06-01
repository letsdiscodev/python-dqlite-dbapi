"""Cascade-close must null _executing_task like AsyncCursor.close(), else __aenter__'s
single-flight check raises a misleading "already executing" on a cascade-closed cursor.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dqlitedbapi.aio.connection import _cascade_cursors_closed
from dqlitedbapi.aio.cursor import AsyncCursor


def _make_cursor_executing() -> AsyncCursor:
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._rows = [("x",)]
    cur._description = (("c", 1, None, None, None, None, None),)
    cur._rowcount = 1
    cur._lastrowid = None
    cur._row_index = 0
    cur._arraysize = 1
    cur._row_factory = None
    cur._completed_iterations = 0
    cur.messages = []
    cur._executing_task = object()  # type: ignore[assignment]  # a sibling task is parked mid-execute
    cur._connection = MagicMock()
    return cur


def test_cascade_close_clears_executing_task() -> None:
    cur = _make_cursor_executing()
    _cascade_cursors_closed([cur])
    assert cur._closed is True
    assert cur._executing_task is None, (
        "cascade-close must null _executing_task (like AsyncCursor.close), else __aenter__ "
        "reports 'already executing' on a closed cursor"
    )
