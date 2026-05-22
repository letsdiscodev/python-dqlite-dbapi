"""Pin: ``AsyncCursor.executemany`` clears ``_executing_task`` on
validation-rejected raise paths.

Previously the slot was set BEFORE the verb-reject / PRAGMA-reject /
row-returning-reject checks; a validation-rejected executemany pinned
``_executing_task`` to a completed task, then a cross-task
``cur.execute(...)`` observed the stale slot and raised
``InterfaceError("cursor is already executing in another task")`` on
a cursor that was NOT actually executing.

Sibling ``execute()`` already scoped the slot inside the try/finally
so any non-success exit path cleared it; this pin enforces the same
discipline on ``executemany``.
"""

from __future__ import annotations

import asyncio
import os
import threading
from typing import Any, cast

import pytest

import dqlitedbapi
import dqlitedbapi.aio
from dqlitedbapi.exceptions import ProgrammingError


def _bare_async_cursor() -> Any:
    aconn = cast(Any, dqlitedbapi.aio.AsyncConnection.__new__(dqlitedbapi.aio.AsyncConnection))
    aconn._closed = False
    aconn._creator_pid = os.getpid()
    aconn._loop_ref = None
    aconn._creator_thread = threading.get_ident()
    acur = cast(Any, dqlitedbapi.aio.AsyncCursor.__new__(dqlitedbapi.aio.AsyncCursor))
    acur._closed = False
    acur._connection = aconn
    acur._executing_task = None
    acur.messages = []
    # ``_completed_iterations`` is snapshotted in ``executemany``
    # before ``_reset_execute_state`` runs; seed so the bare-cursor
    # fixture supports the validation-reject paths.
    acur._completed_iterations = 0
    return acur


async def test_executemany_select_reject_clears_executing_task() -> None:
    """SELECT is rejected by the row-returning guard. The slot must
    be cleared so a subsequent operation on the cursor doesn't trip
    the "cursor is already executing" guard."""
    cur = _bare_async_cursor()
    assert cur._executing_task is None
    with pytest.raises(ProgrammingError, match="DML statements"):
        await cur.executemany("SELECT 1", [(1,), (2,)])
    assert cur._executing_task is None, (
        "_executing_task must be cleared after a validation-rejected executemany "
        f"so subsequent execute() doesn't trip the cross-task guard; "
        f"got {cur._executing_task!r}"
    )


async def test_executemany_pragma_reject_clears_executing_task() -> None:
    cur = _bare_async_cursor()
    with pytest.raises(ProgrammingError, match="PRAGMA"):
        await cur.executemany("PRAGMA foo", [(1,), (2,)])
    assert cur._executing_task is None


async def test_executemany_begin_reject_clears_executing_task() -> None:
    """Verb-reject path (BEGIN is in _EXECUTEMANY_REJECT_VERBS)."""
    cur = _bare_async_cursor()
    with pytest.raises(ProgrammingError, match="BEGIN"):
        await cur.executemany("BEGIN", [()])
    assert cur._executing_task is None


async def test_executemany_none_seq_clears_executing_task() -> None:
    """``None`` for seq_of_parameters is rejected before the slot-set
    body. Belt-and-braces — the slot must be None at exit."""
    cur = _bare_async_cursor()
    with pytest.raises(ProgrammingError, match="None"):
        await cur.executemany("INSERT INTO t VALUES (?)", None)
    assert cur._executing_task is None


async def test_subsequent_execute_after_rejected_executemany_does_not_trip_cross_task_guard() -> (
    None
):
    """Behavioural pin: after a validation-rejected executemany, a
    sibling task can call cur.execute() without tripping
    ``InterfaceError("cursor is already executing")``."""
    cur = _bare_async_cursor()

    # Validation-reject first.
    with pytest.raises(ProgrammingError):
        await cur.executemany("SELECT 1", [(1,)])

    # Sibling task / continuation calls execute. With the bug,
    # _executing_task was pinned to the original task; this would
    # raise InterfaceError on the cross-task observation. With the
    # fix, the slot is None so a fresh execute proceeds.
    captured: list[BaseException] = []

    async def _sibling() -> None:
        try:
            # Manually mimic the cross-task check at execute() entry —
            # we don't actually run the wire roundtrip (no transport).
            cur_task = asyncio.current_task()
            if cur._executing_task is not None and cur._executing_task is not cur_task:
                from dqlitedbapi.exceptions import InterfaceError as _IfaceErr

                raise _IfaceErr("cursor is already executing in another task")
        except BaseException as e:
            captured.append(e)

    await asyncio.create_task(_sibling())
    assert captured == [], f"sibling task should not see the cross-task guard fire; got {captured}"
