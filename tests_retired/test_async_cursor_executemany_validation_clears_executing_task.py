"""``executemany`` clears ``_executing_task`` on validation-rejected raise paths, so a stale slot
doesn't make a later cross-task ``execute()`` raise a bogus "already executing" error."""

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
    # Snapshotted in executemany before _reset_execute_state; seed for the validation-reject paths.
    acur._completed_iterations = 0
    return acur


async def test_executemany_select_reject_clears_executing_task() -> None:
    """SELECT is rejected by the row-returning guard; the slot must still be cleared."""
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
    """``None`` seq_of_parameters is rejected before the slot-set body; slot stays None at exit."""
    cur = _bare_async_cursor()
    with pytest.raises(ProgrammingError, match="None"):
        await cur.executemany("INSERT INTO t VALUES (?)", None)
    assert cur._executing_task is None


async def test_subsequent_execute_after_rejected_executemany_does_not_trip_cross_task_guard() -> (
    None
):
    """After a validation-rejected executemany, a sibling execute() must not trip the cross-task
    "already executing" guard."""
    cur = _bare_async_cursor()

    with pytest.raises(ProgrammingError):
        await cur.executemany("SELECT 1", [(1,)])

    captured: list[BaseException] = []

    async def _sibling() -> None:
        try:
            # Mimic the cross-task check at execute() entry; no wire roundtrip (no transport).
            cur_task = asyncio.current_task()
            if cur._executing_task is not None and cur._executing_task is not cur_task:
                from dqlitedbapi.exceptions import InterfaceError as _IfaceErr

                raise _IfaceErr("cursor is already executing in another task")
        except BaseException as e:
            captured.append(e)

    await asyncio.create_task(_sibling())
    assert captured == [], f"sibling task should not see the cross-task guard fire; got {captured}"
