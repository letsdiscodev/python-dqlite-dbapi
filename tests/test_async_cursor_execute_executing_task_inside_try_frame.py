"""``execute`` sets ``_executing_task = cur_task`` INSIDE the try-frame: a KeyboardInterrupt /
SystemExit on the assignment/``try:`` bytecode boundary could otherwise leave the slot pinned to a
completed task, wedging later ``execute()`` calls with a bogus "already executing" error."""

from __future__ import annotations

import inspect

from dqlitedbapi.aio import cursor as aio_cursor_module


def _source_of(name: str) -> str:
    obj = getattr(aio_cursor_module.AsyncCursor, name)
    return inspect.getsource(obj)


def _try_index(src: str) -> int:
    return src.find("try:")


def _slot_assign_index(src: str) -> int:
    return src.find("self._executing_task = cur_task")


def test_execute_executing_task_assignment_inside_try() -> None:
    src = _source_of("execute")
    try_pos = _try_index(src)
    assign_pos = _slot_assign_index(src)
    assert try_pos != -1, "execute() must contain a try-block"
    assert assign_pos != -1, "execute() must assign self._executing_task = cur_task"
    assert try_pos < assign_pos, (
        "AsyncCursor.execute() must set self._executing_task INSIDE the "
        "try-frame so a KeyboardInterrupt / SystemExit landing on the "
        "bytecode boundary cannot leave the slot pinned to a completed "
        "task. The sibling executemany() already follows this discipline "
        "(commit 28a3e44)."
    )


def test_executemany_executing_task_assignment_inside_try() -> None:
    """executemany already follows the discipline; pin it so a regression that flips it fails."""
    src = _source_of("executemany")
    try_pos = _try_index(src)
    assign_pos = _slot_assign_index(src)
    assert try_pos != -1
    assert assign_pos != -1
    assert try_pos < assign_pos
