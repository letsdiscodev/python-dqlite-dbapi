"""Pin: ``AsyncCursor.execute`` sets ``_executing_task = cur_task``
INSIDE the protective try-frame, mirroring the sibling
``executemany``'s already-correct discipline.

The bytecode boundary between the assignment and the ``try:`` is
where a ``KeyboardInterrupt`` / ``SystemExit`` (delivered by the
interpreter's signal-eval machinery between any two bytecodes) can
escape with the slot pinned to a now-completed task. A subsequent
``cur.execute(...)`` from any task then trips
``InterfaceError("cursor is already executing in another task")``
on a cursor that is not actually executing — process-lifetime
stuck.

The pin is structural: assert ``try:`` opens before
``self._executing_task = cur_task`` in both ``execute`` and
``executemany``.
"""

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
    """Negative-control: executemany already follows the discipline.
    Pin asserts the in-package reference shape stays intact so a
    future regression that flips executemany also gets fenced."""
    src = _source_of("executemany")
    try_pos = _try_index(src)
    assign_pos = _slot_assign_index(src)
    assert try_pos != -1
    assert assign_pos != -1
    assert try_pos < assign_pos
