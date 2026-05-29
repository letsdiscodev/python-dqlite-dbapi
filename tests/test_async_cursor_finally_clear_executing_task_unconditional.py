"""execute/executemany finally clauses clear ``_executing_task`` unconditionally.

A guarded clear leaves a BaseException window between the ``is`` check and the
write that would pin the slot to a completed task.
"""

from __future__ import annotations

import ast
import inspect
import textwrap

from dqlitedbapi.aio import cursor as aio_cursor_mod


def _finally_clears_unconditionally(method: object) -> bool:
    """True if the finally body unconditionally assigns ``self._executing_task``."""
    src = textwrap.dedent(inspect.getsource(method))  # type: ignore[arg-type]
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.Try):
            for stmt in node.finalbody:
                if isinstance(stmt, ast.Assign):
                    for tgt in stmt.targets:
                        if isinstance(tgt, ast.Attribute) and tgt.attr == "_executing_task":
                            return True
    return False


def test_async_execute_finally_clears_executing_task_unconditionally() -> None:
    assert _finally_clears_unconditionally(aio_cursor_mod.AsyncCursor.execute), (
        "AsyncCursor.execute must clear _executing_task unconditionally "
        "in its finally clause to close the bytecode-tight signal window."
    )


def test_async_executemany_finally_clears_executing_task_unconditionally() -> None:
    assert _finally_clears_unconditionally(aio_cursor_mod.AsyncCursor.executemany), (
        "AsyncCursor.executemany must clear _executing_task unconditionally "
        "in its finally clause to close the bytecode-tight signal window."
    )
