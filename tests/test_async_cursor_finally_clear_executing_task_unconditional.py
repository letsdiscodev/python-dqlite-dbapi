"""Pin: ``AsyncCursor.execute`` and ``executemany`` finally clauses
clear ``_executing_task`` UNCONDITIONALLY (no ``is cur_task`` guard).

The clear-side bytecode-tight signal window: a BaseException
delivered between the read of ``self._executing_task`` (the ``is``
check) and the write ``self._executing_task = None`` would leave
the slot pinned to a now-completed task. Always-clearing closes
that window. Safe because ``row_factory`` runs only in fetch* and
never re-enters execute* from the same task.
"""

from __future__ import annotations

import ast
import inspect
import textwrap

from dqlitedbapi.aio import cursor as aio_cursor_mod


def _finally_clears_unconditionally(method: object) -> bool:
    """Return True if the method's finally body unconditionally
    assigns ``self._executing_task = None`` (no surrounding ``If``).
    """
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
