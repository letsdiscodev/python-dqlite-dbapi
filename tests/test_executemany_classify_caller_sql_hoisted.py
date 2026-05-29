"""Sync ``_executemany_async`` runs ``_classify_caller_sql`` once at the top, not
per-iteration: re-parsing invariant SQL per row would be O(N * |SQL|)."""

from __future__ import annotations

import ast
import inspect
import textwrap

from dqlitedbapi import cursor as sync_cur_mod


def test_executemany_async_calls_classifier_once_at_top() -> None:
    """``_classify_caller_sql`` is referenced at function scope, not inside the for loop."""
    src = textwrap.dedent(inspect.getsource(sync_cur_mod.Cursor._executemany_async))
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.For):
            for sub in ast.walk(node):
                if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name):
                    assert sub.func.id != "_classify_caller_sql", (
                        "_executemany_async must NOT call _classify_caller_sql "
                        "inside its per-iteration loop — hoist to function scope."
                    )

    # Body must reference _classify_caller_sql before/outside the For.
    fn_def = tree.body[0]
    assert isinstance(fn_def, ast.AsyncFunctionDef | ast.FunctionDef)
    fn_body = fn_def.body
    found_at_top = False
    for stmt in fn_body:
        if isinstance(stmt, ast.For):
            break
        for sub in ast.walk(stmt):
            if (
                isinstance(sub, ast.Call)
                and isinstance(sub.func, ast.Name)
                and sub.func.id in ("_classify_caller_sql", "_strip_sql_noise")
            ):
                found_at_top = True
                break
    assert found_at_top, (
        "_executemany_async must capture the classifier's parse output "
        "(or call _classify_caller_sql) ONCE at function scope, before "
        "the per-iteration loop."
    )
