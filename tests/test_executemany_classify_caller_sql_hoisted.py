"""Pin: sync ``_executemany_async`` runs ``_classify_caller_sql``
ONCE at the top, capturing the placeholder count, and per-iteration
just compares lengths.

The redundancy: routing each iteration through the public-execute
path would re-parse the SQL via ``_strip_sql_noise`` (a regex sub
with DOTALL) and ``_strip_leading_comments`` for every row. For
N=10000 rows this is O(N * |SQL|) wasted work. Hoisting the parse
gives O(|SQL| + N) for invariant SQL.

Mirrors the architect note: capture the classifier output upfront
and pass it (or its derivative) to the inner loop.
"""

from __future__ import annotations

import ast
import inspect
import textwrap

from dqlitedbapi import cursor as sync_cur_mod


def test_executemany_async_calls_classifier_once_at_top() -> None:
    """Source-level pin: ``_executemany_async`` references
    ``_classify_caller_sql`` (or a related parser) at function scope
    (not inside the per-iteration ``for`` loop)."""
    src = textwrap.dedent(inspect.getsource(sync_cur_mod.Cursor._executemany_async))
    tree = ast.parse(src)
    # Find the For node and check its body for _classify_caller_sql.
    for node in ast.walk(tree):
        if isinstance(node, ast.For):
            for sub in ast.walk(node):
                if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name):
                    assert sub.func.id != "_classify_caller_sql", (
                        "_executemany_async must NOT call _classify_caller_sql "
                        "inside its per-iteration loop — hoist to function scope."
                    )

    # And: the function body MUST reference _classify_caller_sql at
    # function scope (i.e. before / outside the For).
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
