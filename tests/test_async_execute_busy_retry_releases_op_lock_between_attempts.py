"""``AsyncCursor.execute`` releases ``op_lock`` between BUSY retries.

The retry must acquire the lock per-attempt rather than wrapping the whole
``retry_async_on_busy`` call, so the backoff sleep between attempts runs outside
the lock and sibling tasks can make progress. (executemany keeps the outer-lock
atomicity, ISSUE-544.)
"""

from __future__ import annotations

import ast
import inspect
import textwrap

from dqlitedbapi.aio import cursor as aio_cursor_mod


def _async_execute_source() -> str:
    return textwrap.dedent(inspect.getsource(aio_cursor_mod.AsyncCursor.execute))


def test_async_execute_does_not_wrap_retry_call_in_outer_op_lock() -> None:
    """``execute`` must not call ``retry_async_on_busy`` inside ``async with op_lock``."""
    src = _async_execute_source()
    tree = ast.parse(src)

    found_retry_inside_op_lock = False
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncWith):
            continue
        binds_op_lock = False
        for item in node.items:
            ctx = item.context_expr
            if isinstance(ctx, ast.Name) and ctx.id == "op_lock":
                binds_op_lock = True
        if not binds_op_lock:
            continue
        for inner in ast.walk(ast.Module(body=node.body, type_ignores=[])):
            if not isinstance(inner, ast.Call):
                continue
            func = inner.func
            if isinstance(func, ast.Name) and func.id == "retry_async_on_busy":
                found_retry_inside_op_lock = True
            if isinstance(func, ast.Attribute) and func.attr == "retry_async_on_busy":
                found_retry_inside_op_lock = True

    assert not found_retry_inside_op_lock, (
        "AsyncCursor.execute still wraps ``retry_async_on_busy`` in "
        "an outer ``async with op_lock:`` block. The retry loop's "
        "``await asyncio.sleep`` between BUSY attempts must run "
        "OUTSIDE the lock so sibling tasks on the same connection "
        "(``commit``, ``rollback``, ``close``, new cursors) can "
        "acquire ``op_lock`` between attempts instead of parking "
        "for the full backoff curve."
    )


def test_async_execute_calls_retry_with_a_factory_that_acquires_op_lock() -> None:
    """The retry helper must be passed a per-attempt callable that owns op_lock."""
    src = _async_execute_source()
    assert "async def _attempt" in src or "_attempt = " in src, (
        "AsyncCursor.execute should define a per-attempt coroutine "
        "(typically named ``_attempt``) that owns the ``op_lock`` "
        "acquire — pass that callable to ``retry_async_on_busy`` so "
        "each retry acquires + releases the lock separately."
    )
    assert "async with op_lock" in src, (
        "the per-attempt coroutine must include ``async with op_lock:`` "
        "so each retry attempt re-acquires the lock"
    )
