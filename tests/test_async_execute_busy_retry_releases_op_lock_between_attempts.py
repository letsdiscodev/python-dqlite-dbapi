"""Pin: ``AsyncCursor.execute`` releases the connection's
``op_lock`` between BUSY retries so sibling tasks on the same
``AsyncConnection`` can make progress during the SQLite-curve
backoff sleep.

The prior shape held the outer ``async with op_lock:`` around the
whole ``retry_async_on_busy(...)`` call. Under BUSY contention
the helper's ``await asyncio.sleep(...)`` between attempts ran
INSIDE the lock — sibling task ``await conn.commit()`` /
``rollback()`` / ``close()`` / new-cursor execute() parked on
``op_lock`` for the entire backoff curve (up to ``busy_timeout``,
default 5 s).

The fix moves the lock acquisition INTO a per-attempt callable
the helper invokes (mirrors the sync sibling's
``retry_sync_on_busy``-with-``run_sync`` shape). Each attempt
acquires, runs the wire round-trip, releases. The helper's sleep
between attempts runs in the gap, leaving ``op_lock`` available
to sibling tasks.

``executemany`` retains the outer-lock atomicity (ISSUE-544
invariant): a concurrent task cannot slip arbitrary statements
between executemany iterations.
"""

from __future__ import annotations

import ast
import inspect
import textwrap

from dqlitedbapi.aio import cursor as aio_cursor_mod


def _async_execute_source() -> str:
    return textwrap.dedent(inspect.getsource(aio_cursor_mod.AsyncCursor.execute))


def test_async_execute_does_not_wrap_retry_call_in_outer_op_lock() -> None:
    """Structural pin: ``AsyncCursor.execute`` must NOT call
    ``retry_async_on_busy`` directly inside an ``async with
    op_lock:`` block. The retry's per-attempt locking is owned by
    the factory callable.
    """
    src = _async_execute_source()
    tree = ast.parse(src)

    # Walk every ``async with op_lock:`` and check whether its
    # body contains a direct call to ``retry_async_on_busy``.
    found_retry_inside_op_lock = False
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncWith):
            continue
        # Check the items of this async-with for ``op_lock``.
        binds_op_lock = False
        for item in node.items:
            ctx = item.context_expr
            if isinstance(ctx, ast.Name) and ctx.id == "op_lock":
                binds_op_lock = True
        if not binds_op_lock:
            continue
        # Walk body for ``retry_async_on_busy(...)`` call.
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
    """The post-fix shape supplies the retry helper with a
    factory callable that itself opens an ``async with op_lock:``
    block. This pin ensures the per-attempt lock discipline is
    structurally present.
    """
    src = _async_execute_source()
    # The factory in the fix is a nested ``async def _attempt()``
    # that contains ``async with op_lock:``. Verify both shapes
    # appear in the source.
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
