"""AsyncConnection: execute shortcuts, factory getters, busy retry, stubs, and in_transaction."""

from __future__ import annotations

import ast
import inspect
import textwrap
from unittest.mock import AsyncMock, MagicMock

import pytest

import dqlitedbapi.aio
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.aio import cursor as aio_cursor_mod
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import NotSupportedError


async def test_aconn_execute_with_parameters_forwards_to_cursor() -> None:
    """execute(sql, params) dispatches both the operation and the parameters to the cursor."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    cursors_seen: list[AsyncCursor] = []
    seen_calls: list[tuple[object, ...]] = []

    def fake_cursor() -> AsyncCursor:
        cur = MagicMock(spec=AsyncCursor)

        async def _execute(*args: object) -> None:
            seen_calls.append(args)

        cur.execute = AsyncMock(side_effect=_execute)
        cur.close = AsyncMock()
        cursors_seen.append(cur)
        return cur

    aconn.cursor = fake_cursor  # type: ignore[assignment]
    aconn.messages = []

    cur = await aconn.execute("SELECT ?", [1])

    assert cur is cursors_seen[0]
    assert seen_calls == [("SELECT ?", [1])]
    close_mock = cursors_seen[0].close
    assert isinstance(close_mock, AsyncMock)
    close_mock.assert_not_called()


def test_aconn_row_factory_getter_returns_set_value() -> None:
    """row_factory getter returns the value last assigned (None by default)."""
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        assert aconn.row_factory is None

        def factory(cur: object, row: tuple[object, ...]) -> dict[str, object]:
            return dict(zip(("a", "b"), row, strict=False))

        # Bypass the setter (which enforces loop binding) — pure getter pin.
        aconn._row_factory = factory
        assert aconn.row_factory is factory
        assert aconn.row_factory(None, (1, 2)) == {"a": 1, "b": 2}
    finally:
        aconn.force_close_transport()


def test_aconn_text_factory_getter_returns_str() -> None:
    """text_factory getter is the stdlib-parity stub returning str unconditionally."""
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        assert aconn.text_factory is str
    finally:
        aconn.force_close_transport()


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


def test_async_backup_is_not_async_def() -> None:
    method = dqlitedbapi.aio.AsyncConnection.backup
    assert not inspect.iscoroutinefunction(method), (
        "AsyncConnection.backup is async def — a forgotten `await` "
        "would silently produce a discarded coroutine. Match the "
        "plain-def discipline applied to executescript."
    )


def test_async_backup_raises_immediately_without_await() -> None:
    aconn = dqlitedbapi.aio.AsyncConnection("localhost:9001")
    with pytest.raises(NotSupportedError):
        aconn.backup(None)


def test_async_connection_executescript_is_plain_def_not_coroutine() -> None:
    assert not inspect.iscoroutinefunction(AsyncConnection.executescript), (
        "AsyncConnection.executescript must be `def`, not `async def`, so the "
        "NotSupportedError fires on the call line — not deferred to await"
    )


def test_async_cursor_executescript_is_plain_def_not_coroutine() -> None:
    assert not inspect.iscoroutinefunction(AsyncCursor.executescript), (
        "AsyncCursor.executescript must be `def`, not `async def`, so the "
        "NotSupportedError fires on the call line — not deferred to await"
    )


def test_async_connection_executescript_call_raises_immediately() -> None:
    """An unawaited call must raise immediately, not return a warning-at-GC coroutine."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn.messages = []
    with pytest.raises(NotSupportedError, match="executescript"):
        aconn.executescript("SELECT 1; SELECT 2;")


def test_async_cursor_executescript_call_raises_immediately() -> None:
    from unittest.mock import MagicMock

    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur.messages = []
    cur._connection = MagicMock()
    cur._connection.messages = []
    cur._connection._check_loop_binding = MagicMock()
    with pytest.raises(NotSupportedError, match="executescript"):
        cur.executescript("SELECT 1;")


def test_sync_closed_connection_in_transaction_returns_false() -> None:
    """Behaviour regression guard: closed connection returns False."""
    conn = Connection("127.0.0.1:9999")
    conn.close()
    assert conn.in_transaction is False
