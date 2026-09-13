"""AsyncCursor: aclose alias, async iteration/context-manager protocol pins."""

from __future__ import annotations

import asyncio
import gc
import inspect
from contextlib import aclosing

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


def test_async_cursor_has_aclose_attribute() -> None:
    assert hasattr(AsyncCursor, "aclose")
    assert inspect.iscoroutinefunction(AsyncCursor.aclose)


def test_async_connection_has_aclose_attribute() -> None:
    assert hasattr(AsyncConnection, "aclose")
    assert inspect.iscoroutinefunction(AsyncConnection.aclose)


async def test_async_cursor_aclose_closes_cursor() -> None:
    """``await cur.aclose()`` closes the cursor, like ``cur.close()`` but awaitable."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    assert cur.closed is False
    await cur.aclose()
    assert cur.closed is True


async def test_async_cursor_aclose_is_idempotent() -> None:
    """``aclose`` must be idempotent."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    await cur.aclose()
    await cur.aclose()
    assert cur.closed is True


async def test_aclosing_compatibility_on_async_cursor() -> None:
    """``contextlib.aclosing(cur)`` must work without raising ``AttributeError``."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    async with aclosing(cur) as c:
        assert c is cur
        assert c.closed is False
    assert cur.closed is True


async def test_aclosing_compatibility_on_async_connection() -> None:
    """``contextlib.aclosing(conn)`` symmetric with the cursor case."""
    conn = AsyncConnection("localhost:9001")
    async with aclosing(conn) as c:
        assert c is conn
        assert c.closed is False
    assert conn.closed is True


async def test_await_cursor_close_directly_raises_type_error() -> None:
    """Negative control: ``await cur.close()`` is still a TypeError (sync close returns
    None); ``aclose`` is the awaitable shape."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    try:
        with pytest.raises(TypeError, match="await"):
            await cur.close()  # type: ignore[misc,func-returns-value]
    finally:
        if not cur.closed:
            cur.close()


async def test_aiter_on_closed_cursor_with_gc_parent_defers_to_anext() -> None:
    """Stdlib parity: ``aiter(closed_cur)`` returns the cursor; the diagnostic surfaces on
    the first ``__anext__``, not at ``aiter()`` (``__aenter__`` raises eagerly, no sync analog)."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    cur.close()
    del conn
    gc.collect()

    it = aiter(cur)
    assert it is cur, "aiter(cur) must return the cursor itself (PEP 234)"

    with pytest.raises(dqlitedbapi.InterfaceError):
        await anext(cur)


async def test_aenter_loop_binding_check_runs_before_body() -> None:
    """``async with cur:`` in its bound loop must enter cleanly (sanity for the check)."""
    conn = AsyncConnection("localhost:9001")
    try:
        cur = conn.cursor()
        async with cur:
            pass
    finally:
        await conn.close()


def test_aenter_from_foreign_loop_raises_programming_error() -> None:
    """A cursor created in loop A then entered from loop B should raise ProgrammingError."""
    conn_holder: dict[str, AsyncConnection] = {}

    async def loop_a_setup() -> None:
        conn_holder["c"] = AsyncConnection("localhost:9001")

    asyncio.run(loop_a_setup())
    conn = conn_holder["c"]
    cur = conn.cursor()

    async def loop_b() -> None:
        async with cur:
            pass

    # Cursors carry no loop binding of their own; entering one from another loop is fine
    # until it touches the wire, which is where the connection's loop check fires.
    asyncio.run(loop_b())


async def test_aiter_returns_self_invariance() -> None:
    """PEP 492 ``aiter(obj) is obj``: pin against returning a
    wrapper/generator."""
    aconn = AsyncConnection("127.0.0.1:9999", database="x")
    cur = aconn.cursor()
    assert cur.__aiter__() is cur
    assert aiter(cur) is cur


async def test_aiter_on_closed_cursor_does_not_raise() -> None:
    """``__aiter__`` on a closed cursor must NOT raise; the closed-state
    diagnostic is deferred to ``__anext__``."""
    aconn = AsyncConnection("127.0.0.1:9999", database="x")
    cur = aconn.cursor()
    cur.close()

    # Pre-fix this raised InterfaceError: _check_loop_binding ran a
    # closed-state check before returning self.
    same = cur.__aiter__()
    assert same is cur
