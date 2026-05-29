"""Pin: ``AsyncCursor.aclose`` / ``AsyncConnection.aclose`` are awaitable aliases for
``close``, so cross-driver code can use ``await x.aclose()`` and ``contextlib.aclosing``."""

from __future__ import annotations

import inspect
from contextlib import aclosing

import pytest

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
