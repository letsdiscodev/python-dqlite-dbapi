"""Pin: ``AsyncCursor.aclose`` and ``AsyncConnection.aclose`` provide
awaitable cross-driver-compatible aliases for ``close``.

The native ``AsyncCursor.close`` is sync by design (see its
docstring); the aliased ``aclose`` lets cross-driver code targeting
the aiosqlite / asyncpg / psycopg ``async def close`` shape use
``await cur.aclose()`` and ``contextlib.aclosing(cur)`` without the
``TypeError: object NoneType can't be used in 'await' expression``
that ``await cur.close()`` raises and without the
``AttributeError: 'AsyncCursor' object has no attribute 'aclose'``
that ``contextlib.aclosing`` would otherwise raise.

``AsyncConnection.close`` is already ``async def``; ``aclose``
exists there for symmetry with the cursor surface and for
``contextlib.aclosing`` compatibility.
"""

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
    """``await cur.aclose()`` closes the cursor — same observable
    effect as ``cur.close()`` but awaitable."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    assert cur.closed is False
    await cur.aclose()
    assert cur.closed is True


async def test_async_cursor_aclose_is_idempotent() -> None:
    """Like the sync ``close``, ``aclose`` must be safe to call
    multiple times."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    await cur.aclose()
    # Second call must not raise.
    await cur.aclose()
    assert cur.closed is True


async def test_aclosing_compatibility_on_async_cursor() -> None:
    """``contextlib.aclosing(cur)`` must work without raising
    ``AttributeError`` — the canonical PEP 525-style helper for
    async-iterator-with-explicit-cleanup."""
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
    """Negative-control: ``await cur.close()`` is STILL a TypeError —
    the sync ``close`` returns ``None`` and ``await None`` is invalid.
    The ``aclose`` alias is the documented awaitable shape; this pin
    catches a future change that silently flips ``close`` to async
    (which would invalidate the design rationale in close()'s
    docstring)."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    try:
        with pytest.raises(TypeError, match="await"):
            await cur.close()  # type: ignore[misc,func-returns-value]
    finally:
        # ``close`` was already called synchronously above, even
        # though awaiting its return value raised. Defensively ensure
        # cleanup.
        if not cur.closed:
            cur.close()
