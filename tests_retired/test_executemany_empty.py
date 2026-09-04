"""executemany([]) doesn't leak stale SELECT state."""

from collections import deque
from unittest.mock import MagicMock, patch

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def _cursor_with_prior_select() -> Cursor:
    conn = MagicMock()
    c = Cursor(conn)
    c._description = [("id", None, None, None, None, None, None)]  # type: ignore[assignment]
    c._rows = [(1,), (2,)]
    c._rowcount = 2
    return c


def _async_cursor_with_prior_select() -> AsyncCursor:
    import asyncio

    conn = MagicMock()
    # executemany does ``async with op_lock``, so hand back a real asyncio.Lock.
    conn._ensure_locks.return_value = (asyncio.Lock(), asyncio.Lock())
    c = AsyncCursor(conn)
    c._description = [("id", None, None, None, None, None, None)]  # type: ignore[assignment]
    c._rows = deque([(1,), (2,)])  # type: ignore[assignment]
    c._rowcount = 2
    return c


async def _noop(*_a: object, **_kw: object) -> None:
    return None


class TestExecutemanyEmpty:
    async def test_empty_executemany_clears_description(self) -> None:
        """After executemany([]) the cursor must not still hold a prior SELECT result."""
        c = _cursor_with_prior_select()

        # Cursor uses __slots__, so patch the class attribute, not the instance.
        with patch.object(Cursor, "_execute_async", new=_noop):
            await c._executemany_async("INSERT INTO t VALUES (?)", [])

        assert c.description is None
        assert c._rows == []
        # 0, not -1: matches stdlib sqlite3 / psycopg2 for empty executemany.
        assert c.rowcount == 0

    async def test_async_cursor_executemany_empty_via_public_surface(self) -> None:
        """Mirror of the sync test on AsyncCursor's public executemany surface."""
        c = _async_cursor_with_prior_select()

        # Empty seq never enters the loop, so no execute patch needed.
        await c.executemany("INSERT INTO t VALUES (?)", [])

        assert c.description is None
        assert list(c._rows) == []
        assert c.rowcount == 0


class TestExecutemanyEmptyIterableShape:
    """Empty iterator and empty generator exercise the accumulator's no-yield
    branch distinctly from the empty-list case above."""

    async def test_sync_cursor_executemany_empty_iter(self) -> None:
        c = _cursor_with_prior_select()
        await c._executemany_async("INSERT INTO t VALUES (?)", iter([]))
        assert c.description is None
        assert c._rows == []
        assert c.rowcount == 0

    async def test_sync_cursor_executemany_empty_generator(self) -> None:
        c = _cursor_with_prior_select()
        await c._executemany_async("INSERT INTO t VALUES (?)", (x for x in []))  # type: ignore[var-annotated]
        assert c.description is None
        assert c._rows == []
        assert c.rowcount == 0

    async def test_async_cursor_executemany_empty_iter(self) -> None:
        c = _async_cursor_with_prior_select()
        await c.executemany("INSERT INTO t VALUES (?)", iter([]))
        assert c.description is None
        assert list(c._rows) == []
        assert c.rowcount == 0

    async def test_async_cursor_executemany_empty_generator(self) -> None:
        c = _async_cursor_with_prior_select()
        await c.executemany("INSERT INTO t VALUES (?)", (x for x in []))  # type: ignore[var-annotated]
        assert c.description is None
        assert list(c._rows) == []
        assert c.rowcount == 0
