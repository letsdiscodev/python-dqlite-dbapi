"""executemany([]) doesn't leak stale SELECT state."""

from collections import deque
from unittest.mock import MagicMock, patch

import pytest

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
    # ``AsyncCursor.executemany`` acquires ``op_lock`` across the whole
    # iteration; the mocked connection must hand back a real asyncio.Lock
    # so ``async with op_lock:`` works even on the empty-iteration path.
    conn._ensure_locks.return_value = (asyncio.Lock(), asyncio.Lock())
    c = AsyncCursor(conn)
    c._description = [("id", None, None, None, None, None, None)]  # type: ignore[assignment]
    c._rows = deque([(1,), (2,)])  # type: ignore[assignment]
    c._rowcount = 2
    return c


async def _noop(*_a: object, **_kw: object) -> None:
    return None


class TestExecutemanyEmpty:
    @pytest.mark.asyncio
    async def test_empty_executemany_clears_description(self) -> None:
        """After executemany([]) the cursor must not appear to hold a
        prior SELECT result."""
        c = _cursor_with_prior_select()

        # ``Cursor`` uses ``__slots__`` so per-instance method override
        # is not possible; patch the class attribute for the test scope.
        with patch.object(Cursor, "_execute_async", new=_noop):
            await c._executemany_async("INSERT INTO t VALUES (?)", [])

        assert c.description is None
        assert c._rows == []
        # stdlib sqlite3 / psycopg2 report 0 for empty executemany;
        # zero iterations → zero rows affected is deterministic. PEP
        # 249 also permits -1 ("undetermined") but matching the
        # stdlib / psycopg2 contract avoids surprising callers doing
        # ``if cur.rowcount > 0: ...`` checks.
        assert c.rowcount == 0

    @pytest.mark.asyncio
    async def test_async_cursor_executemany_empty_via_public_surface(self) -> None:
        """Mirror of the sync test on the async cursor's public
        ``executemany`` entry point. The existing empty-sequence test
        exercises the internal ``_executemany_async`` helper on the
        sync ``Cursor`` class; without a test at the ``AsyncCursor``
        public surface a future refactor could diverge the two paths
        silently.
        """
        c = _async_cursor_with_prior_select()

        # An empty ``seq_of_parameters`` never enters the loop so no
        # monkey-patch of ``execute`` is required.
        await c.executemany("INSERT INTO t VALUES (?)", [])

        assert c.description is None
        assert list(c._rows) == []
        # Matches the sync sibling above: stdlib-parity 0, not -1.
        assert c.rowcount == 0


class TestExecutemanyEmptyIterableShape:
    """``executemany`` accepts ``Iterable[Sequence[Any]]``. The
    empty-list test above pins ``[]``, but an empty iterator (e.g.,
    ``iter([])``) and an empty generator are structurally distinct
    and exercise the accumulator's no-yield branch separately. A
    future refactor that special-cased lists could regress the
    iterator path silently — pin both shapes here."""

    @pytest.mark.asyncio
    async def test_sync_cursor_executemany_empty_iter(self) -> None:
        c = _cursor_with_prior_select()
        await c._executemany_async("INSERT INTO t VALUES (?)", iter([]))
        assert c.description is None
        assert c._rows == []
        assert c.rowcount == 0

    @pytest.mark.asyncio
    async def test_sync_cursor_executemany_empty_generator(self) -> None:
        c = _cursor_with_prior_select()
        await c._executemany_async("INSERT INTO t VALUES (?)", (x for x in []))  # type: ignore[var-annotated]
        assert c.description is None
        assert c._rows == []
        assert c.rowcount == 0

    @pytest.mark.asyncio
    async def test_async_cursor_executemany_empty_iter(self) -> None:
        c = _async_cursor_with_prior_select()
        await c.executemany("INSERT INTO t VALUES (?)", iter([]))
        assert c.description is None
        assert list(c._rows) == []
        assert c.rowcount == 0

    @pytest.mark.asyncio
    async def test_async_cursor_executemany_empty_generator(self) -> None:
        c = _async_cursor_with_prior_select()
        await c.executemany("INSERT INTO t VALUES (?)", (x for x in []))  # type: ignore[var-annotated]
        assert c.description is None
        assert list(c._rows) == []
        assert c.rowcount == 0
