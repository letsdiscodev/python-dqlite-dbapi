"""executemany([]) doesn't leak stale SELECT state (ISSUE-34)."""

from unittest.mock import AsyncMock, MagicMock

from dqlitedbapi.cursor import Cursor


def _cursor_with_prior_select() -> Cursor:
    conn = MagicMock()
    c = Cursor(conn)
    c._description = [("id", None, None, None, None, None, None)]
    c._rows = [(1,), (2,)]
    c._rowcount = 2
    return c


class TestExecutemanyEmpty:
    def test_empty_executemany_clears_description(self) -> None:
        """After executemany([]) the cursor must not appear to hold a
        prior SELECT result."""
        c = _cursor_with_prior_select()
        # Mock _run_sync so we don't need a loop.
        c._connection._run_sync = MagicMock(side_effect=lambda coro: coro.close() or None)
        c._connection._check_thread = MagicMock()

        # Directly drive _executemany_async — it's the code we're
        # verifying. Run it synchronously via a throwaway event loop.
        import asyncio

        asyncio.run(_run_empty(c))

        assert c.description is None
        assert c._rows == []
        assert c.rowcount == 0


async def _run_empty(c: Cursor) -> None:
    # Stub _execute_async so we don't need the full connection pathway.
    async def _noop(*_a: object, **_kw: object) -> None:
        return None

    c._execute_async = _noop  # type: ignore[method-assign]
    await c._executemany_async("INSERT INTO t VALUES (?)", [])
