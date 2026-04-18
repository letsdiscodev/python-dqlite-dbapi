"""executemany([]) doesn't leak stale SELECT state."""

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.cursor import Cursor


def _cursor_with_prior_select() -> Cursor:
    conn = MagicMock()
    c = Cursor(conn)
    c._description = [("id", None, None, None, None, None, None)]
    c._rows = [(1,), (2,)]
    c._rowcount = 2
    return c


class TestExecutemanyEmpty:
    @pytest.mark.asyncio
    async def test_empty_executemany_clears_description(self) -> None:
        """After executemany([]) the cursor must not appear to hold a
        prior SELECT result."""
        c = _cursor_with_prior_select()

        async def _noop(*_a: object, **_kw: object) -> None:
            return None

        c._execute_async = _noop  # type: ignore[method-assign]
        await c._executemany_async("INSERT INTO t VALUES (?)", [])

        assert c.description is None
        assert c._rows == []
        assert c.rowcount == 0
