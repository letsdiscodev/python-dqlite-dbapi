"""_row_index must reset to 0 after every execute (SELECT or DML).

The SELECT branch always set ``_row_index = 0``; the DML branch did
not. Because ``_check_result_set`` gates fetches on ``_description
is None`` after DML, the stale ``_row_index`` was not directly
observable — but it broke the invariant the rest of the code
assumes (and that the iterator-reset tests pin).
"""

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


class _AwaitableObj:
    """A bare-bones awaitable that resolves to ``obj`` on ``await``."""

    def __init__(self, obj: object) -> None:
        self.obj = obj

    def __await__(self):  # type: ignore[no-untyped-def]
        yield from ()
        return self.obj


class _FakeClient:
    """Mock of the async client interface the cursor talks to."""

    def execute(self, sql: str, params):  # type: ignore[no-untyped-def]
        return _AwaitableObj(obj=(42, 1))

    def query_raw_typed(self, sql: str, params):  # type: ignore[no-untyped-def]
        return _AwaitableObj(obj=([], [], []))


def _cursor_with_prior_select() -> Cursor:
    conn = MagicMock()
    c = Cursor(conn)
    c._description = [("id", None, None, None, None, None, None)]
    c._rows = [(1,), (2,), (3,)]
    c._row_index = 2  # Caller had fetched two rows.
    c._rowcount = 3
    return c


@pytest.mark.asyncio
async def test_sync_cursor_dml_resets_row_index() -> None:
    """Sync cursor's ``_execute_async`` DML branch must reset
    ``_row_index`` to 0 so that a subsequent SELECT starts iteration
    from a clean state (see the iterator-reset tests in
    ``test_cursor_iterator_reset``)."""
    c = _cursor_with_prior_select()

    async def fake_get_async_connection():  # type: ignore[no-untyped-def]
        return _FakeClient()

    c._connection._get_async_connection = fake_get_async_connection
    await c._execute_async("INSERT INTO t VALUES (1)")

    assert c._row_index == 0
    assert c._description is None
    assert c._rows == []


@pytest.mark.asyncio
async def test_async_cursor_dml_resets_row_index() -> None:
    """AsyncCursor.execute DML branch must reset ``_row_index``."""
    import asyncio

    conn = MagicMock()
    conn._closed = False
    lock = asyncio.Lock()

    async def fake_ensure_connection():  # type: ignore[no-untyped-def]
        return _FakeClient()

    conn._ensure_connection = fake_ensure_connection
    conn._ensure_locks = MagicMock(return_value=(lock, lock))

    c = AsyncCursor(conn)
    c._description = [("id", None, None, None, None, None, None)]
    c._rows = [(1,), (2,), (3,)]
    c._row_index = 2
    c._rowcount = 3

    await c.execute("INSERT INTO t VALUES (1)")

    assert c._row_index == 0
    assert c._description is None
    assert c._rows == []
