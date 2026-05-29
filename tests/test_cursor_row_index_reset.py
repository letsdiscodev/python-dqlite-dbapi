"""``_row_index`` resets to 0 after every execute, including DML (the DML branch
previously skipped it — latent because fetches are gated on ``_description``)."""

from unittest.mock import MagicMock

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


class _AwaitableObj:
    def __init__(self, obj: object) -> None:
        self.obj = obj

    def __await__(self):
        yield from ()
        return self.obj


class _FakeClient:
    def execute(self, sql: str, params):
        return _AwaitableObj(obj=(42, 1))

    def query_raw_typed(self, sql: str, params):
        return _AwaitableObj(obj=([], [], []))


def _cursor_with_prior_select() -> Cursor:
    conn = MagicMock()
    c = Cursor(conn)
    c._description = [("id", None, None, None, None, None, None)]  # type: ignore[assignment]
    c._rows = [(1,), (2,), (3,)]
    c._row_index = 2
    c._rowcount = 3
    return c


async def test_sync_cursor_dml_resets_row_index() -> None:
    """Sync cursor's ``_execute_async`` DML branch resets ``_row_index`` to 0."""
    c = _cursor_with_prior_select()

    async def fake_get_async_connection():
        return _FakeClient()

    c._connection._get_async_connection = fake_get_async_connection
    await c._execute_async("INSERT INTO t VALUES (1)")

    assert c._row_index == 0
    assert c._description is None
    assert c._rows == []


async def test_async_cursor_dml_resets_row_index() -> None:
    """AsyncCursor.execute DML branch must reset ``_row_index``."""
    import asyncio

    conn = MagicMock()
    conn._closed = False
    lock = asyncio.Lock()

    async def fake_ensure_connection():
        return _FakeClient()

    conn._ensure_connection = fake_ensure_connection
    conn._ensure_locks = MagicMock(return_value=(lock, lock))

    c = AsyncCursor(conn)
    c._description = [("id", None, None, None, None, None, None)]  # type: ignore[assignment]
    c._rows = [(1,), (2,), (3,)]
    c._row_index = 2
    c._rowcount = 3

    await c.execute("INSERT INTO t VALUES (1)")

    assert c._row_index == 0
    assert c._description is None
    assert c._rows == []
