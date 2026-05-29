"""Exhausted cursor iterators keep raising StopIteration / StopAsyncIteration on every
subsequent call (PEP 234 / PEP 492), for both cursor flavours.
"""

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


class _AwaitableObj:
    def __init__(self, obj: object) -> None:
        self.obj = obj

    def __await__(self):
        yield from ()
        return self.obj


class _ScriptedClient:
    def __init__(self, rows: list[list]) -> None:  # type: ignore[type-arg]
        self._rows = rows

    def query_raw_typed(self, sql: str, params):
        from dqlitewire.constants import ValueType

        # One ValueType per column; row_types stays empty since per-row
        # dispatch can handle these rows from column_types alone.
        column_types = [ValueType.INTEGER]
        row_types = [[] for _ in self._rows]  # type: ignore[var-annotated]
        return _AwaitableObj(obj=(["x"], column_types, row_types, self._rows))

    def execute(self, sql: str, params):
        return _AwaitableObj(obj=(0, 0))


async def test_sync_cursor_stop_iteration_repeats_after_exhaustion() -> None:
    conn = MagicMock()
    scripted = _ScriptedClient([[1], [2]])

    async def get_client():
        return scripted

    conn._get_async_connection = get_client

    c = Cursor(conn)
    await c._execute_async("SELECT x FROM t")

    it = iter(c)
    assert next(it) == (1,)
    assert next(it) == (2,)
    with pytest.raises(StopIteration):
        next(it)
    with pytest.raises(StopIteration):
        next(it)


async def test_async_cursor_stop_async_iteration_repeats_after_exhaustion() -> None:
    import asyncio

    conn = MagicMock()
    conn._closed = False
    lock = asyncio.Lock()
    scripted = _ScriptedClient([[1], [2]])

    async def fake_ensure_connection():
        return scripted

    conn._ensure_connection = fake_ensure_connection
    conn._ensure_locks = MagicMock(return_value=(lock, lock))

    c = AsyncCursor(conn)
    await c.execute("SELECT x FROM t")

    assert await c.__anext__() == (1,)
    assert await c.__anext__() == (2,)
    with pytest.raises(StopAsyncIteration):
        await c.__anext__()
    with pytest.raises(StopAsyncIteration):
        await c.__anext__()
