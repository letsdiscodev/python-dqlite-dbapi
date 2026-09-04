"""A wire response with fewer ``column_types`` than ``columns`` must raise
``DataError``, not yield ``type_code=None`` rows that silently fail every check.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi import DataError
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


class _Awaitable:
    def __init__(self, obj: object) -> None:
        self.obj = obj

    def __await__(self):
        yield from ()
        return self.obj


class _ShortTypeCodesClient:
    """Mock client whose ``query_raw_typed`` returns fewer type codes than columns."""

    def query_raw_typed(self, sql: str, params):
        return _Awaitable(obj=(["a", "b"], [], [[], []], [[1, 2], [3, 4]]))


async def test_sync_execute_raises_dataerror_on_short_column_types() -> None:
    conn = MagicMock()

    async def get_client():
        return _ShortTypeCodesClient()

    conn._get_async_connection = get_client
    cur = Cursor(conn)
    with pytest.raises(DataError, match="columns but 0 type codes"):
        await cur._execute_async("SELECT a, b FROM t")


async def test_async_execute_raises_dataerror_on_short_column_types() -> None:
    conn = MagicMock()

    async def ensure_connection():
        return _ShortTypeCodesClient()

    conn._ensure_connection = ensure_connection
    cur = AsyncCursor(conn)
    with pytest.raises(DataError, match="columns but 0 type codes"):
        await cur._execute_unlocked("SELECT a, b FROM t", ())
