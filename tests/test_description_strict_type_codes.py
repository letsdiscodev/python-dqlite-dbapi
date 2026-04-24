"""``Cursor.description[i][1]`` must compare equal to a PEP 249 Type
Object. A wire response where ``column_types`` is shorter than
``columns`` used to produce ``description`` rows with ``type_code=None``,
which silently failed every ``type_code == STRING`` check downstream.
Raise ``DataError`` instead so the wire anomaly surfaces loudly.
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

    def __await__(self):  # type: ignore[no-untyped-def]
        yield from ()
        return self.obj


class _ShortTypeCodesClient:
    """Mock client whose ``query_raw_typed`` returns fewer type codes
    than columns. No production path produces this, but fuzz / broken
    peer / future protocol change could."""

    def query_raw_typed(self, sql: str, params):  # type: ignore[no-untyped-def]
        # columns = 2, column_types = 0 — mismatched.
        return _Awaitable(obj=(["a", "b"], [], [[], []], [[1, 2], [3, 4]]))


@pytest.mark.asyncio
async def test_sync_execute_raises_dataerror_on_short_column_types() -> None:
    conn = MagicMock()

    async def get_client():  # type: ignore[no-untyped-def]
        return _ShortTypeCodesClient()

    conn._get_async_connection = get_client
    cur = Cursor(conn)
    with pytest.raises(DataError, match="columns but 0 type codes"):
        await cur._execute_async("SELECT a, b FROM t")


@pytest.mark.asyncio
async def test_async_execute_raises_dataerror_on_short_column_types() -> None:
    conn = MagicMock()

    async def ensure_connection():  # type: ignore[no-untyped-def]
        return _ShortTypeCodesClient()

    conn._ensure_connection = ensure_connection
    cur = AsyncCursor(conn)
    with pytest.raises(DataError, match="columns but 0 type codes"):
        await cur._execute_unlocked("SELECT a, b FROM t", ())
