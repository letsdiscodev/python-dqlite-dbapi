"""Cursor decodes each row with its own wire types, not the first row's.

SQLite is dynamically typed: under UNION/CASE/COALESCE/typeof() a column's
wire ``ValueType`` can differ per row, so collapsing to ``column_types`` misdecodes."""

from __future__ import annotations

import datetime
from typing import Any
from unittest.mock import MagicMock

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor
from dqlitewire import ValueType


class _AwaitableObj:
    def __init__(self, obj: object) -> None:
        self.obj = obj

    def __await__(self):
        yield from ()
        return self.obj


class _ScriptedClient:
    def __init__(
        self,
        columns: list[str],
        column_types: list[int],
        row_types: list[list[int]],
        rows: list[list[Any]],
    ) -> None:
        self._result = (columns, column_types, row_types, rows)

    def query_raw_typed(self, sql: str, params):
        return _AwaitableObj(obj=self._result)


async def test_sync_cursor_uses_per_row_types_iso8601_then_text() -> None:
    """Row 0 ISO8601 -> datetime; row 1 TEXT -> stays str (no fake-datetime parse)."""
    conn = MagicMock()
    scripted = _ScriptedClient(
        columns=["col"],
        column_types=[int(ValueType.ISO8601)],
        row_types=[
            [int(ValueType.ISO8601)],
            [int(ValueType.TEXT)],
        ],
        rows=[
            ["2024-01-01 12:34:56"],
            ["not-a-date"],
        ],
    )

    async def get_client():
        return scripted

    conn._get_async_connection = get_client
    c = Cursor(conn)

    await c._execute_async("SELECT col FROM t")
    rows = c.fetchall()
    assert rows[0] == (datetime.datetime(2024, 1, 1, 12, 34, 56),)
    assert rows[1] == ("not-a-date",), (
        "Row 1's TEXT wire type should NOT be parsed as ISO8601; it must stay a string."
    )


async def test_sync_cursor_uses_per_row_types_text_then_iso8601() -> None:
    """Row 0 TEXT -> stays str; row 1 ISO8601 -> datetime."""
    conn = MagicMock()
    scripted = _ScriptedClient(
        columns=["col"],
        column_types=[int(ValueType.TEXT)],
        row_types=[
            [int(ValueType.TEXT)],
            [int(ValueType.ISO8601)],
        ],
        rows=[
            ["plain-string"],
            ["2024-06-15 08:00:00"],
        ],
    )

    async def get_client():
        return scripted

    conn._get_async_connection = get_client
    c = Cursor(conn)

    await c._execute_async("SELECT col FROM t")
    rows = c.fetchall()
    assert rows[0] == ("plain-string",)
    assert rows[1] == (datetime.datetime(2024, 6, 15, 8, 0, 0),), (
        "Row 1's ISO8601 wire type must be converted to datetime even "
        "though row 0 (and thus column_types) declared TEXT."
    )


async def test_async_cursor_uses_per_row_types() -> None:
    """Async parity of the sync test."""
    import asyncio

    conn = MagicMock()
    conn._closed = False
    lock = asyncio.Lock()

    scripted = _ScriptedClient(
        columns=["col"],
        column_types=[int(ValueType.UNIXTIME)],
        row_types=[
            [int(ValueType.UNIXTIME)],
            [int(ValueType.TEXT)],
        ],
        rows=[
            [1_700_000_000],
            ["literal-string"],
        ],
    )

    async def fake_ensure_connection():
        return scripted

    conn._ensure_connection = fake_ensure_connection
    conn._ensure_locks = MagicMock(return_value=(lock, lock))

    c = AsyncCursor(conn)
    await c.execute("SELECT col FROM t")
    rows = await c.fetchall()
    assert isinstance(rows[0][0], datetime.datetime)
    assert rows[1] == ("literal-string",)


async def test_empty_rows_still_build_description() -> None:
    """Zero rows: ``column_types`` still populates ``description`` (row_types empty)."""
    conn = MagicMock()
    scripted = _ScriptedClient(
        columns=["col"],
        column_types=[int(ValueType.INTEGER)],
        row_types=[],
        rows=[],
    )

    async def get_client():
        return scripted

    conn._get_async_connection = get_client
    c = Cursor(conn)
    await c._execute_async("SELECT col FROM t")
    assert c.description is not None
    assert c.description[0][1] == int(ValueType.INTEGER)
    assert c.fetchall() == []
