"""Pin: a NULL-first-row column's ``type_code`` resolves from the first
non-NULL type in later rows, falling back to UNKNOWN only if all rows are NULL."""

from __future__ import annotations

import asyncio
import datetime
from collections.abc import Sequence
from typing import Any

from dqlitedbapi import Connection
from dqlitedbapi.cursor import Cursor
from dqlitewire import ValueType


class _StubInnerConn:
    """Stub ``DqliteConnection`` returning a hand-seeded wire response."""

    def __init__(
        self,
        columns: Sequence[bytes],
        column_types: Sequence[int],
        row_types: Sequence[Sequence[int]],
        rows: Sequence[tuple[Any, ...]],
    ) -> None:
        self._columns = columns
        self._column_types = column_types
        self._row_types = row_types
        self._rows = rows

    def query_raw_typed(self, _operation: str, _params: object) -> object:
        async def _co() -> tuple[
            Sequence[bytes],
            Sequence[int],
            Sequence[Sequence[int]],
            Sequence[tuple[Any, ...]],
        ]:
            return (self._columns, self._column_types, self._row_types, self._rows)

        return _co()


def _seed_cursor() -> tuple[Cursor, Connection]:
    """Hand-seeded sync ``Cursor`` + ``Connection`` shell over a stub conn."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._async_conn = None  # set per test
    conn._creator_thread = 1  # placeholder; _check_thread bypassed via direct call
    conn.messages = []
    conn._row_factory = None
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._connection = conn
    cur._rows = []
    cur._row_index = 0
    cur._description = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_factory = None
    cur._arraysize = 1
    cur.messages = []
    return cur, conn


def test_sync_description_null_first_row_resolves_through_subsequent_rows() -> None:
    """A NULL-first column with typed later rows must surface the resolved type."""
    cur, conn = _seed_cursor()
    column_types = [int(ValueType.NULL)]
    row_types = [[int(ValueType.NULL)], [int(ValueType.TEXT)]]
    rows = [(None,), ("hello",)]
    conn._async_conn = _StubInnerConn(  # type: ignore[assignment]
        [b"col"], column_types, row_types, rows
    )

    async def _get_inner() -> Any:
        return conn._async_conn

    conn._get_async_connection = _get_inner

    asyncio.run(cur._execute_async("SELECT col FROM t", []))

    assert cur._description is not None
    type_code = cur._description[0][1]
    assert type_code == int(ValueType.TEXT), (
        f"expected description type_code to resolve to TEXT (3), got {type_code}"
    )


def test_sync_value_conversion_resolves_for_null_first_converter_column() -> None:
    """Data-path half: a NULL-first column with a later converter type
    (ISO8601) must dispatch the per-row converter, not just resolve the
    description; the NULL row stays None and the typed row becomes datetime."""
    cur, conn = _seed_cursor()
    column_types = [int(ValueType.NULL)]
    row_types = [[int(ValueType.NULL)], [int(ValueType.ISO8601)]]
    rows = [(None,), ("2024-01-15 10:30:45",)]
    conn._async_conn = _StubInnerConn(  # type: ignore[assignment]
        [b"col"], column_types, row_types, rows
    )

    async def _get_inner() -> Any:
        return conn._async_conn

    conn._get_async_connection = _get_inner

    asyncio.run(cur._execute_async("SELECT col FROM t", []))

    assert cur._description is not None
    assert cur._description[0][1] == int(ValueType.ISO8601)
    assert cur._rows[0][0] is None
    assert isinstance(cur._rows[1][0], datetime.datetime)
    assert cur._rows[1][0] == datetime.datetime(2024, 1, 15, 10, 30, 45)


def test_sync_description_all_null_falls_back_to_unknown() -> None:
    """All-NULL column falls back to the UNKNOWN sentinel (a real Type
    Object, so chained ``==`` returns False cleanly)."""
    from dqlitedbapi import UNKNOWN

    cur, conn = _seed_cursor()
    column_types = [int(ValueType.NULL)]
    row_types = [[int(ValueType.NULL)], [int(ValueType.NULL)]]
    rows = [(None,), (None,)]
    conn._async_conn = _StubInnerConn(  # type: ignore[assignment]
        [b"col"], column_types, row_types, rows
    )

    async def _get_inner() -> Any:
        return conn._async_conn

    conn._get_async_connection = _get_inner

    asyncio.run(cur._execute_async("SELECT col FROM t", []))

    assert cur._description is not None
    assert cur._description[0][1] is UNKNOWN
