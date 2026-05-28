"""Pin: ``description[i][1]`` (``type_code``) resolves through
``row_types[1:]`` for the first non-NULL type when ``column_types[i]
== ValueType.NULL``.

PEP 249 §6.1.2 says ``type_code`` "must compare equal to one of Type
Objects". The previous behaviour mapped any NULL-first-row column to
``None`` even when subsequent rows carried meaningful types — the
wire carries per-row types (used by ``_convert_row``) so the column-
header could and should reflect the resolved type. Only when EVERY
row's value at that column is NULL does the fallback to ``None``
fire (the genuinely unrecoverable case).
"""

from __future__ import annotations

import asyncio
import datetime
from collections.abc import Sequence
from typing import Any

from dqlitedbapi import Connection
from dqlitedbapi.cursor import Cursor
from dqlitewire import ValueType


class _StubInnerConn:
    """Stub for ``DqliteConnection`` returning a hand-seeded wire
    response. Exercises ``_execute_async``'s description-build path."""

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
    """Build a hand-seeded sync ``Cursor`` + ``Connection`` shell so
    we can call ``_execute_async`` against a stub inner conn."""
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
    """A column whose first row is NULL but whose subsequent rows have
    typed values must surface the resolved type in description, not
    ``None``."""
    cur, conn = _seed_cursor()
    # Two rows: row 0 has NULL in column 0; row 1 has TEXT (10/ISO8601-like
    # convention — pick a non-NULL ValueType).
    # Use ValueType.TEXT (3) and ValueType.NULL (5).
    column_types = [int(ValueType.NULL)]
    row_types = [[int(ValueType.NULL)], [int(ValueType.TEXT)]]
    rows = [(None,), ("hello",)]
    conn._async_conn = _StubInnerConn(  # type: ignore[assignment]
        [b"col"], column_types, row_types, rows
    )

    # ``_get_async_connection`` returns the inner conn — patch it.
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
    """Companion to the description-resolution pin above: a column
    whose first row is NULL but whose later row carries a *converter*
    type (ISO8601) must both (a) resolve the description ``type_code``
    from the later row AND (b) run the per-row value converter so the
    NULL row stays ``None`` while the typed row becomes a ``datetime``.

    The existing description pin uses ``ValueType.TEXT`` — a non-
    converter type — so it exercises only the description-build half.
    This pins the data-path half: ``_convert_rows``/``_convert_row``
    must dispatch on the per-row type for a NULL-first column. A
    regression that narrowed the conversion probe to row 0 (finding
    NULL → "no conversion needed") would leave the typed row as a raw
    string and this test would catch it.
    """
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

    # Description resolved from the later typed row.
    assert cur._description is not None
    assert cur._description[0][1] == int(ValueType.ISO8601)
    # Data path: the NULL row stays None; the typed row converts to a
    # datetime via the per-row converter dispatch.
    assert cur._rows[0][0] is None
    assert isinstance(cur._rows[1][0], datetime.datetime)
    assert cur._rows[1][0] == datetime.datetime(2024, 1, 15, 10, 30, 45)


def test_sync_description_all_null_falls_back_to_unknown() -> None:
    """When EVERY row's value at the column index is NULL, the type
    code falls back to the ``UNKNOWN`` sentinel — genuinely
    unrecoverable. UNKNOWN is a PEP 249 Type Object (with empty
    ``values``) so the chained-``==`` introspection idiom returns
    False cleanly for every real Type Object.
    """
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
