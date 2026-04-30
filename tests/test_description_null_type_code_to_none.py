"""Pin: ``Cursor.description`` / ``AsyncCursor.description`` map
``ValueType.NULL`` to ``None`` (cycle 22 mapping).

Wire-layer returns ``column_types`` derived from row 0; if a
column was tagged ``ValueType.NULL`` (e.g. a ``LEFT JOIN``
unmatched row, or a literal ``SELECT NULL``), the cycle 22
mapping replaces ``ValueType.NULL`` (5) with ``None`` in the
description's ``type_code`` slot. PEP 249 §6.1.2 says the
type_code "must compare equal to one of the Type Objects" —
NULL is not one of the five Type Objects, and surfacing
``None`` matches the documented empty-result-set deviation
already in this module.

The mixed-row case is the load-bearing surface for the test
gap: a column tagged NULL alongside columns with real type
codes. A refactor that drops the comprehension would silently
re-introduce ``ValueType.NULL`` into description and break
every cross-driver type-branch.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor
from dqlitewire.constants import ValueType


class _MixedNullTypesClient:
    """Returns ``column_types=[TEXT, NULL, INTEGER]`` — the
    mixed-row case the cycle 22 mapping must handle."""

    async def query_raw_typed(
        self, sql: str, params: Any
    ) -> tuple[list[str], list[int], list[list[int]], list[list[Any]]]:
        return (
            ["name", "n", "age"],
            [int(ValueType.TEXT), int(ValueType.NULL), int(ValueType.INTEGER)],
            [[3, 5, 1], [3, 5, 1]],
            [["alice", None, 30], ["bob", None, 31]],
        )


class _AllNullClient:
    async def query_raw_typed(
        self, sql: str, params: Any
    ) -> tuple[list[str], list[int], list[list[int]], list[list[Any]]]:
        return (
            ["a", "b"],
            [int(ValueType.NULL), int(ValueType.NULL)],
            [[5, 5]],
            [[None, None]],
        )


@pytest.mark.asyncio
async def test_sync_description_maps_null_to_none_in_mixed_row() -> None:
    conn = MagicMock()

    async def _get() -> object:
        return _MixedNullTypesClient()

    conn._get_async_connection = _get
    cur = Cursor(conn)
    await cur._execute_async("SELECT name, NULL AS n, age FROM users")

    assert cur.description is not None
    assert cur.description[0][1] == ValueType.TEXT
    assert cur.description[1][1] is None
    assert cur.description[2][1] == ValueType.INTEGER


@pytest.mark.asyncio
async def test_async_description_maps_null_to_none_in_mixed_row() -> None:
    conn = MagicMock()

    async def _ensure() -> object:
        return _MixedNullTypesClient()

    conn._ensure_connection = _ensure
    cur = AsyncCursor(conn)
    await cur._execute_unlocked("SELECT name, NULL AS n, age FROM users", ())

    assert cur.description is not None
    assert cur.description[0][1] == ValueType.TEXT
    assert cur.description[1][1] is None
    assert cur.description[2][1] == ValueType.INTEGER


@pytest.mark.asyncio
async def test_sync_description_all_null_columns_all_none() -> None:
    conn = MagicMock()

    async def _get() -> object:
        return _AllNullClient()

    conn._get_async_connection = _get
    cur = Cursor(conn)
    await cur._execute_async("SELECT NULL AS a, NULL AS b")

    assert cur.description is not None
    assert all(d[1] is None for d in cur.description)
