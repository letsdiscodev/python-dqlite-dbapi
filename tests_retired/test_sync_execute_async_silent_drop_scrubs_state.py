"""Sync mirror of the async silent-drop guard: a foreign-thread close mid-execute
(check_same_thread=False) scrubs description/rows/rowcount/row_index so a closed cursor
never advertises the prior query's result set. DML preserves _lastrowid (stdlib parity).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.cursor import Cursor


def _make_sync_cursor() -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._rows = []
    cur._description = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_index = 0
    cur._arraysize = 1
    cur._row_factory = None
    cur.messages = []
    cur._completed_iterations = 0
    return cur


@pytest.mark.asyncio
async def test_query_branch_silent_drop_scrubs_prior_description(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cur = _make_sync_cursor()
    cur._description = (("prior_col", 1, None, None, None, None, None),)
    cur._rows = [("prior_row",)]
    cur._rowcount = 1

    inner = MagicMock()
    inner.query_raw_typed = MagicMock(return_value="fake_coro")
    parent_conn = MagicMock()
    parent_conn._get_async_connection = AsyncMock(return_value=inner)
    cur._connection = parent_conn

    async def fake_call_client(_coro: Any) -> Any:
        cur._closed = True  # foreign-thread close ran while we awaited the wire
        return (["new_col"], [1], [[1]], [[42]])

    from dqlitedbapi import cursor as cursor_mod

    monkeypatch.setattr(cursor_mod, "_call_client", fake_call_client)
    await cur._execute_async("SELECT 1", None)

    assert cur._description is None, (
        f"silent-drop must scrub _description; got {cur._description!r}"
    )
    assert cur._rows == [], f"silent-drop must scrub _rows; got {cur._rows!r}"
    assert cur._rowcount == -1, f"silent-drop must scrub _rowcount; got {cur._rowcount!r}"
    assert cur._row_index == 0, f"silent-drop must scrub _row_index; got {cur._row_index!r}"


@pytest.mark.asyncio
async def test_dml_branch_silent_drop_scrubs_prior_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DML mirror: scrub the introspection surface but preserve _lastrowid (stdlib parity)."""
    cur = _make_sync_cursor()
    cur._description = (("prior_col", 1, None, None, None, None, None),)
    cur._rows = [("prior_row",)]
    cur._rowcount = 3
    cur._lastrowid = 99

    inner = MagicMock()
    inner.execute = MagicMock(return_value="fake_coro")
    parent_conn = MagicMock()
    parent_conn._get_async_connection = AsyncMock(return_value=inner)
    cur._connection = parent_conn

    async def fake_call_client(_coro: Any) -> Any:
        cur._closed = True
        return (123, 1)

    from dqlitedbapi import cursor as cursor_mod

    monkeypatch.setattr(cursor_mod, "_call_client", fake_call_client)
    await cur._execute_async("INSERT INTO t VALUES (1)", None)

    assert cur._description is None
    assert cur._rows == []
    assert cur._rowcount == -1
    assert cur._row_index == 0
    # _lastrowid is intentionally preserved on close (stdlib parity).
    assert cur._lastrowid == 99, "DML silent-drop must NOT clobber prior _lastrowid"
