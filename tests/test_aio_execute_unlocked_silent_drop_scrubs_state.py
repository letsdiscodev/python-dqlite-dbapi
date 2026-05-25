"""Pin: ``AsyncCursor._execute_unlocked``'s post-await close-race
silent-drop arm must scrub the introspection-surface fields
(``description`` / ``rows`` / ``rowcount`` / ``row_index``) so a
sibling-task ``close()`` mid-execute does NOT leave the cursor
advertising the PRIOR query's result set as the fresh execute's
outcome.

The closed-cursor gate at ``_check_closed`` blocks the fetch
methods but does NOT block ``description`` / ``rowcount`` /
``lastrowid`` property reads (PEP 249 §6.2.* properties intended
to survive on closed cursors per stdlib-parity for the read-only
introspection slots). Without this scrub, ``await cur.execute(
"SELECT new")`` could return while ``cur.description`` reports the
PRIOR query's tuple — a misleading silent surface.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor


def _make_async_cursor() -> AsyncCursor:
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._rows = []
    cur._description = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_index = 0
    cur._arraysize = 1
    cur._row_factory = None
    cur.messages = []
    cur._executing_task = None
    cur._completed_iterations = 0
    return cur


@pytest.mark.asyncio
async def test_query_branch_silent_drop_scrubs_prior_description(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sibling-task close() that fires between the wire return and
    the post-await guard must leave the cursor reporting "no result
    set" — NOT the PRIOR query's description.
    """
    cur = _make_async_cursor()
    # Pretend a previous query populated state we want to verify gets
    # scrubbed.
    cur._description = (("prior_col", 1, None, None, None, None, None),)
    cur._rows = [("prior_row",)]
    cur._rowcount = 1
    cur._row_index = 0

    # Stub the connection wire layer.
    inner = MagicMock()
    inner.query_raw_typed = MagicMock(return_value="fake_coro")
    parent_conn = MagicMock()
    parent_conn._ensure_connection = AsyncMock(return_value=inner)
    cur._connection = parent_conn

    async def fake_call_client(_coro: Any) -> Any:
        # Sibling close ran while we awaited the wire.
        cur._closed = True
        return (["new_col"], [1], [[1]], [[42]])

    from dqlitedbapi.aio import cursor as cursor_mod

    monkeypatch.setattr(cursor_mod, "_call_client", fake_call_client)
    await cur._execute_unlocked("SELECT 1", None)

    # After the silent-drop, the introspection surface must reflect
    # "no result set", not the prior query's state.
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
    """DML mirror: a sibling close mid-await must scrub the
    introspection surface. ``_lastrowid`` is intentionally PRESERVED
    (stdlib sqlite3.Cursor.lastrowid persists across the boundary).
    """
    cur = _make_async_cursor()
    cur._description = (("prior_col", 1, None, None, None, None, None),)
    cur._rows = [("prior_row",)]
    cur._rowcount = 3
    cur._row_index = 0
    cur._lastrowid = 99

    inner = MagicMock()
    inner.execute = MagicMock(return_value="fake_coro")
    parent_conn = MagicMock()
    parent_conn._ensure_connection = AsyncMock(return_value=inner)
    cur._connection = parent_conn

    async def fake_call_client(_coro: Any) -> Any:
        cur._closed = True
        return (123, 1)

    from dqlitedbapi.aio import cursor as cursor_mod

    monkeypatch.setattr(cursor_mod, "_call_client", fake_call_client)
    await cur._execute_unlocked("INSERT INTO t VALUES (1)", None)

    assert cur._description is None
    assert cur._rows == []
    assert cur._rowcount == -1
    assert cur._row_index == 0
    # _lastrowid is intentionally preserved on close (stdlib parity).
    assert cur._lastrowid == 99, "DML silent-drop must NOT clobber prior _lastrowid"


# Quiet unused-asyncio import for ruff.
_ = asyncio
