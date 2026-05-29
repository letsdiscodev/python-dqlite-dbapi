"""``AsyncCursor.close`` is sync-by-design and ``_execute_unlocked`` re-checks ``_closed`` after
the wire await, so a sibling close mid-execute can't repopulate state onto a closed cursor."""

from __future__ import annotations

import inspect
from typing import Any
from unittest.mock import AsyncMock, patch

from dqlitedbapi.aio import AsyncConnection, AsyncCursor


def test_async_cursor_close_is_not_a_coroutine_function() -> None:
    """``close()`` returns immediately, not a coroutine the caller must await."""
    assert not inspect.iscoroutinefunction(AsyncCursor.close)


async def test_async_cursor_close_without_await_takes_effect() -> None:
    """Calling ``close()`` without await must flip ``_closed`` and scrub state."""
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    cur._description = (("a", None, None, None, None, None, None),)
    cur._rows = [(1,)]

    cur.close()  # no await

    assert cur._closed is True
    assert cur._description is None
    assert cur._rows == []


async def test_execute_post_await_closed_check_drops_result() -> None:
    """Sibling close while ``_execute_unlocked`` is parked on the wire await: the post-await
    closed-check must drop the result, not repopulate ``_rows``/``_description``."""
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    # Fake inner connection; the coroutine never runs (we patch _call_client).
    inner = AsyncMock()
    inner.query_raw_typed = lambda _op, _params: None
    cur._connection._ensure_connection = AsyncMock(return_value=inner)
    cur._description = (("old", None, None, None, None, None, None),)
    cur._rows = [(0,)]

    async def race_call(_coro: Any) -> Any:
        # External close fires while the executor is parked in the wire await.
        cur._closed = True
        cur._rows = []
        cur._description = None
        return (
            ("c",),  # columns
            [0],  # column_types
            [],  # row_types
            [(99,)],  # rows
        )

    with patch("dqlitedbapi.aio.cursor._call_client", new=race_call):
        await cur._execute_unlocked("SELECT 1", None)

    assert cur._closed is True
    assert cur._rows == []
    assert cur._description is None


async def test_execute_post_await_closed_check_drops_insert_result() -> None:
    """Non-query branch: a sibling close while parked on the wire await must drop the
    ``(last_insert_id, rows_affected)`` tuple, not repopulate ``_lastrowid``/``_rowcount``."""
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    inner = AsyncMock()
    inner.execute = lambda _op, _params: None
    cur._connection._ensure_connection = AsyncMock(return_value=inner)
    cur._lastrowid = 7  # pre-existing state
    cur._rowcount = 3

    async def race_call(_coro: Any) -> Any:
        # External close fires while parked; close preserves _lastrowid/_rowcount (stdlib parity).
        cur._closed = True
        return (42, 1)  # non-query wire shape: (last_insert_id, rows_affected)

    # Force the non-query branch via _is_row_returning=False.
    with (
        patch("dqlitedbapi.aio.cursor._call_client", new=race_call),
        patch("dqlitedbapi.aio.cursor._is_row_returning", return_value=False),
    ):
        await cur._execute_unlocked("INSERT INTO t VALUES (1)", (1,))

    # Guard drops (42, 1): _lastrowid stays at sticky 7, _rowcount is reset to -1.
    assert cur._closed is True
    assert cur._lastrowid == 7
    assert cur._rowcount == -1
