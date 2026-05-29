"""Pin: ``AsyncCursor.close`` is sync-by-design and ``_execute_unlocked``
re-checks ``_closed`` after the wire await so a sibling-task close
mid-execute does not repopulate state onto a closed cursor.

Three coupled defects resolved together:

* ``async def close`` invited a forgot-``await`` footgun — converted
  to plain ``def`` matching ``Cursor.close``.
* close did not acquire ``_op_lock`` and was racy with in-flight
  execute — hybrid resolution: keep close sync, flip ``_closed=True``
  first (GIL-atomic), then best-effort teardown.
* ``_execute_unlocked`` populated state after the wire await without
  re-checking ``_closed`` — add the post-await guard so the executor
  drops the result rather than repopulating.
"""

from __future__ import annotations

import inspect
from typing import Any
from unittest.mock import AsyncMock, patch

from dqlitedbapi.aio import AsyncConnection, AsyncCursor


def test_async_cursor_close_is_not_a_coroutine_function() -> None:
    """Forgot-await footgun pin: ``close()`` returns immediately,
    not a coroutine that the caller must await."""
    assert not inspect.iscoroutinefunction(AsyncCursor.close)


async def test_async_cursor_close_without_await_takes_effect() -> None:
    """Calling ``close()`` without await must flip ``_closed`` and
    scrub state — proving the function is not a coroutine stub.
    """
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    cur._description = (("a", None, None, None, None, None, None),)
    cur._rows = [(1,)]

    cur.close()  # no await

    assert cur._closed is True
    assert cur._description is None
    assert cur._rows == []


async def test_execute_post_await_closed_check_drops_result() -> None:
    """Race scenario: a sibling task calls ``close()`` while
    ``_execute_unlocked`` is parked on the wire await. The executor
    must drop the result (not repopulate ``_rows`` /
    ``_description``).

    Verifies the post-await ``if self._closed: return`` short-circuit
    by simulating the close-flip via patching ``_call_client``.
    """
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    # Stand up a minimal fake inner connection so _execute_unlocked
    # can call ``query_raw_typed`` on it; the actual coroutine never
    # runs because we patch _call_client.
    inner = AsyncMock()
    inner.query_raw_typed = lambda _op, _params: None
    cur._connection._ensure_connection = AsyncMock(return_value=inner)
    cur._description = (("old", None, None, None, None, None, None),)
    cur._rows = [(0,)]

    async def race_call(_coro: Any) -> Any:
        # While the executor is "parked" inside the wire await, an
        # external close fires (simulated by flipping _closed here).
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

    # The executor returned cleanly; cursor remains in the post-close
    # state and was NOT repopulated with the wire response.
    assert cur._closed is True
    assert cur._rows == []
    assert cur._description is None


async def test_execute_post_await_closed_check_drops_insert_result() -> None:
    """Race scenario for the SIBLING branch: a non-query (INSERT /
    UPDATE / DDL) ``_execute_unlocked`` is parked on the wire await
    when a sibling task closes the cursor. The post-await
    ``if self._closed: return`` guard at the non-query branch must
    drop the ``(last_insert_id, rows_affected)`` tuple so
    ``_lastrowid`` / ``_rowcount`` are NOT repopulated onto the
    closed cursor.

    Mirror of ``test_execute_post_await_closed_check_drops_result``
    (which covers the query branch's symmetric guard). Without this
    pin, a regression that removes ONLY the non-query branch's guard
    would not fail any test.
    """
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    inner = AsyncMock()
    inner.execute = lambda _op, _params: None
    cur._connection._ensure_connection = AsyncMock(return_value=inner)
    cur._lastrowid = 7  # pre-existing state
    cur._rowcount = 3

    async def race_call(_coro: Any) -> Any:
        # While the executor is "parked" inside the wire await, an
        # external close fires (simulated by flipping _closed here,
        # exactly as ``close()`` would — close preserves _lastrowid /
        # _rowcount, matching stdlib, so we do NOT touch them).
        cur._closed = True
        # Non-query wire shape: (last_insert_id, rows_affected).
        return (42, 1)

    # Drive into the non-query branch by forcing ``_is_row_returning``
    # to return False (the operation text is otherwise unused on this
    # code path).
    with (
        patch("dqlitedbapi.aio.cursor._call_client", new=race_call),
        patch("dqlitedbapi.aio.cursor._is_row_returning", return_value=False),
    ):
        await cur._execute_unlocked("INSERT INTO t VALUES (1)", (1,))

    # After fix: the post-await closed-check DROPS the wire response
    # (42, 1) instead of writing it. ``_lastrowid`` stays at its sticky
    # pre-call value 7 (NOT the dropped 42; close no longer scrubs it),
    # and the closed-check arm sets ``_rowcount`` to -1 (NOT the dropped
    # 1). If the guard regressed, these would be 42 and 1.
    assert cur._closed is True
    assert cur._lastrowid == 7
    assert cur._rowcount == -1
