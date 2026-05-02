"""Pin: ``Cursor.executemany`` / ``AsyncCursor.executemany``
``except BaseException`` arms reset every per-result-set
field on mid-batch failure and re-raise.

PEP 249 §6.1.5 says ``rowcount=-1`` means undetermined.
The cycle 22 reset block uses that signal so callers
cannot mistake the LAST iteration's rowcount for the
cumulative count of successfully-applied iterations.
A regression that drops the bare ``raise`` would
silently turn ``executemany`` failures into "succeeded
with rowcount=-1"; a regression that drops one of the
field assignments leaves stale state observable.

The end-to-end behaviour is covered by integration tests
(``test_executemany_failure_resets_rowcount``,
``test_executemany_cancel_mid_batch``); these unit pins
exercise the same reset block without a live cluster
so PR-time CI catches the regression.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def _seed_post_iteration_state(cur: Cursor | AsyncCursor) -> None:
    """Mimic state that a successfully-applied iteration would
    have left behind — so the reset's effect is observable."""
    cur._rowcount = 42
    cur._rows = [(1,), (2,)]
    cur._description = (("col0", None, None, None, None, None, None),)
    cur._lastrowid = 99
    cur._row_index = 1


@pytest.mark.asyncio
async def test_sync_executemany_basecaught_resets_all_fields_and_reraises() -> None:
    conn = MagicMock()
    raised = RuntimeError("simulated mid-batch failure")

    async def _execute_then_fail(*args: object, **kwargs: object) -> None:
        raise raised

    cur = Cursor(conn)

    async def _seeded_execute(*args: object, **kwargs: object) -> None:
        _seed_post_iteration_state(cur)
        await _execute_then_fail()

    with (
        patch.object(Cursor, "_execute_async", _seeded_execute),
        pytest.raises(RuntimeError, match="simulated mid-batch"),
    ):
        await cur._executemany_async("INSERT INTO t VALUES (?)", [(1,), (2,)])

    # Every field is reset to the "no operation performed" surface;
    # rowcount=-1 (PEP 249 "undetermined"). _lastrowid is intentionally
    # NOT reset — stdlib sqlite3.Cursor.lastrowid is documented as not
    # being cleared by failed/cancelled operations, and the cursor's
    # docstring at module top pins close() as the single lifecycle
    # event that scrubs it.
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._description is None
    assert cur._lastrowid == 99  # preserved (stdlib parity)
    assert cur._row_index == 0


@pytest.mark.asyncio
async def test_async_executemany_basecaught_resets_all_fields_and_reraises() -> None:
    conn = MagicMock()
    raised = RuntimeError("simulated mid-batch failure")

    async def _execute_then_fail(*args: object, **kwargs: object) -> None:
        raise raised

    aconn_cursor = AsyncCursor(conn)

    async def _seeded_execute(*args: object, **kwargs: object) -> None:
        _seed_post_iteration_state(aconn_cursor)
        await _execute_then_fail()

    import asyncio

    op_lock = asyncio.Lock()
    aconn_cursor._connection._ensure_locks = MagicMock(return_value=(MagicMock(), op_lock))
    aconn_cursor._connection._ensure_connection = MagicMock(return_value=MagicMock())

    with (
        patch.object(AsyncCursor, "_execute_unlocked", _seeded_execute),
        pytest.raises(RuntimeError, match="simulated mid-batch"),
    ):
        await aconn_cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])

    assert aconn_cursor._rowcount == -1
    assert list(aconn_cursor._rows) == []
    assert aconn_cursor._description is None
    assert aconn_cursor._lastrowid == 99  # preserved (stdlib parity)
    assert aconn_cursor._row_index == 0
