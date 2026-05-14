"""Pin: ``Cursor.execute`` / ``AsyncCursor.execute`` input-validation reject
paths scrub prior per-result-set state (description / rowcount / rows /
row_index) BEFORE raising.

Stdlib ``sqlite3.Cursor.execute`` resets these fields on entry, so a
rejected call leaves the cursor at the "no result set" baseline. The
dqlite dbapi previously fired the non-str ``operation`` reject (sync
and async) and the async cross-task slot reject BEFORE
``_reset_execute_state`` ran, leaving the prior ``SELECT``'s
description, rowcount, and row buffer observable on the cursor after
a ``ProgrammingError`` / ``InterfaceError``. A cross-driver
``try: cur.execute(maybe_bytes_sql) / except dbapi.Error: cur.fetchall()``
recovery path would read stale rows.

Sibling pattern: ``test_executemany_reject_scrubs_prior_result_state.py``
pinned the same shape on the ``executemany`` entry point.

``_lastrowid`` is the documented exception: it is cursor-scoped and
intentionally preserved across rejection. ``_reset_execute_state``
deliberately does not touch ``_lastrowid`` or ``_executing_task``.
"""

from __future__ import annotations

import asyncio
import contextlib
from unittest.mock import MagicMock

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import InterfaceError, ProgrammingError


def _seed_prior_select_state(cur: Cursor | AsyncCursor) -> None:
    """Mimic state that a prior ``SELECT ... fetchall()`` would have
    left on the cursor."""
    cur._description = (
        ("a", None, None, None, None, None, None),
        ("b", None, None, None, None, None, None),
    )
    cur._rowcount = 7
    cur._rows = [(1, 2), (3, 4), (5, 6)]
    cur._row_index = 3
    cur._lastrowid = 4242
    if hasattr(cur, "_completed_iterations"):
        cur._completed_iterations = 17


def _assert_scrubbed_to_baseline(cur: Cursor | AsyncCursor) -> None:
    """Stdlib-parity baseline after a rejected execute:
    description / rowcount / rows / row_index reset; lastrowid preserved."""
    assert cur._description is None, (
        f"description must scrub to None after rejection; got {cur._description!r}"
    )
    assert cur._rowcount == -1, (
        f"rowcount must scrub to -1 (PEP 249 undetermined); got {cur._rowcount}"
    )
    assert cur._rows == [], f"rows must scrub to []; got {cur._rows!r}"
    assert cur._row_index == 0, f"row_index must scrub to 0; got {cur._row_index}"
    # Per the lastrowid lifecycle contract, rejection preserves it.
    assert cur._lastrowid == 4242, f"lastrowid must survive rejection; got {cur._lastrowid}"
    if hasattr(cur, "_completed_iterations"):
        assert cur._completed_iterations == 0, (
            f"_completed_iterations must scrub to 0 after rejection; "
            f"got {cur._completed_iterations}"
        )


def _make_sync_cursor() -> Cursor:
    cursor = Cursor.__new__(Cursor)
    cursor._closed = False
    cursor._description = None
    cursor._rowcount = -1
    cursor._row_factory = None
    cursor._rows = []
    cursor._row_index = 0
    cursor._arraysize = 1
    cursor.messages = []
    cursor._lastrowid = None
    conn = MagicMock(spec=Connection)
    conn._check_thread = MagicMock()
    conn._run_sync = MagicMock(
        side_effect=AssertionError("rejection must short-circuit before _run_sync")
    )
    cursor._connection = conn
    return cursor


def _make_async_cursor() -> AsyncCursor:
    cursor = AsyncCursor.__new__(AsyncCursor)
    cursor._closed = False
    cursor._description = None
    cursor._rowcount = -1
    cursor._row_factory = None
    cursor._rows = []
    cursor._row_index = 0
    cursor._arraysize = 1
    cursor.messages = []
    cursor._executing_task = None
    cursor._completed_iterations = 0
    cursor._lastrowid = None
    cursor._connection = MagicMock(spec=AsyncConnection)
    return cursor


def test_sync_execute_non_str_operation_rejection_scrubs_prior_result_state() -> None:
    cursor = _make_sync_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="operation must be a str SQL statement"):
        cursor.execute(b"SELECT 1")  # type: ignore[arg-type]
    _assert_scrubbed_to_baseline(cursor)


async def test_async_execute_non_str_operation_rejection_scrubs_prior_result_state() -> None:
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="operation must be a str SQL statement"):
        await cursor.execute(b"SELECT 1")  # type: ignore[arg-type]
    _assert_scrubbed_to_baseline(cursor)


async def test_async_execute_cross_task_slot_rejection_scrubs_prior_result_state() -> None:
    """The cross-task slot reject (``_executing_task is not None`` and not
    the current task) raises ``InterfaceError`` BEFORE the
    ``_reset_execute_state`` call. Same stdlib-parity baseline contract."""
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    # Pin the slot to a sentinel task that is not the current one.
    other_task = asyncio.create_task(asyncio.sleep(60))
    try:
        cursor._executing_task = other_task
        with pytest.raises(InterfaceError, match="already executing in another task"):
            await cursor.execute("SELECT 1")
        _assert_scrubbed_to_baseline(cursor)
        # The cross-task slot reject must NOT clear the foreign task's
        # slot (it belongs to the other task).
        assert cursor._executing_task is other_task, (
            "cross-task reject must leave the foreign task's slot intact"
        )
    finally:
        other_task.cancel()
        with contextlib.suppress(BaseException):
            await other_task
