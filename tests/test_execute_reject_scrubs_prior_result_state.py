"""Pin: execute distinguishes two rejection classes, matching stdlib sqlite3.

Input-validation rejections (non-str operation, cross-task slot) PRESERVE prior cursor
state; prepare-stage rejections (empty SQL, multi-statement, NUL byte, bind-count) SCRUB it.
``_lastrowid`` is cursor-scoped and intentionally preserved across BOTH classes.
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

_PRIOR_DESCRIPTION = (
    ("a", None, None, None, None, None, None),
    ("b", None, None, None, None, None, None),
)
_PRIOR_ROWS = [(1, 2), (3, 4), (5, 6)]


def _seed_prior_select_state(cur: Cursor | AsyncCursor) -> None:
    """Mimic state a prior ``SELECT ... fetchall()`` would have left on the cursor."""
    cur._description = _PRIOR_DESCRIPTION
    cur._rowcount = 7
    cur._rows = list(_PRIOR_ROWS)
    cur._row_index = 3
    cur._lastrowid = 4242
    if hasattr(cur, "_completed_iterations"):
        cur._completed_iterations = 17


def _assert_state_preserved(cur: Cursor | AsyncCursor) -> None:
    """Input-validation reject preserves prior cursor state."""
    assert cur._description == _PRIOR_DESCRIPTION, (
        f"description must be PRESERVED on input-validation reject; got {cur._description!r}"
    )
    assert cur._rowcount == 7, (
        f"rowcount must be PRESERVED on input-validation reject; got {cur._rowcount}"
    )
    assert cur._rows == _PRIOR_ROWS, f"rows must be PRESERVED; got {cur._rows!r}"
    assert cur._row_index == 3, f"row_index must be PRESERVED; got {cur._row_index}"
    assert cur._lastrowid == 4242, f"lastrowid must survive; got {cur._lastrowid}"
    if hasattr(cur, "_completed_iterations"):
        assert cur._completed_iterations == 17


def _assert_state_scrubbed(cur: Cursor | AsyncCursor) -> None:
    """Prepare-stage reject scrubs cursor state to the "no result set" baseline; lastrowid
    survives."""
    assert cur._description is None, (
        f"description must SCRUB on prepare-stage reject; got {cur._description!r}"
    )
    assert cur._rowcount == -1, (
        f"rowcount must SCRUB to -1 (PEP 249 undetermined); got {cur._rowcount}"
    )
    assert cur._rows == [], f"rows must SCRUB to []; got {cur._rows!r}"
    assert cur._row_index == 0, f"row_index must SCRUB to 0; got {cur._row_index}"
    assert cur._lastrowid == 4242, f"lastrowid must survive scrub; got {cur._lastrowid}"


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
    cursor._completed_iterations = 0
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


def test_sync_execute_non_str_operation_preserves_prior_result_state() -> None:
    cursor = _make_sync_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="operation must be a str SQL statement"):
        cursor.execute(b"SELECT 1")  # type: ignore[arg-type]
    _assert_state_preserved(cursor)


async def test_async_execute_non_str_operation_preserves_prior_result_state() -> None:
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="operation must be a str SQL statement"):
        await cursor.execute(b"SELECT 1")  # type: ignore[arg-type]
    _assert_state_preserved(cursor)


async def test_async_execute_cross_task_slot_rejection_preserves_prior_result_state() -> None:
    """Cross-task slot reject (no stdlib analog) preserves state and the foreign task's slot."""
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    other_task = asyncio.create_task(asyncio.sleep(60))
    try:
        cursor._executing_task = other_task
        with pytest.raises(InterfaceError, match="already executing in another task"):
            await cursor.execute("SELECT 1")
        _assert_state_preserved(cursor)
        assert cursor._executing_task is other_task, (
            "cross-task reject must leave the foreign task's slot intact"
        )
    finally:
        other_task.cancel()
        with contextlib.suppress(BaseException):
            await other_task


def test_sync_execute_empty_sql_rejection_scrubs_prior_result_state() -> None:
    """Empty SQL is a prepare-stage rejection, so it clears ``cur.description``."""
    cursor = _make_sync_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError):
        cursor.execute("")
    _assert_state_scrubbed(cursor)


async def test_async_execute_empty_sql_rejection_scrubs_prior_result_state() -> None:
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError):
        await cursor.execute("")
    _assert_state_scrubbed(cursor)
