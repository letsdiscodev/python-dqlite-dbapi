"""executemany applies the two-class rejection contract.

Input-validation rejects (None seq, bad outer shape, non-str operation, cross-task slot)
PRESERVE prior cursor state; prepare-stage rejects (verb/row-returning/PRAGMA) SCRUB it.
``_lastrowid`` is preserved across both classes.
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
    """Mimic state that a prior ``SELECT ... fetchall()`` would have left on the cursor."""
    cur._description = (
        ("a", None, None, None, None, None, None),
        ("b", None, None, None, None, None, None),
    )
    cur._rowcount = 7
    cur._rows = [(1, 2), (3, 4), (5, 6)]
    cur._row_index = 3
    cur._lastrowid = 4242
    # Only the async cursor exposes ``_completed_iterations``; seed non-zero so
    # "scrubbed" is distinguishable from "never set".
    if hasattr(cur, "_completed_iterations"):
        cur._completed_iterations = 17


def _assert_scrubbed_to_baseline(cur: Cursor | AsyncCursor) -> None:
    """Baseline after a rejected executemany: result state reset, lastrowid preserved."""
    assert cur._description is None, (
        f"description must scrub to None after rejection; got {cur._description!r}"
    )
    assert cur._rowcount == -1, (
        f"rowcount must scrub to -1 (PEP 249 undetermined); got {cur._rowcount}"
    )
    assert cur._rows == [], f"rows must scrub to []; got {cur._rows!r}"
    assert cur._row_index == 0, f"row_index must scrub to 0; got {cur._row_index}"
    assert cur._lastrowid == 4242, f"lastrowid must survive rejection; got {cur._lastrowid}"
    # Outer-dispatch rejects run ``_reset_execute_state`` before raising, scrubbing the
    # counter to 0. The inner snapshot/restore arm fires only on classifier-raise paths;
    # see ``test_executemany_classifier_resets_state_first.py``.
    if hasattr(cur, "_completed_iterations"):
        assert cur._completed_iterations == 0, (
            f"_completed_iterations must scrub to 0 after outer-dispatch "
            f"rejection; got {cur._completed_iterations}"
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


def test_sync_executemany_verb_rejection_scrubs_prior_result_state() -> None:
    cursor = _make_sync_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for BEGIN"):
        cursor.executemany("BEGIN", [(1,)])
    _assert_scrubbed_to_baseline(cursor)


def test_sync_executemany_row_returning_rejection_scrubs_prior_result_state() -> None:
    cursor = _make_sync_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="executemany.*can only execute DML"):
        cursor.executemany("SELECT 1", [(1,)])
    _assert_scrubbed_to_baseline(cursor)


def test_sync_executemany_pragma_rejection_scrubs_prior_result_state() -> None:
    cursor = _make_sync_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="executemany.*does not accept PRAGMA"):
        cursor.executemany("PRAGMA journal_mode", [(1,)])
    _assert_scrubbed_to_baseline(cursor)


async def test_async_executemany_verb_rejection_scrubs_prior_result_state() -> None:
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for BEGIN"):
        await cursor.executemany("BEGIN", [(1,)])
    _assert_scrubbed_to_baseline(cursor)


async def test_async_executemany_row_returning_rejection_scrubs_prior_result_state() -> None:
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="executemany.*can only execute DML"):
        await cursor.executemany("SELECT 1", [(1,)])
    _assert_scrubbed_to_baseline(cursor)


async def test_async_executemany_pragma_rejection_scrubs_prior_result_state() -> None:
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="executemany.*does not accept PRAGMA"):
        await cursor.executemany("PRAGMA journal_mode", [(1,)])
    _assert_scrubbed_to_baseline(cursor)


# Input-validation rejects PRESERVE prior cursor state (stdlib bare-TypeError parity).


_PRIOR_DESC = (
    ("a", None, None, None, None, None, None),
    ("b", None, None, None, None, None, None),
)
_PRIOR_ROWS = [(1, 2), (3, 4), (5, 6)]


def _assert_state_preserved(cur: Cursor | AsyncCursor) -> None:
    assert cur._description == _PRIOR_DESC, (
        f"description must be PRESERVED on input-validation reject; got {cur._description!r}"
    )
    assert cur._rowcount == 7, f"rowcount preserved; got {cur._rowcount}"
    assert cur._rows == _PRIOR_ROWS, f"rows preserved; got {cur._rows!r}"
    assert cur._row_index == 3, f"row_index preserved; got {cur._row_index}"
    assert cur._lastrowid == 4242
    if hasattr(cur, "_completed_iterations"):
        assert cur._completed_iterations == 17


def test_sync_executemany_none_seq_rejection_preserves_prior_result_state() -> None:
    cursor = _make_sync_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="seq_of_parameters must be a sequence"):
        cursor.executemany("INSERT INTO t VALUES (?)", None)  # type: ignore[arg-type]
    _assert_state_preserved(cursor)


def test_sync_executemany_bad_outer_shape_rejection_preserves_prior_result_state() -> None:
    cursor = _make_sync_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError):
        cursor.executemany("INSERT INTO t VALUES (?)", "abc")
    _assert_state_preserved(cursor)


def test_sync_executemany_non_str_operation_rejection_preserves_prior_result_state() -> None:
    cursor = _make_sync_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="operation must be a str SQL statement"):
        cursor.executemany(b"INSERT INTO t VALUES (?)", [(1,)])  # type: ignore[arg-type]
    _assert_state_preserved(cursor)


async def test_async_executemany_none_seq_rejection_preserves_prior_result_state() -> None:
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="seq_of_parameters must be a sequence"):
        await cursor.executemany("INSERT INTO t VALUES (?)", None)  # type: ignore[arg-type]
    _assert_state_preserved(cursor)


async def test_async_executemany_bad_outer_shape_rejection_preserves_prior_result_state() -> None:
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError):
        await cursor.executemany("INSERT INTO t VALUES (?)", "abc")
    _assert_state_preserved(cursor)


async def test_async_executemany_non_str_operation_rejection_preserves_prior_result_state() -> None:
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="operation must be a str SQL statement"):
        await cursor.executemany(b"INSERT INTO t VALUES (?)", [(1,)])  # type: ignore[arg-type]
    _assert_state_preserved(cursor)


async def test_async_executemany_cross_task_slot_rejection_preserves_prior_result_state() -> None:
    """Cross-task slot reject preserves prior state and leaves the foreign task's slot intact."""
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    other_task = asyncio.create_task(asyncio.sleep(60))
    try:
        cursor._executing_task = other_task
        with pytest.raises(InterfaceError, match="already executing in another task"):
            await cursor.executemany("INSERT INTO t VALUES (?)", [(1,)])
        _assert_state_preserved(cursor)
        assert cursor._executing_task is other_task, (
            "cross-task reject must leave the foreign task's slot intact"
        )
    finally:
        other_task.cancel()
        with contextlib.suppress(BaseException):
            await other_task
