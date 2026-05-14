"""Pin: ``Cursor.executemany`` / ``AsyncCursor.executemany`` reject paths
scrub prior per-result-set state (description / rowcount / rows / row_index)
BEFORE raising ``ProgrammingError``.

Stdlib ``sqlite3.Cursor.executemany`` resets these fields on entry, so a
rejected batch leaves the cursor at the "no result set" baseline. The
dqlite dbapi previously fired the verb-reject / row-returning-reject
guards BEFORE ``_reset_execute_state`` ran, leaving the prior
``SELECT``'s description, rowcount, and row buffer observable on the
cursor after a ``ProgrammingError``. A SQLAlchemy / pandas adapter that
introspects ``cur.description`` after a rejected batch would see stale
shape.

``_lastrowid`` is the documented exception: it is cursor-scoped and
intentionally preserved across rejection (see
``test_executemany_rejects_transaction_verbs.py::
test_sync_executemany_rejection_preserves_prior_lastrowid``). The two
contracts coexist because ``_reset_execute_state`` does not touch
``_lastrowid``.
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
    # Only the async cursor exposes ``_completed_iterations`` (mid-loop
    # progress counter). Seed it to a non-zero value so that "scrubbed"
    # is distinguishable from "never set" — ``_reset_execute_state``
    # co-scrubs this sixth field per its docstring.
    if hasattr(cur, "_completed_iterations"):
        cur._completed_iterations = 17


def _assert_scrubbed_to_baseline(cur: Cursor | AsyncCursor) -> None:
    """Stdlib-parity baseline after a rejected executemany:
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
    # Async cursor only: ``_reset_execute_state`` zeroes the mid-loop
    # progress counter so an empty / rejected ``seq_of_parameters``
    # ends with the same shape as empty ``execute``.
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


# Input-validation rejects (None seq, bad outer shape, non-str operation,
# async cross-task slot). Same stdlib-parity baseline contract: cursor
# state must scrub before the guard raises so a caller catching the
# rejection observes the "no result set" shape rather than the prior
# SELECT's description / rowcount / rows / row_index.


def test_sync_executemany_none_seq_rejection_scrubs_prior_result_state() -> None:
    cursor = _make_sync_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="seq_of_parameters must be a sequence"):
        cursor.executemany("INSERT INTO t VALUES (?)", None)  # type: ignore[arg-type]
    _assert_scrubbed_to_baseline(cursor)


def test_sync_executemany_bad_outer_shape_rejection_scrubs_prior_result_state() -> None:
    cursor = _make_sync_cursor()
    _seed_prior_select_state(cursor)
    # ``str`` is one of the rejected outer shapes (would iterate over chars).
    with pytest.raises(ProgrammingError):
        cursor.executemany("INSERT INTO t VALUES (?)", "abc")
    _assert_scrubbed_to_baseline(cursor)


def test_sync_executemany_non_str_operation_rejection_scrubs_prior_result_state() -> None:
    cursor = _make_sync_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="operation must be a str SQL statement"):
        cursor.executemany(b"INSERT INTO t VALUES (?)", [(1,)])  # type: ignore[arg-type]
    _assert_scrubbed_to_baseline(cursor)


async def test_async_executemany_none_seq_rejection_scrubs_prior_result_state() -> None:
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="seq_of_parameters must be a sequence"):
        await cursor.executemany("INSERT INTO t VALUES (?)", None)  # type: ignore[arg-type]
    _assert_scrubbed_to_baseline(cursor)


async def test_async_executemany_bad_outer_shape_rejection_scrubs_prior_result_state() -> None:
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError):
        await cursor.executemany("INSERT INTO t VALUES (?)", "abc")
    _assert_scrubbed_to_baseline(cursor)


async def test_async_executemany_non_str_operation_rejection_scrubs_prior_result_state() -> None:
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    with pytest.raises(ProgrammingError, match="operation must be a str SQL statement"):
        await cursor.executemany(b"INSERT INTO t VALUES (?)", [(1,)])  # type: ignore[arg-type]
    _assert_scrubbed_to_baseline(cursor)


async def test_async_executemany_cross_task_slot_rejection_scrubs_prior_result_state() -> None:
    """The cross-task slot reject (``_executing_task is not None`` and not
    the current task) raises ``InterfaceError`` BEFORE the
    ``_reset_execute_state`` call. Same stdlib-parity baseline contract."""
    cursor = _make_async_cursor()
    _seed_prior_select_state(cursor)
    # Pin the slot to a sentinel task that is not the current one. Use
    # a freshly-scheduled task so its identity differs from the test's
    # running task; cancel it so it doesn't outlive the test.
    other_task = asyncio.create_task(asyncio.sleep(60))
    try:
        cursor._executing_task = other_task
        with pytest.raises(InterfaceError, match="already executing in another task"):
            await cursor.executemany("INSERT INTO t VALUES (?)", [(1,)])
        _assert_scrubbed_to_baseline(cursor)
    finally:
        other_task.cancel()
        with contextlib.suppress(BaseException):
            await other_task
