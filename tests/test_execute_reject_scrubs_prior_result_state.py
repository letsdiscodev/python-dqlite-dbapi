"""Pin: ``Cursor.execute`` / ``AsyncCursor.execute`` distinguish two
classes of rejection, matching stdlib ``sqlite3``:

1. **Input-validation rejection** (non-str ``operation``, cross-task
   slot mismatch). These are caller-shape misuses fired before any
   SQL parser touches the bytes. Stdlib `sqlite3` preserves prior
   cursor state on the TypeError path — verified empirically:

       >>> import sqlite3
       >>> con = sqlite3.connect(":memory:")
       >>> cur = con.cursor()
       >>> cur.execute("CREATE TABLE t(a)")
       ... cur.execute("SELECT * FROM t")
       >>> prior_desc = cur.description
       >>> try:
       ...     cur.execute(123)
       ... except TypeError:
       ...     pass
       >>> cur.description == prior_desc
       True

   The dqlite driver matches: input-validation rejections leave
   ``description`` / ``rowcount`` / ``rows`` / ``row_index`` intact
   so a retry-with-coerce idiom can inspect them.

2. **Prepare-stage rejection** (empty SQL, multi-statement, NUL byte,
   wrong ``?``-count). Stdlib SCRUBS prior state on these — verified:

       >>> cur.execute("SELECT * FROM t")
       >>> try:
       ...     cur.execute("")
       ... except sqlite3.ProgrammingError:
       ...     pass
       >>> cur.description is None
       True

   The dqlite driver matches: prepare-stage rejections reset state.

Sibling pattern: ``test_executemany_reject_scrubs_prior_result_state.py``
pins the same shape on the ``executemany`` entry point.

``_lastrowid`` is the documented exception: it is cursor-scoped and
intentionally preserved across BOTH classes of rejection.
``_reset_execute_state`` deliberately does not touch ``_lastrowid``
or ``_executing_task``.
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
    """Mimic state that a prior ``SELECT ... fetchall()`` would have
    left on the cursor."""
    cur._description = _PRIOR_DESCRIPTION
    cur._rowcount = 7
    cur._rows = list(_PRIOR_ROWS)
    cur._row_index = 3
    cur._lastrowid = 4242
    if hasattr(cur, "_completed_iterations"):
        cur._completed_iterations = 17


def _assert_state_preserved(cur: Cursor | AsyncCursor) -> None:
    """Input-validation reject: stdlib preserves prior cursor state."""
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
        # _completed_iterations is also part of result state; preserve.
        assert cur._completed_iterations == 17


def _assert_state_scrubbed(cur: Cursor | AsyncCursor) -> None:
    """Prepare-stage reject: stdlib scrubs prior cursor state to
    the "no result set" baseline. lastrowid is preserved."""
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


# ---------------- Input-validation: PRESERVE ----------------


def test_sync_execute_non_str_operation_preserves_prior_result_state() -> None:
    """Stdlib parity: ``cur.execute(123)`` raises TypeError (we surface
    ProgrammingError) and PRESERVES ``cur.description`` / rowcount /
    rows. Verified against CPython 3.x stdlib `sqlite3` directly."""
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
    """The cross-task slot reject is a project-specific misuse-rejection
    (no stdlib analog because sqlite3 is sync-only). It follows the
    input-validation pattern: PRESERVE prior cursor state, leave the
    foreign task's slot intact."""
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


# ---------------- Prepare-stage: SCRUB ----------------


def test_sync_execute_empty_sql_rejection_scrubs_prior_result_state() -> None:
    """Stdlib parity: ``cur.execute("")`` raises ProgrammingError AND
    clears ``cur.description`` because empty SQL is a prepare-stage
    rejection (the parser saw nothing to compile)."""
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
