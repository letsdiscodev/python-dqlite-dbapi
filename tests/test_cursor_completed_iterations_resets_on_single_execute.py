"""completed_iterations must reset to 0 on a single-row execute even after
a prior executemany left a non-zero count: the reset belongs in
_reset_execute_state (one source of truth), not in _executemany_async."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def _prime_sync_cursor() -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._rows = []
    cur._row_index = 0
    cur._description = None
    cur._row_factory = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._arraysize = 1
    cur._completed_iterations = 0
    cur.messages = []
    conn = MagicMock()
    conn._check_thread = MagicMock()
    cur._connection = conn
    return cur


def _prime_async_cursor() -> AsyncCursor:
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._rows = []
    cur._row_index = 0
    cur._description = None
    cur._row_factory = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._arraysize = 1
    cur._completed_iterations = 0
    cur.messages = []
    cur._connection = MagicMock()
    return cur


def test_sync_reset_execute_state_clears_completed_iterations() -> None:
    cur = _prime_sync_cursor()
    cur._completed_iterations = 5

    cur._reset_execute_state()
    assert cur._completed_iterations == 0


def test_async_reset_execute_state_clears_completed_iterations() -> None:
    cur = _prime_async_cursor()
    cur._completed_iterations = 7

    cur._reset_execute_state()
    assert cur._completed_iterations == 0


def test_sync_execute_resets_completed_iterations_via_reset_helper() -> None:
    cur = _prime_sync_cursor()
    cur._completed_iterations = 9

    def _run_sync(coro: Any) -> None:
        coro.close()  # avoid unawaited-coroutine warning

    cur._connection._run_sync = _run_sync  # type: ignore[assignment]
    cur.execute("SELECT 1")
    assert cur._completed_iterations == 0
