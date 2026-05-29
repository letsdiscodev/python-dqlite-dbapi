"""Sync ``Cursor._executemany_async`` resets cursor state BEFORE the hoisted SQL
classifier (stdlib parity: sqlite3 resets before prepare), so a classifier-raise
leaves the no-result baseline rather than a prior SELECT's description."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError


def _prime_sync_cursor() -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._rows = [("prior",)]
    cur._row_index = 0
    cur._description = (("col0", None, None, None, None, None, None),)
    cur._row_factory = None
    cur._rowcount = 7
    cur._lastrowid = None
    cur._arraysize = 1
    cur._completed_iterations = 5
    cur.messages = []
    conn = MagicMock()
    conn._check_thread = MagicMock()
    conn._max_total_rows = None
    cur._connection = conn
    return cur


def _drive_coroutine_to_first_exception(coro: Any) -> BaseException:
    """Send a coroutine forward once; return the exception it raises (before any await)."""
    try:
        coro.send(None)
    except StopIteration:
        pytest.fail("coroutine completed without raising")
    except BaseException as e:
        return e
    pytest.fail("coroutine did not raise")


def test_executemany_async_classifier_raise_resets_state_first_empty_sql() -> None:
    """Empty-SQL classifier raise scrubs prior SELECT state; _completed_iterations
    restored to the pre-batch snapshot (5)."""
    cur = _prime_sync_cursor()

    coro = cur._executemany_async("", iter([]))
    exc = _drive_coroutine_to_first_exception(coro)
    assert isinstance(exc, ProgrammingError)
    assert "empty statement" in str(exc)

    assert cur._description is None
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._completed_iterations == 5


def test_executemany_async_classifier_raise_resets_state_first_multi_statement() -> None:
    """Multi-statement classifier raise also resets state first; counter restored to 5."""
    cur = _prime_sync_cursor()

    coro = cur._executemany_async("SELECT 1; SELECT 2", iter([]))
    exc = _drive_coroutine_to_first_exception(coro)
    assert isinstance(exc, ProgrammingError)
    assert "one statement" in str(exc)

    assert cur._description is None
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._completed_iterations == 5


def test_executemany_async_classifier_raise_resets_state_first_nul_byte() -> None:
    """NUL-in-SQL classifier raise resets state too; counter restored to 5."""
    cur = _prime_sync_cursor()

    coro = cur._executemany_async("SELECT \x00", iter([]))
    exc = _drive_coroutine_to_first_exception(coro)
    assert isinstance(exc, ProgrammingError)
    assert "null character" in str(exc)

    assert cur._description is None
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._completed_iterations == 5
