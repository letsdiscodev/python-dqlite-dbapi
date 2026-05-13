"""Pin: sync ``Cursor._executemany_async`` resets per-execute cursor
state BEFORE running the hoisted SQL classifier so a classifier-raise
leaves the cursor at the no-result baseline.

Stdlib ``sqlite3.Cursor.executemany`` calls
``pysqlite_statement_reset`` BEFORE ``pysqlite_statement_prepare`` —
i.e. resets the cursor before validating SQL. The dqlite sync
``_executemany_async`` previously ran ``_classify_caller_sql`` FIRST
and ``_reset_execute_state`` SECOND, so an empty-SQL /
multi-statement / NUL classifier raise after a prior SELECT left the
cursor's ``description`` populated from that SELECT — confusing
post-raise inspection code.

The async sibling does not hoist ``_classify_caller_sql`` in its
``executemany`` body (it already resets state first), so this pin
covers only the sync surface; the existing reset-then-loop order on
the async side is already aligned with the stdlib-parity contract.

Driven via the internal ``_executemany_async`` coroutine to avoid
the wire / op_lock plumbing.
"""

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
    """Push a coroutine forward until it raises; return the exception.
    Used because ``_executemany_async`` is a coroutine and we don't
    want to spin a loop just to observe a synchronous classifier
    raise — the raise happens before any await."""
    try:
        coro.send(None)
    except StopIteration:
        pytest.fail("coroutine completed without raising")
    except BaseException as e:
        return e
    pytest.fail("coroutine did not raise")


def test_executemany_async_classifier_raise_resets_state_first_empty_sql() -> None:
    """A classifier-raise on empty SQL leaves the cursor's prior
    SELECT state scrubbed: ``description`` is ``None``, ``rowcount``
    is ``-1``, ``_rows`` is empty, ``_completed_iterations`` is 0."""
    cur = _prime_sync_cursor()

    coro = cur._executemany_async("", iter([]))
    exc = _drive_coroutine_to_first_exception(coro)
    assert isinstance(exc, ProgrammingError)
    assert "empty statement" in str(exc)

    assert cur._description is None
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._completed_iterations == 0


def test_executemany_async_classifier_raise_resets_state_first_multi_statement() -> None:
    """Multi-statement classifier raise on empty seq also resets
    state first, mirroring the empty-SQL pin."""
    cur = _prime_sync_cursor()

    coro = cur._executemany_async("SELECT 1; SELECT 2", iter([]))
    exc = _drive_coroutine_to_first_exception(coro)
    assert isinstance(exc, ProgrammingError)
    assert "one statement" in str(exc)

    assert cur._description is None
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._completed_iterations == 0


def test_executemany_async_classifier_raise_resets_state_first_nul_byte() -> None:
    """NUL-in-SQL classifier raise resets state too."""
    cur = _prime_sync_cursor()

    coro = cur._executemany_async("SELECT \x00", iter([]))
    exc = _drive_coroutine_to_first_exception(coro)
    assert isinstance(exc, ProgrammingError)
    assert "null character" in str(exc)

    assert cur._description is None
    assert cur._rowcount == -1
    assert cur._rows == []
    assert cur._completed_iterations == 0
