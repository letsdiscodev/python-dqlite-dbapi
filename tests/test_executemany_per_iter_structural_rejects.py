"""``Cursor._executemany_async`` per-iteration structural-type rejects (sync side)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError


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
    conn._max_total_rows = None
    cur._connection = conn
    return cur


def _drive_coroutine_to_first_exception(coro: Any) -> BaseException:
    """Push a coroutine forward until it raises; return the exception.

    The per-iter structural rejects raise synchronously before any awaitable yields.
    """
    try:
        coro.send(None)
    except StopIteration:
        pytest.fail("coroutine completed without raising")
    except BaseException as e:
        return e
    pytest.fail("coroutine did not raise")


@pytest.mark.parametrize(
    ("bad_row", "match"),
    [
        ("abc", "parameters must be a sequence of values"),
        (b"abc", "parameters must be a sequence of values"),
        (bytearray(b"abc"), "parameters must be a sequence of values"),
        (memoryview(b"abc"), "parameters must be a sequence of values"),
        ({"a": 1}, "qmark paramstyle requires a sequence; got a mapping"),
        ({1, 2}, "qmark paramstyle requires an ordered sequence; got a set"),
        (frozenset((1, 2)), "qmark paramstyle requires an ordered sequence; got a set"),
    ],
)
def test_executemany_async_per_iter_rejects_structural_shapes(bad_row: object, match: str) -> None:
    """Each structural-type per-iter row raises ``ProgrammingError`` before reaching the wire."""
    cur = _prime_sync_cursor()
    coro = cur._executemany_async("INSERT INTO t VALUES (?)", iter([bad_row]))  # type: ignore[arg-type]
    exc = _drive_coroutine_to_first_exception(coro)
    assert isinstance(exc, ProgrammingError)
    assert match in str(exc)
    assert cur._completed_iterations == 0


def test_executemany_async_per_iter_count_mismatch_after_structural_pass() -> None:
    """A wrong-arity sequence row hits the count-mismatch arm; no iteration completes."""
    cur = _prime_sync_cursor()
    coro = cur._executemany_async("INSERT INTO t VALUES (?)", iter([(1, 2, 3)]))
    exc = _drive_coroutine_to_first_exception(coro)
    assert isinstance(exc, ProgrammingError)
    assert "Incorrect number of bindings supplied" in str(exc)
    assert "uses 1, and there are 3 supplied" in str(exc)
    assert cur._completed_iterations == 0
