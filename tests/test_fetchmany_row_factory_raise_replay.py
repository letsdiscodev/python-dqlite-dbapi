"""``fetchone``/``fetchmany`` apply ``_row_factory`` before advancing ``_row_index``,
so a raising factory leaves the index unchanged (no replay, no skip on the next call)."""

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def _prime_sync_cursor(rows: list[tuple[Any, ...]]) -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._rows = rows
    cur._row_index = 0
    cur._description = (("col0", None, None, None, None, None, None),)
    cur._row_factory = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._arraysize = 1
    cur.messages = []
    conn = MagicMock()
    conn._check_thread = MagicMock()
    cur._connection = conn
    return cur


def _prime_async_cursor(rows: list[tuple[Any, ...]]) -> AsyncCursor:
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._rows = rows
    cur._row_index = 0
    cur._description = (("col0", None, None, None, None, None, None),)
    cur._row_factory = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._arraysize = 1
    cur.messages = []
    conn = MagicMock()
    conn._check_thread_for_async = MagicMock()
    cur._connection = conn
    return cur


def test_sync_fetchone_factory_raise_does_not_advance_index() -> None:
    cur = _prime_sync_cursor([("a",), ("b",), ("c",)])

    def boom(_c: object, _r: tuple[Any, ...]) -> tuple[Any, ...]:
        raise RuntimeError("simulated factory failure")

    cur._row_factory = boom

    with pytest.raises(RuntimeError, match="simulated factory failure"):
        cur.fetchone()
    assert cur._row_index == 0


def test_sync_fetchmany_factory_raise_no_replay_no_skip() -> None:
    """Factory raises on rows[2]; the next fetchone must return rows[2], not [1] or [3]."""
    cur = _prime_sync_cursor([(0,), (1,), (2,), (3,), (4,)])

    call_count = [0]

    def factory(_c: object, r: tuple[Any, ...]) -> tuple[Any, ...]:
        call_count[0] += 1
        if call_count[0] == 3:
            raise RuntimeError("simulated factory failure")
        return r

    cur._row_factory = factory

    with pytest.raises(RuntimeError):
        cur.fetchmany(size=5)

    cur._row_factory = None
    assert cur.fetchone() == (2,)


async def test_async_fetchone_factory_raise_does_not_advance_index() -> None:
    cur = _prime_async_cursor([("a",), ("b",), ("c",)])

    def boom(_c: object, _r: tuple[Any, ...]) -> tuple[Any, ...]:
        raise RuntimeError("simulated factory failure")

    cur._row_factory = boom

    with pytest.raises(RuntimeError, match="simulated factory failure"):
        await cur.fetchone()
    assert cur._row_index == 0


async def test_async_fetchmany_factory_raise_no_replay_no_skip() -> None:
    cur = _prime_async_cursor([(0,), (1,), (2,), (3,), (4,)])

    call_count = [0]

    def factory(_c: object, r: tuple[Any, ...]) -> tuple[Any, ...]:
        call_count[0] += 1
        if call_count[0] == 3:
            raise RuntimeError("simulated factory failure")
        return r

    cur._row_factory = factory

    with pytest.raises(RuntimeError):
        await cur.fetchmany(size=5)

    cur._row_factory = None
    assert await cur.fetchone() == (2,)
