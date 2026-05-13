"""Pin: ``fetchall`` applies ``_row_factory`` BEFORE advancing
``_row_index``. A factory that raises must leave the index unchanged
so the rows can be re-fetched.

Companion to ``test_fetchmany_row_factory_raise_replay`` which pins
the discipline for ``fetchone`` and ``fetchmany``. ``fetchall`` was
the asymmetric outlier with no test pin at either the sync or async
surface.

The sync surface carries a documented ``try/except BaseException:
raise`` arm; the async surface relies on Python's list-
comprehension-raises-before-the-next-statement semantics implicitly.
Both produce the same observable behaviour today (index preserved on
factory raise); this file pins the contract end-to-end so a future
refactor that hoists the comprehension into an ``await`` helper
(async side) or that swaps the index-advance / comprehension order
(sync side) lights up.
"""

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


def test_sync_fetchall_factory_raise_does_not_advance_index() -> None:
    """A factory raise out of fetchall must leave ``_row_index``
    unchanged so a subsequent fetchall (e.g. after the caller swaps
    in a working factory) returns ALL rows, not the tail."""
    cur = _prime_sync_cursor([("a",), ("b",), ("c",)])

    def boom(_c: object, _r: tuple[Any, ...]) -> tuple[Any, ...]:
        raise RuntimeError("simulated factory failure")

    cur._row_factory = boom
    pre_index = cur._row_index

    with pytest.raises(RuntimeError, match="simulated factory failure"):
        cur.fetchall()
    assert cur._row_index == pre_index

    # Retry with a working factory returns all rows.
    cur._row_factory = None
    assert cur.fetchall() == [("a",), ("b",), ("c",)]


def test_sync_fetchall_factory_raise_midstream_index_unchanged() -> None:
    """3 rows; factory raises on the 2nd row. The full ``fetchall``
    aborts and ``_row_index`` is unchanged — no partial advance."""
    cur = _prime_sync_cursor([(0,), (1,), (2,)])

    call_count = [0]

    def factory(_c: object, r: tuple[Any, ...]) -> tuple[Any, ...]:
        call_count[0] += 1
        if call_count[0] == 2:
            raise RuntimeError("simulated factory failure")
        return r

    cur._row_factory = factory

    with pytest.raises(RuntimeError):
        cur.fetchall()
    assert cur._row_index == 0

    # Neutralise factory; the rows are still available start-to-end.
    cur._row_factory = None
    assert cur.fetchall() == [(0,), (1,), (2,)]


async def test_async_fetchall_factory_raise_does_not_advance_index() -> None:
    """Async-sibling parity. The current implementation relies on the
    list-comprehension-raises-before-the-next-statement Python
    semantic; a future hoist that interposed an ``await`` between the
    comprehension and the index advance would break this."""
    cur = _prime_async_cursor([("a",), ("b",), ("c",)])

    def boom(_c: object, _r: tuple[Any, ...]) -> tuple[Any, ...]:
        raise RuntimeError("simulated factory failure")

    cur._row_factory = boom
    pre_index = cur._row_index

    with pytest.raises(RuntimeError, match="simulated factory failure"):
        await cur.fetchall()
    assert cur._row_index == pre_index

    cur._row_factory = None
    assert await cur.fetchall() == [("a",), ("b",), ("c",)]


async def test_async_fetchall_factory_raise_midstream_index_unchanged() -> None:
    cur = _prime_async_cursor([(0,), (1,), (2,)])

    call_count = [0]

    def factory(_c: object, r: tuple[Any, ...]) -> tuple[Any, ...]:
        call_count[0] += 1
        if call_count[0] == 2:
            raise RuntimeError("simulated factory failure")
        return r

    cur._row_factory = factory

    with pytest.raises(RuntimeError):
        await cur.fetchall()
    assert cur._row_index == 0

    cur._row_factory = None
    assert await cur.fetchall() == [(0,), (1,), (2,)]
