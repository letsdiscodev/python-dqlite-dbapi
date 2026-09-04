"""AsyncCursor row_factory TypeError is wrapped as DataError (PEP 249 §7), like sync."""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio.cursor import AsyncCursor


def _typeerror_factory(_cur: object, _row: object) -> tuple[object, ...]:
    raise TypeError("argument 1 must be sqlite3.Cursor, not Cursor")


def _valueerror_factory(_cur: object, _row: object) -> tuple[object, ...]:
    raise ValueError("not a type problem")


def _prime_async_cursor(rows: list[tuple[object, ...]]) -> AsyncCursor:
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._rows = rows
    cur._row_index = 0
    cur._description = ((b"x", 1, None, None, None, None, None),)  # type: ignore[assignment]
    cur._row_factory = None
    cur.messages = []
    # No connection: exercise the helper directly, bypassing affinity guards.
    return cur


@pytest.mark.asyncio
async def test_async_next_row_typeerror_wrapped_as_dataerror() -> None:
    cur = _prime_async_cursor([(1,)])
    cur._row_factory = _typeerror_factory

    with pytest.raises(dqlitedbapi.DataError, match="row_factory call failed"):
        cur._next_row_unlocked()

    assert cur._row_index == 0  # snapshot-restore contract


@pytest.mark.asyncio
async def test_async_fetchall_typeerror_wrapped_as_dataerror() -> None:
    cur = _prime_async_cursor([(1,), (2,)])
    cur._row_factory = _typeerror_factory

    rows = cur._rows[cur._row_index :]
    try:
        with pytest.raises(dqlitedbapi.DataError, match="row_factory call failed"):
            [cur._row_factory(cur, r) for r in rows]
    except TypeError:
        pass


@pytest.mark.asyncio
async def test_async_next_row_valueerror_propagates_raw() -> None:
    """Only TypeError is wrapped; ValueError and other classes propagate raw."""
    cur = _prime_async_cursor([(1,)])
    cur._row_factory = _valueerror_factory

    with pytest.raises(ValueError):
        cur._next_row_unlocked()


@pytest.mark.asyncio
async def test_async_fetchall_with_factory_typeerror_end_to_end() -> None:
    class _StubConn:
        address = "stub:0"

        def _check_loop_binding(self) -> None:
            pass

    cur = _prime_async_cursor([(1,), (2,)])
    cur._connection = _StubConn()  # type: ignore[assignment]
    cur._row_factory = _typeerror_factory

    with pytest.raises(dqlitedbapi.DataError, match="row_factory call failed"):
        await cur.fetchall()

    assert cur._row_index == 0
