"""``setinputsizes`` rejects non-Sequence iterables (int/dict) via the Sequence-ABC arm."""

from __future__ import annotations

import collections
from typing import Any

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError


@pytest.mark.parametrize("bad_sizes", [42, {"k": 1}])
def test_sync_setinputsizes_rejects_non_sequence_iterable(bad_sizes: Any) -> None:
    conn = Connection("localhost:9001", timeout=2.0)
    cur = Cursor(conn)
    with pytest.raises(ProgrammingError, match="expects a Sequence"):
        cur.setinputsizes(bad_sizes)


@pytest.mark.parametrize("bad_sizes", [42, {"k": 1}])
async def test_async_setinputsizes_rejects_non_sequence_iterable(bad_sizes: Any) -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    with pytest.raises(ProgrammingError, match="expects a Sequence"):
        cur.setinputsizes(bad_sizes)


def test_sync_setinputsizes_accepts_deque_and_range() -> None:
    """Sequence-ABC dispatch accepts deque/range (psycopg2/stdlib parity)."""
    conn = Connection("localhost:9001", timeout=2.0)
    cur = Cursor(conn)
    cur.setinputsizes(collections.deque([1, 2]))
    cur.setinputsizes(range(3))


async def test_async_setinputsizes_accepts_deque_and_range() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    cur.setinputsizes(collections.deque([1, 2]))
    cur.setinputsizes(range(3))


def test_sync_setinputsizes_rejects_memoryview() -> None:
    """memoryview satisfies Sequence so it needs explicit rejection alongside str/bytes."""
    conn = Connection("localhost:9001", timeout=2.0)
    cur = Cursor(conn)
    with pytest.raises(ProgrammingError, match="size hints"):
        cur.setinputsizes(memoryview(b"ab"))


async def test_async_setinputsizes_rejects_memoryview() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    with pytest.raises(ProgrammingError, match="size hints"):
        cur.setinputsizes(memoryview(b"ab"))
