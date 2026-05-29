"""Cursor.executemany rejects the same outer shapes (str/bytes/bytearray/
memoryview/dict/set/frozenset) as the Connection.executemany shortcuts.
Otherwise executemany(sql, "abc") silently iterates char-by-char. Shared
helper _validate_executemany_seq_shape is the single source of truth."""

from __future__ import annotations

import os
import threading
from typing import Any, cast

import pytest

import dqlitedbapi
import dqlitedbapi.aio
from dqlitedbapi.connection import Connection as SyncConnection
from dqlitedbapi.exceptions import ProgrammingError


def _sync_cursor() -> Any:
    """Connection/Cursor without dialing — enough to drive the up-front
    shape validation on executemany."""
    conn = cast(Any, SyncConnection.__new__(SyncConnection))
    conn._closed = False
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn._cursors = []
    cur = cast(Any, dqlitedbapi.Cursor.__new__(dqlitedbapi.Cursor))
    cur._closed = False
    cur._connection = conn
    cur.messages = []
    return cur


def _bare_async_cursor() -> Any:
    """Async sibling of _sync_cursor (see test_executemany_none_seq_rejected
    for the _loop_ref / _creator_pid attribute rationale)."""
    aconn = cast(Any, dqlitedbapi.aio.AsyncConnection.__new__(dqlitedbapi.aio.AsyncConnection))
    aconn._closed = False
    aconn._creator_pid = os.getpid()
    aconn._loop_ref = None
    acur = cast(Any, dqlitedbapi.aio.AsyncCursor.__new__(dqlitedbapi.aio.AsyncCursor))
    acur._closed = False
    acur._connection = aconn
    acur._executing_task = None
    acur.messages = []
    return acur


_BAD_SHAPES: list[Any] = [
    "abc",
    b"abc",
    bytearray(b"abc"),
    memoryview(b"abc"),
    {"a": 1},
    {1, 2, 3},
    frozenset((1, 2, 3)),
]


@pytest.mark.parametrize("bad_shape", _BAD_SHAPES)
def test_sync_cursor_executemany_rejects_outer_shape(bad_shape: Any) -> None:
    cur = _sync_cursor()
    with pytest.raises(ProgrammingError, match="must be an iterable of parameter sets"):
        cur.executemany("INSERT INTO t VALUES (?)", bad_shape)


@pytest.mark.parametrize("bad_shape", _BAD_SHAPES)
async def test_async_cursor_executemany_rejects_outer_shape(bad_shape: Any) -> None:
    acur = _bare_async_cursor()
    with pytest.raises(ProgrammingError, match="must be an iterable of parameter sets"):
        await acur.executemany("INSERT INTO t VALUES (?)", bad_shape)
