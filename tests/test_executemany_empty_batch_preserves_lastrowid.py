"""Pin: empty ``executemany`` preserves the pre-batch ``lastrowid``
instead of clearing it to None — stdlib parity (stdlib
``sqlite3.Cursor.executemany([])`` leaves ``lastrowid`` unchanged).

Sibling `_ExecuteManyAccumulator.apply()` already special-cases the
empty-batch path for rowcount (sets rowcount=0); the symmetric
lastrowid treatment had been missed. The fix lives inside
``Cursor._executemany_async`` (sync surface) and inside the async
sibling's executemany body; both are exercised here directly so the
test is independent of the daemon-loop bridge.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest


def _make_sync_cursor(starting_lastrowid: int | None) -> object:
    from dqlitedbapi.cursor import Cursor

    cursor = Cursor.__new__(Cursor)
    cursor._closed = False
    cursor.messages = []
    cursor._arraysize = 1
    cursor._description = None
    cursor._rowcount = -1
    cursor._rows = []
    cursor._row_index = 0
    cursor._lastrowid = starting_lastrowid
    cursor._row_factory = None
    cursor._completed_iterations = 0
    conn = MagicMock()
    conn._creator_thread = None
    conn._creator_pid = -1
    conn._check_thread = lambda: None
    conn._check_pid = lambda: None
    conn._closed = False
    conn._pool_released = False
    cursor._connection = conn
    return cursor


def test_sync_empty_executemany_async_preserves_lastrowid() -> None:
    """A prior single-row INSERT set lastrowid=5; an empty-batch
    ``_executemany_async`` must leave it at 5 (not clear to None).
    Drives the inner coroutine directly to verify the fix
    (independent of the daemon-loop bridge)."""
    cursor = _make_sync_cursor(starting_lastrowid=5)
    asyncio.run(cursor._executemany_async("INSERT INTO t VALUES (?)", []))  # type: ignore[attr-defined]
    assert cursor._lastrowid == 5  # type: ignore[attr-defined]


def test_sync_empty_executemany_async_with_none_pre_batch_value() -> None:
    """Boundary: pre-batch lastrowid=None (no prior INSERT) →
    empty-batch executemany preserves the None."""
    cursor = _make_sync_cursor(starting_lastrowid=None)
    asyncio.run(cursor._executemany_async("INSERT INTO t VALUES (?)", []))  # type: ignore[attr-defined]
    assert cursor._lastrowid is None  # type: ignore[attr-defined]


# Quiet pytest import lint.
_ = pytest
