"""Empty ``executemany`` preserves the pre-batch ``lastrowid`` (stdlib parity:
``sqlite3.Cursor.executemany([])`` leaves it unchanged) rather than clearing to None."""

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
    """Prior INSERT set lastrowid=5; empty-batch executemany must leave it at 5."""
    cursor = _make_sync_cursor(starting_lastrowid=5)
    asyncio.run(cursor._executemany_async("INSERT INTO t VALUES (?)", []))  # type: ignore[attr-defined]
    assert cursor._lastrowid == 5  # type: ignore[attr-defined]


def test_sync_empty_executemany_async_with_none_pre_batch_value() -> None:
    """Boundary: pre-batch lastrowid=None is preserved as None."""
    cursor = _make_sync_cursor(starting_lastrowid=None)
    asyncio.run(cursor._executemany_async("INSERT INTO t VALUES (?)", []))  # type: ignore[attr-defined]
    assert cursor._lastrowid is None  # type: ignore[attr-defined]


# Quiet pytest import lint.
_ = pytest
