"""``executemany`` rejects pure queries before running the loop.

Summing ``len(rows)`` across N iterations of ``SELECT ?`` produces an
``N × rows_per_iter`` total that is not a semantically meaningful
"affected" count. stdlib ``sqlite3.Cursor.executemany`` rejects this
up front; dqlitedbapi now does the same. DML with or without a
RETURNING clause (INSERT / UPDATE / DELETE / REPLACE) remains
admitted.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError


def _make_sync_cursor() -> Cursor:
    conn = MagicMock()
    conn.messages = []
    conn._check_thread = MagicMock()
    conn._run_sync = MagicMock()
    cur = Cursor(conn)
    return cur


class TestSyncExecutemanyRejectsPureQueries:
    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT ?",
            "SELECT id FROM t WHERE id = ?",
            "VALUES (?)",
            "PRAGMA foreign_keys",
            "WITH t(x) AS (VALUES (?)) SELECT * FROM t",
        ],
    )
    def test_rejects_pure_query(self, sql: str) -> None:
        cur = _make_sync_cursor()
        with pytest.raises(ProgrammingError, match="DML"):
            cur.executemany(sql, [(1,), (2,)])

    @pytest.mark.parametrize(
        "sql",
        [
            "INSERT INTO t VALUES (?)",
            "UPDATE t SET v = ? WHERE id = 1",
            "DELETE FROM t WHERE id = ?",
            "REPLACE INTO t VALUES (?)",
            "INSERT INTO t VALUES (?) RETURNING id",
            "UPDATE t SET v = ? RETURNING v",
        ],
    )
    def test_admits_dml(self, sql: str) -> None:
        cur = _make_sync_cursor()
        # Must NOT raise ProgrammingError at the gate. We don't drive
        # the actual wire call — _run_sync is a MagicMock.
        with patch.object(Cursor, "_executemany_async"):
            cur.executemany(sql, [])  # empty seq — short-circuits


class TestAsyncExecutemanyRejectsPureQueries:
    def test_rejects_pure_query(self) -> None:
        from dqlitedbapi.aio.connection import AsyncConnection
        from dqlitedbapi.aio.cursor import AsyncCursor

        async def _run() -> None:
            conn = AsyncConnection("localhost:19001")
            cur = AsyncCursor(conn)
            with pytest.raises(ProgrammingError, match="DML"):
                await cur.executemany("SELECT ?", [(1,), (2,)])
            await conn.close()

        asyncio.run(_run())
