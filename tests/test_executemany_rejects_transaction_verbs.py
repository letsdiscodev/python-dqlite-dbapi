"""Pin: executemany() rejects transaction-control verbs.

Stdlib ``sqlite3.Cursor.executemany`` rejects statement shapes that
take no parameters (transaction control). The dqlite dbapi previously
admitted these verbs and silently re-ran the bare statement N times
against ignored bind parameters, producing duplicate server-side
savepoint frames (compounding with the LIFO duplicate-name rule) and
generally violating the executemany contract.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError

_REJECT_VERBS = ["SAVEPOINT sp", "RELEASE sp", "ROLLBACK", "BEGIN", "COMMIT", "END"]


@pytest.mark.parametrize("statement", _REJECT_VERBS)
def test_sync_executemany_rejects_transaction_verb(statement: str) -> None:
    conn = Connection("localhost:9001")
    cursor = Cursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for"):
        cursor.executemany(statement, [(1,)])


@pytest.mark.parametrize("statement", _REJECT_VERBS)
@pytest.mark.asyncio
async def test_async_executemany_rejects_transaction_verb(statement: str) -> None:
    conn = AsyncConnection("localhost:9001")
    cursor = AsyncCursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for"):
        await cursor.executemany(statement, [(1,)])


def test_sync_executemany_rejects_lowercase_savepoint() -> None:
    """Verb match is case-insensitive."""
    conn = Connection("localhost:9001")
    cursor = Cursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for SAVEPOINT"):
        cursor.executemany("savepoint sp", [(1,)])


def test_sync_executemany_rejects_comment_prefixed_savepoint() -> None:
    """Comment stripping must apply before the verb check, mirroring
    the row-returning rejection path."""
    conn = Connection("localhost:9001")
    cursor = Cursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for SAVEPOINT"):
        cursor.executemany("/* annotation */ SAVEPOINT sp", [(1,)])


def test_sync_executemany_admits_dml_unchanged() -> None:
    """Negative pin: INSERT (and other DML) is still admitted by the
    new reject-list — ensures the carve-out doesn't accidentally widen."""
    conn = MagicMock(spec=Connection)
    conn._check_thread = MagicMock()

    # Close the coroutine the cursor would otherwise leak when
    # _run_sync is replaced with a no-op.
    def consume_coroutine(coro: object) -> None:
        if hasattr(coro, "close"):
            coro.close()

    conn._run_sync = MagicMock(side_effect=consume_coroutine)

    cursor = Cursor.__new__(Cursor)
    # Minimal field setup to satisfy executemany's preflight checks;
    # mock _run_sync so we never actually need a connection.
    cursor._connection = conn
    cursor._closed = False
    cursor._description = None
    cursor._rowcount = -1
    cursor._lastrowid = None
    cursor._rows = []
    cursor._row_index = 0
    cursor._arraysize = 1
    cursor.messages = []
    # Should not raise.
    cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
