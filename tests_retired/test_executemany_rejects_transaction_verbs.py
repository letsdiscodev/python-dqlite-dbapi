"""executemany() rejects transaction-control verbs (which take no parameters)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError

_REJECT_VERBS = ["SAVEPOINT sp", "RELEASE sp", "ROLLBACK", "BEGIN", "COMMIT", "END"]

# No-operand verbs glued to a trailing semicolon (canonicalised by rstrip(";")).
_REJECT_VERBS_SEMICOLON_GLUED = ["BEGIN;", "COMMIT;", "ROLLBACK;", "END;"]


@pytest.mark.parametrize("statement", _REJECT_VERBS)
def test_sync_executemany_rejects_transaction_verb(statement: str) -> None:
    conn = Connection("localhost:9001")
    cursor = Cursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for"):
        cursor.executemany(statement, [(1,)])


@pytest.mark.parametrize("statement", _REJECT_VERBS)
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
    """Comment stripping applies before the verb check."""
    conn = Connection("localhost:9001")
    cursor = Cursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for SAVEPOINT"):
        cursor.executemany("/* annotation */ SAVEPOINT sp", [(1,)])


def test_sync_executemany_admits_dml_unchanged() -> None:
    """Negative pin: INSERT (and other DML) stays admitted by the reject-list."""
    conn = MagicMock(spec=Connection)
    conn._check_thread = MagicMock()

    # Close the coroutine the cursor would otherwise leak when _run_sync is a no-op.
    def consume_coroutine(coro: object) -> None:
        if hasattr(coro, "close"):
            coro.close()

    conn._run_sync = MagicMock(side_effect=consume_coroutine)

    cursor = Cursor.__new__(Cursor)
    cursor._connection = conn
    cursor._closed = False
    cursor._description = None
    cursor._rowcount = -1
    cursor._lastrowid = None
    cursor._row_factory = None
    cursor._rows = []
    cursor._row_index = 0
    cursor._arraysize = 1
    cursor.messages = []
    cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])


@pytest.mark.parametrize("statement", _REJECT_VERBS_SEMICOLON_GLUED)
def test_sync_executemany_rejects_transaction_verb_glued_to_semicolon(
    statement: str,
) -> None:
    """Verbs glued to a semicolon are rejected via the reject-list's ``rstrip(";")``."""
    conn = Connection("localhost:9001")
    cursor = Cursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for"):
        cursor.executemany(statement, [(1,)])


@pytest.mark.parametrize("statement", _REJECT_VERBS_SEMICOLON_GLUED)
async def test_async_executemany_rejects_transaction_verb_glued_to_semicolon(
    statement: str,
) -> None:
    conn = AsyncConnection("localhost:9001")
    cursor = AsyncCursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for"):
        await cursor.executemany(statement, [(1,)])


def test_sync_executemany_rejects_begin_glued_to_following_statement() -> None:
    """``BEGIN; INSERT ...`` is rejected: first_verb "BEGIN;" canonicalises via rstrip(";")."""
    conn = Connection("localhost:9001")
    cursor = Cursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for BEGIN"):
        cursor.executemany("BEGIN; INSERT INTO t VALUES (?)", [(1,)])


_LEADING_SEMICOLON_VERBS = [
    ";BEGIN",
    ";SAVEPOINT sp",
    ";COMMIT",
    ";ROLLBACK",
    ";END",
    ";RELEASE sp",
    "  ;BEGIN",
    ";;BEGIN",
    "; ; BEGIN",
]


@pytest.mark.parametrize("statement", _LEADING_SEMICOLON_VERBS)
def test_sync_executemany_rejects_leading_semicolon_verb(statement: str) -> None:
    """Leading ``;`` + interleaved whitespace must be stripped before verb extraction."""
    conn = Connection("localhost:9001")
    cursor = Cursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for"):
        cursor.executemany(statement, [(1,)])


@pytest.mark.parametrize("statement", _LEADING_SEMICOLON_VERBS)
async def test_async_executemany_rejects_leading_semicolon_verb(statement: str) -> None:
    conn = AsyncConnection("localhost:9001")
    cursor = AsyncCursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for"):
        await cursor.executemany(statement, [(1,)])


_SEMICOLON_THEN_COMMENT_VERBS = [
    "; /* x */ SAVEPOINT foo",
    ";; /* x */ BEGIN",
    "-- a\n; SAVEPOINT foo",
    "; -- a\nSAVEPOINT foo",
    "/* x */ ; BEGIN",
    "-- a\n; -- b\n; SAVEPOINT foo",
]


@pytest.mark.parametrize("statement", _SEMICOLON_THEN_COMMENT_VERBS)
def test_sync_executemany_rejects_semicolon_then_comment_verb(statement: str) -> None:
    """Comments sitting after a leading ``;`` must be stripped before verb extraction."""
    conn = Connection("localhost:9001")
    cursor = Cursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for"):
        cursor.executemany(statement, [(1,)])


@pytest.mark.parametrize("statement", _SEMICOLON_THEN_COMMENT_VERBS)
async def test_async_executemany_rejects_semicolon_then_comment_verb(
    statement: str,
) -> None:
    conn = AsyncConnection("localhost:9001")
    cursor = AsyncCursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for"):
        await cursor.executemany(statement, [(1,)])


def test_sync_executemany_rejection_preserves_prior_lastrowid() -> None:
    """A rejected executemany ran no execute, so prior ``lastrowid`` is preserved."""
    cursor = Cursor.__new__(Cursor)
    cursor._closed = False
    cursor._description = None
    cursor._rowcount = -1
    cursor._row_factory = None
    cursor._rows = []
    cursor._row_index = 0
    cursor._arraysize = 1
    cursor.messages = []
    conn = MagicMock(spec=Connection)
    conn._check_thread = MagicMock()
    conn._run_sync = MagicMock(
        side_effect=AssertionError("rejection must short-circuit before _run_sync")
    )
    cursor._connection = conn
    cursor._lastrowid = 4242

    with pytest.raises(ProgrammingError, match="executemany.*not supported for BEGIN"):
        cursor.executemany("BEGIN", [(1,)])
    assert cursor.lastrowid == 4242, (
        f"rejected executemany must preserve prior lastrowid; got {cursor.lastrowid}"
    )


async def test_async_executemany_rejection_preserves_prior_lastrowid() -> None:
    """Async sibling: parity with the sync lastrowid-preservation contract."""
    cursor = AsyncCursor.__new__(AsyncCursor)
    cursor._closed = False
    cursor._description = None
    cursor._rowcount = -1
    cursor._row_factory = None
    cursor._rows = []
    cursor._row_index = 0
    cursor._arraysize = 1
    cursor.messages = []
    cursor._executing_task = None
    cursor._completed_iterations = 0
    cursor._connection = MagicMock(spec=AsyncConnection)
    cursor._lastrowid = 4242

    with pytest.raises(ProgrammingError, match="executemany.*not supported for BEGIN"):
        await cursor.executemany("BEGIN", [(1,)])
    assert cursor.lastrowid == 4242


def test_sync_executemany_row_returning_rejection_preserves_prior_lastrowid() -> None:
    """Row-returning rejection follows the same lastrowid-preservation contract."""
    cursor = Cursor.__new__(Cursor)
    cursor._closed = False
    cursor._description = None
    cursor._rowcount = -1
    cursor._row_factory = None
    cursor._rows = []
    cursor._row_index = 0
    cursor._arraysize = 1
    cursor.messages = []
    conn = MagicMock(spec=Connection)
    conn._check_thread = MagicMock()
    conn._run_sync = MagicMock(
        side_effect=AssertionError("rejection must short-circuit before _run_sync")
    )
    cursor._connection = conn
    cursor._lastrowid = 4242

    with pytest.raises(ProgrammingError, match="executemany.*can only execute DML"):
        cursor.executemany("SELECT 1", [(1,)])
    assert cursor.lastrowid == 4242
