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

# Verbs glued directly to a trailing semicolon (canonicalised by
# rstrip(";") in the reject-list check). Tests pin the no-operand
# verbs only — operand-bearing verbs like SAVEPOINT split cleanly at
# the space.
_REJECT_VERBS_SEMICOLON_GLUED = ["BEGIN;", "COMMIT;", "ROLLBACK;", "END;"]


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
    cursor._row_factory = None
    cursor._rows = []
    cursor._row_index = 0
    cursor._arraysize = 1
    cursor.messages = []
    # Should not raise.
    cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])


@pytest.mark.parametrize("statement", _REJECT_VERBS_SEMICOLON_GLUED)
def test_sync_executemany_rejects_transaction_verb_glued_to_semicolon(
    statement: str,
) -> None:
    """``BEGIN;``, ``COMMIT;``, ``ROLLBACK;``, ``END;`` must be rejected
    even when no whitespace separates the verb from the semicolon. The
    reject-list check canonicalises via ``rstrip(";")`` before the
    membership test."""
    conn = Connection("localhost:9001")
    cursor = Cursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for"):
        cursor.executemany(statement, [(1,)])


@pytest.mark.parametrize("statement", _REJECT_VERBS_SEMICOLON_GLUED)
@pytest.mark.asyncio
async def test_async_executemany_rejects_transaction_verb_glued_to_semicolon(
    statement: str,
) -> None:
    conn = AsyncConnection("localhost:9001")
    cursor = AsyncCursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for"):
        await cursor.executemany(statement, [(1,)])


def test_sync_executemany_rejects_begin_glued_to_following_statement() -> None:
    """``executemany("BEGIN; INSERT ...", [...])`` was previously
    silently admitted — first_verb was "BEGIN;" which is not in the
    reject set. Pin the rstrip(";") canonicalisation."""
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
    "  ;BEGIN",  # whitespace before leading ;
    ";;BEGIN",  # multiple leading ;
    "; ; BEGIN",  # interleaved ; and whitespace
]


@pytest.mark.parametrize("statement", _LEADING_SEMICOLON_VERBS)
def test_sync_executemany_rejects_leading_semicolon_verb(statement: str) -> None:
    """``executemany(";BEGIN ...", ...)`` and friends must also be
    rejected — the round-2 ``rstrip(";")`` fix only canonicalised the
    trailing-semicolon side. The leading-semicolon side requires
    stripping leading ``;`` + interleaved whitespace before the verb
    extraction. Otherwise ``head_normalised.split(maxsplit=1)[0]``
    yields ``";BEGIN"`` which is not in the reject set."""
    conn = Connection("localhost:9001")
    cursor = Cursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for"):
        cursor.executemany(statement, [(1,)])


@pytest.mark.parametrize("statement", _LEADING_SEMICOLON_VERBS)
@pytest.mark.asyncio
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
    """``executemany("; /* x */ SAVEPOINT foo", ...)`` and friends must
    be rejected — the original single-pass comment-strip-then-
    semicolon-loop missed comments that sat AFTER a leading ``;``,
    leaving ``first_verb = "/*"`` which is not in the reject set.
    Loop comment-strip + ;-strip together so the verb extraction sees
    past every interleaving."""
    conn = Connection("localhost:9001")
    cursor = Cursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for"):
        cursor.executemany(statement, [(1,)])


@pytest.mark.parametrize("statement", _SEMICOLON_THEN_COMMENT_VERBS)
@pytest.mark.asyncio
async def test_async_executemany_rejects_semicolon_then_comment_verb(
    statement: str,
) -> None:
    conn = AsyncConnection("localhost:9001")
    cursor = AsyncCursor(conn)
    with pytest.raises(ProgrammingError, match="executemany.*not supported for"):
        await cursor.executemany(statement, [(1,)])


def test_sync_executemany_rejection_preserves_prior_lastrowid() -> None:
    """A rejected ``executemany`` (transaction-control verb, row-
    returning, etc.) means no execute happened. The ``lastrowid``
    cursor-scoped property must therefore preserve its prior value —
    matching stdlib ``sqlite3`` behaviour, the documented driver
    contract at the cursor module's top docstring ("ROLLBACK / UPDATE /
    DELETE / DDL do NOT clear it... close() is the single lifecycle
    event that scrubs it"), AND the async sibling's behaviour (which
    never clears at entry).

    Without this pin, a ``cur.execute("INSERT ..."); rid =
    cur.lastrowid; try: cur.executemany("BEGIN", ...) except:
    pass; assert cur.lastrowid == rid`` shape silently fails on the
    sync surface but passes on async — a sync/async drift on a
    public-surface lifecycle property.
    """
    cursor = Cursor.__new__(Cursor)
    cursor._closed = False
    cursor._description = None
    cursor._rowcount = -1
    cursor._row_factory = None
    cursor._rows = []
    cursor._row_index = 0
    cursor._arraysize = 1
    cursor.messages = []
    # Wire a connection that would never be reached on the rejection path.
    conn = MagicMock(spec=Connection)
    conn._check_thread = MagicMock()
    conn._run_sync = MagicMock(
        side_effect=AssertionError("rejection must short-circuit before _run_sync")
    )
    cursor._connection = conn
    # Prior INSERT's rowid sits on the cursor.
    cursor._lastrowid = 4242

    with pytest.raises(ProgrammingError, match="executemany.*not supported for BEGIN"):
        cursor.executemany("BEGIN", [(1,)])
    assert cursor.lastrowid == 4242, (
        f"rejected executemany must preserve prior lastrowid; got {cursor.lastrowid}"
    )


@pytest.mark.asyncio
async def test_async_executemany_rejection_preserves_prior_lastrowid() -> None:
    """Async sibling pin — already correct today; locks in parity with
    the sync sibling fix so both surfaces share the same contract."""
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
    # Prior INSERT's rowid sits on the cursor.
    cursor._lastrowid = 4242

    with pytest.raises(ProgrammingError, match="executemany.*not supported for BEGIN"):
        await cursor.executemany("BEGIN", [(1,)])
    assert cursor.lastrowid == 4242


def test_sync_executemany_row_returning_rejection_preserves_prior_lastrowid() -> None:
    """Row-returning rejection (SELECT, PRAGMA) is a separate guard
    from the verb-rejection but must follow the same lastrowid
    contract — no batch ran, prior value preserved.
    """
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
