"""``Cursor.rowcount`` returns ``-1`` after DDL / non-DML, matching
stdlib ``sqlite3`` and PEP 249 §6.1.1's "not determinable" sentinel.

The exec branch of ``_execute_async`` / ``_execute_unlocked``
historically wrote ``_rowcount = _to_signed_int64(affected)``
unconditionally; the dqlite wire returns 0 for DDL, so
``cur.rowcount == 0`` was deterministic and True. Stdlib gives
``-1`` (undetermined, False) for the same statements:

    >>> sqlite3.connect(":memory:").execute("CREATE TABLE t (x)").rowcount
    -1

Cross-driver code that branches on ``if cur.rowcount == 0:`` after a
DDL — migration tooling, "no-op DDL" detection, commit-decision
shortcuts — took divergent branches on dqlite vs stdlib.

Twin of ``ISSUE-1441_dbapi-rowcount-zero-after-pragma-write-stdlib-minus-one.md``
(which fixed the row-returning empty-columns branch).
"""

from __future__ import annotations

import sqlite3
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import _is_dml_rowcount_meaningful

# -------- helper unit tests on the predicate itself ----------------


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (1)",
        "  INSERT  INTO  t  VALUES (1)",
        "/* leading comment */ INSERT INTO t VALUES (1)",
        "REPLACE INTO t VALUES (1)",
        "insert into t values (1)",  # case-insensitive
        "UPDATE t SET x = 1",
        "DELETE FROM t",
        "INSERT OR REPLACE INTO t VALUES (1)",
        "INSERT OR IGNORE INTO t VALUES (1)",
    ],
)
def test_dml_rowcount_meaningful_accepts_dml(sql: str) -> None:
    """INSERT / UPDATE / DELETE / REPLACE — every verb where SQLite's
    ``sqlite3_changes()`` returns a meaningful count."""
    assert _is_dml_rowcount_meaningful(sql) is True


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE t (x)",
        "CREATE INDEX i ON t (x)",
        "DROP TABLE t",
        "DROP INDEX i",
        "ALTER TABLE t ADD COLUMN y",
        "VACUUM",
        "REINDEX",
        "ANALYZE",
        "ATTACH DATABASE 'x.db' AS aux",
        "DETACH DATABASE aux",
        "SAVEPOINT sp1",
        "RELEASE SAVEPOINT sp1",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "PRAGMA journal_mode = WAL",
        "create table t (x)",  # case-insensitive
        "/* leading */ CREATE TABLE t (x)",
    ],
)
def test_dml_rowcount_meaningful_rejects_non_dml(sql: str) -> None:
    """DDL / PRAGMA write / SAVEPOINT family / BEGIN-COMMIT-ROLLBACK
    — every verb where ``sqlite3_changes()`` is documented to return
    0 (or an unrelated count). Stdlib reports ``-1`` for these; we
    must match."""
    assert _is_dml_rowcount_meaningful(sql) is False


# -------- end-to-end stdlib-parity ---------------------------------


@pytest.mark.parametrize(
    "ddl",
    [
        "CREATE TABLE t_parity_ddl (x INTEGER)",
        "DROP TABLE IF EXISTS t_parity_ddl_nope",
        "VACUUM",
        "REINDEX",
        "ANALYZE",
    ],
)
def test_stdlib_returns_minus_one_for_ddl(ddl: str) -> None:
    """Documents the stdlib behavior the dqlite cursor must match."""
    expected = sqlite3.connect(":memory:").execute(ddl).rowcount
    assert expected == -1, f"stdlib changed: {ddl!r} now returns {expected}; update parity logic"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ddl",
    [
        "CREATE TABLE t (x)",
        "DROP TABLE t",
        "VACUUM",
        "ALTER TABLE t ADD COLUMN y",
    ],
)
async def test_async_cursor_rowcount_minus_one_after_ddl(ddl: str) -> None:
    """End-to-end via the async cursor: the exec branch leaves
    ``_rowcount == -1`` for DDL even though the wire returns
    ``rows_affected = 0`` and ``last_insert_id = 0``."""
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    inner = AsyncMock()
    inner.execute = lambda _op, _params: None
    cur._connection._ensure_connection = AsyncMock(return_value=inner)
    cur._rowcount = 999  # poison: must be overwritten to -1

    async def fake_call(_coro: Any) -> tuple[int, int]:
        return (0, 0)  # wire: last_insert_id=0, rows_affected=0

    with patch("dqlitedbapi.aio.cursor._call_client", new=fake_call):
        await cur._execute_unlocked(ddl, None)

    assert cur._rowcount == -1, (
        f"DDL {ddl!r} should leave rowcount at -1 (stdlib parity); got {cur._rowcount}"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dml,affected",
    [
        ("INSERT INTO t VALUES (1)", 1),
        ("UPDATE t SET x = 1", 3),
        ("DELETE FROM t WHERE x = 1", 2),
    ],
)
async def test_async_cursor_dml_still_reports_affected(dml: str, affected: int) -> None:
    """Sanity check: DML still propagates the wire's ``affected``
    count. The new gating must NOT make every exec return ``-1``."""
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    inner = AsyncMock()
    inner.execute = lambda _op, _params: None
    cur._connection._ensure_connection = AsyncMock(return_value=inner)
    cur._rowcount = -999

    async def fake_call(_coro: Any) -> tuple[int, int]:
        return (0, affected)

    with patch("dqlitedbapi.aio.cursor._call_client", new=fake_call):
        await cur._execute_unlocked(dml, None)

    assert cur._rowcount == affected, (
        f"DML {dml!r} must propagate affected={affected}; got {cur._rowcount}"
    )


@pytest.mark.parametrize(
    "ddl",
    [
        "CREATE TABLE t (x)",
        "DROP TABLE t",
        "VACUUM",
        "ALTER TABLE t ADD COLUMN y",
    ],
)
def test_sync_cursor_rowcount_minus_one_after_ddl(ddl: str) -> None:
    """End-to-end via the SYNC cursor: the exec branch leaves
    ``_rowcount == -1`` for DDL even though the wire returns
    ``rows_affected = 0`` and ``last_insert_id = 0``.

    Mirror of ``test_async_cursor_rowcount_minus_one_after_ddl``. The
    sync sibling previously had only an inspection pin — a refactor
    moving the gate into a helper would defeat the substring scan
    without behavioral regression coverage. Drive ``_execute_async``
    directly as a coroutine via ``asyncio.run``."""
    import asyncio

    from dqlitedbapi.connection import Connection
    from dqlitedbapi.cursor import Cursor

    conn = Connection("localhost:9001", timeout=2.0)
    cur = Cursor(conn)
    inner = AsyncMock()
    inner.execute = lambda _op, _params: None
    cur._connection._get_async_connection = AsyncMock(return_value=inner)
    cur._rowcount = 999

    async def fake_call(_coro: Any) -> tuple[int, int]:
        return (0, 0)

    with patch("dqlitedbapi.cursor._call_client", new=fake_call):
        asyncio.run(cur._execute_async(ddl, None))

    assert cur._rowcount == -1, (
        f"DDL {ddl!r} should leave rowcount at -1 (stdlib parity); got {cur._rowcount}"
    )


@pytest.mark.parametrize(
    "dml,affected",
    [
        ("INSERT INTO t VALUES (1)", 1),
        ("UPDATE t SET x = 1", 3),
        ("DELETE FROM t WHERE x = 1", 2),
    ],
)
def test_sync_cursor_dml_still_reports_affected(dml: str, affected: int) -> None:
    """Sanity twin: DML still propagates the wire's ``affected`` count
    on the SYNC side. The new gating must NOT make every exec return
    ``-1``. Mirror of ``test_async_cursor_dml_still_reports_affected``."""
    import asyncio

    from dqlitedbapi.connection import Connection
    from dqlitedbapi.cursor import Cursor

    conn = Connection("localhost:9001", timeout=2.0)
    cur = Cursor(conn)
    inner = AsyncMock()
    inner.execute = lambda _op, _params: None
    cur._connection._get_async_connection = AsyncMock(return_value=inner)
    cur._rowcount = -999

    async def fake_call(_coro: Any) -> tuple[int, int]:
        return (0, affected)

    with patch("dqlitedbapi.cursor._call_client", new=fake_call):
        asyncio.run(cur._execute_async(dml, None))

    assert cur._rowcount == affected, (
        f"DML {dml!r} must propagate affected={affected}; got {cur._rowcount}"
    )


def test_sync_exec_branch_gates_rowcount_on_dml_predicate() -> None:
    """Inspection pin: the sync exec branch reads
    ``_is_dml_rowcount_meaningful(operation)`` before writing
    ``_rowcount``. A regression that drops the gate would silently
    re-introduce ``rowcount == 0`` after DDL."""
    import inspect

    from dqlitedbapi.cursor import Cursor

    src = inspect.getsource(Cursor._execute_async)
    assert "_is_dml_rowcount_meaningful(operation)" in src, (
        "Sync exec branch must gate _rowcount write on "
        "_is_dml_rowcount_meaningful(operation) for stdlib parity"
    )


def test_async_exec_branch_gates_rowcount_on_dml_predicate() -> None:
    """Inspection pin: async sibling carries the same gate."""
    import inspect

    src = inspect.getsource(AsyncCursor._execute_unlocked)
    assert "_is_dml_rowcount_meaningful(operation)" in src, (
        "Async exec branch must gate _rowcount write on "
        "_is_dml_rowcount_meaningful(operation) for stdlib parity"
    )
