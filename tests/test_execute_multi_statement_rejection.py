"""``Cursor.execute`` and ``AsyncCursor.execute`` reject
multi-statement SQL with ``ProgrammingError``, matching stdlib
``sqlite3.Cursor.execute``.

dqlite's server prepare path returns only the first statement;
without this guard, ``"INSERT ...; INSERT ..."`` silently drops
everything past the first ``;`` with no diagnostic — silent data
loss.

Multi-statement intent is via ``executescript`` (we stub it as
``NotSupportedError``).
"""

import pytest

from dqlitedbapi.cursor import _is_multi_statement


class TestIsMultiStatement:
    @pytest.mark.parametrize(
        "sql",
        [
            # Single statement, no trailing semicolon.
            "SELECT 1",
            # Single statement, trailing whitespace only.
            "SELECT 1   ",
            # Single statement plus trailing semicolon.
            "SELECT 1;",
            # Single statement + trailing whitespace after semicolon.
            "SELECT 1;\n   \n",
            # Single statement + trailing line comment.
            "SELECT 1; -- trailing",
            # Single statement + trailing block comment.
            "SELECT 1; /* trailing */",
            # Semicolon inside string literal — not a boundary.
            "INSERT INTO t VALUES (';')",
            # Semicolon inside line comment — not a boundary.
            "-- ; not a real semicolon\nSELECT 1",
            # Semicolon inside block comment — not a boundary.
            "/* ; not real */ SELECT 1",
            # Semicolon inside double-quoted identifier — not a
            # boundary (SQLite identifier-quoting rule).
            'SELECT "col;name" FROM t',
            # Empty SQL (covered by a sibling issue's empty-SQL
            # classification fix; here it must NOT be flagged as
            # multi-statement).
            "",
            "   ",
            "-- comment\n",
        ],
    )
    def test_single_statement_not_flagged(self, sql: str) -> None:
        assert _is_multi_statement(sql) is False

    @pytest.mark.parametrize(
        "sql",
        [
            # Two DML statements.
            "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)",
            # DDL + DDL.
            "CREATE TABLE a (x); CREATE TABLE b (y)",
            # Mixed DDL + DML.
            "CREATE TABLE t (x); INSERT INTO t VALUES (1)",
            # Whitespace-then-statement after the first ``;``.
            "SELECT 1;   SELECT 2",
            # Comment + second statement past the first ``;``.
            "SELECT 1; /* sep */ SELECT 2",
            # Double semicolon followed by another statement —
            # stdlib treats consecutive ``;`` as separate statements.
            "SELECT 1;; SELECT 2",
        ],
    )
    def test_multi_statement_flagged(self, sql: str) -> None:
        assert _is_multi_statement(sql) is True


class TestExecuteRejectsMultiStatementSync:
    def test_rejects_two_dml(self) -> None:
        from dqlitedbapi.connection import Connection
        from dqlitedbapi.cursor import Cursor
        from dqlitedbapi.exceptions import ProgrammingError

        conn = Connection("localhost:19001", timeout=2.0)
        cur = Cursor(conn)
        with pytest.raises(ProgrammingError, match="one statement at a time"):
            cur.execute("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)")

    def test_accepts_single_statement_with_trailing_comment(self) -> None:
        from dqlitedbapi.connection import Connection
        from dqlitedbapi.cursor import Cursor
        from dqlitedbapi.exceptions import ProgrammingError

        conn = Connection("localhost:19001", timeout=2.0)
        cur = Cursor(conn)
        # Should not raise the multi-statement error. (It will fail
        # at the wire round-trip because the server isn't running,
        # but that's a separate error class.)
        with pytest.raises(Exception) as excinfo:
            cur.execute("SELECT 1; -- comment")
        assert not isinstance(excinfo.value, ProgrammingError) or (
            "one statement at a time" not in str(excinfo.value)
        )


@pytest.mark.asyncio
class TestExecuteRejectsMultiStatementAsync:
    async def test_rejects_two_dml(self) -> None:
        from dqlitedbapi.aio.connection import AsyncConnection
        from dqlitedbapi.aio.cursor import AsyncCursor
        from dqlitedbapi.exceptions import ProgrammingError

        conn = AsyncConnection("localhost:19001")
        cur = AsyncCursor(conn)
        with pytest.raises(ProgrammingError, match="one statement at a time"):
            await cur.execute("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)")
