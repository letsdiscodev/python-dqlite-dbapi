"""execute rejects multi-statement SQL with ``ProgrammingError``, matching stdlib sqlite3.

dqlite's prepare path returns only the first statement, so without this guard
``"INSERT ...; INSERT ..."`` would silently drop everything past the first ``;``.
"""

import pytest

from dqlitedbapi.cursor import _is_multi_statement


class TestIsMultiStatement:
    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1",
            "SELECT 1   ",
            "SELECT 1;",
            "SELECT 1;\n   \n",
            "SELECT 1; -- trailing",
            "SELECT 1; /* trailing */",
            # Semicolons inside literals/comments/quoted identifiers are not boundaries.
            "INSERT INTO t VALUES (';')",
            "-- ; not a real semicolon\nSELECT 1",
            "/* ; not real */ SELECT 1",
            'SELECT "col;name" FROM t',
            # Empty SQL must NOT be flagged as multi-statement.
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
            "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)",
            "CREATE TABLE a (x); CREATE TABLE b (y)",
            "CREATE TABLE t (x); INSERT INTO t VALUES (1)",
            "SELECT 1;   SELECT 2",
            "SELECT 1; /* sep */ SELECT 2",
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
        # Fails at the wire round-trip (no server), but not with the multi-statement error.
        with pytest.raises(Exception) as excinfo:
            cur.execute("SELECT 1; -- comment")
        assert not isinstance(excinfo.value, ProgrammingError) or (
            "one statement at a time" not in str(excinfo.value)
        )


class TestExecuteRejectsMultiStatementAsync:
    async def test_rejects_two_dml(self) -> None:
        from dqlitedbapi.aio.connection import AsyncConnection
        from dqlitedbapi.aio.cursor import AsyncCursor
        from dqlitedbapi.exceptions import ProgrammingError

        conn = AsyncConnection("localhost:19001")
        cur = AsyncCursor(conn)
        with pytest.raises(ProgrammingError, match="one statement at a time"):
            await cur.execute("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)")
