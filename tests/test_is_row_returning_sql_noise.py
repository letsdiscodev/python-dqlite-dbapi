"""``_is_row_returning`` must not match ``RETURNING`` tokens that live
inside string literals, double-quoted identifiers, or comments.

Previously the heuristic did a plain uppercase substring scan for
`` RETURNING ``, so ``INSERT INTO t VALUES('some RETURNING thing')`` or
an ``UPDATE t SET "returning" = 1`` statement was misclassified as
row-returning, the statement was dispatched through QUERY_SQL, and
the cursor reported ``rowcount=0`` / ``lastrowid=None``.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.cursor import _is_row_returning, _strip_sql_noise


class TestStripSqlNoise:
    def test_strips_single_quoted_literal(self) -> None:
        out = _strip_sql_noise("SELECT a FROM t WHERE b = 'abc'")
        assert "'abc'" not in out
        assert "abc" not in out
        assert "SELECT a FROM t WHERE b =" in out

    def test_strips_doubled_single_quote_escape(self) -> None:
        # ``''`` inside the literal is an escape, not a terminator.
        out = _strip_sql_noise("SELECT 'it''s fine'")
        assert "RETURNING" not in out
        assert "SELECT" in out

    def test_strips_double_quoted_identifier(self) -> None:
        out = _strip_sql_noise('UPDATE t SET "returning" = 1')
        assert "returning" not in out
        assert "UPDATE t SET" in out

    def test_strips_block_comment(self) -> None:
        out = _strip_sql_noise("DELETE FROM t /* RETURNING */ WHERE id = 1")
        assert "RETURNING" not in out
        assert "DELETE FROM t" in out

    def test_strips_line_comment(self) -> None:
        out = _strip_sql_noise("DELETE FROM t -- RETURNING\nWHERE id = 1")
        assert "RETURNING" not in out


class TestIsRowReturningRejectsLiteralMatches:
    @pytest.mark.parametrize(
        "sql",
        [
            "INSERT INTO t(name) VALUES ('some RETURNING thing')",
            "UPDATE t SET name = 'a RETURNING b' WHERE id = 1",
            'UPDATE t SET "returning" = 1 WHERE id = 1',
            "DELETE FROM t /* will not be RETURNING */ WHERE id = 1",
            "DELETE FROM t -- RETURNING\nWHERE id = 1",
        ],
    )
    def test_dml_with_returning_in_noise_is_not_row_returning(self, sql: str) -> None:
        assert _is_row_returning(sql) is False, sql

    @pytest.mark.parametrize(
        "sql",
        [
            "INSERT INTO t(name) VALUES ('x') RETURNING id",
            "UPDATE t SET v = 1 RETURNING v",
            "DELETE FROM t WHERE id = 1 RETURNING id",
        ],
    )
    def test_real_returning_still_row_returning(self, sql: str) -> None:
        assert _is_row_returning(sql) is True, sql

    @pytest.mark.parametrize(
        "sql",
        ["SELECT 1", "VALUES (1)", "PRAGMA foreign_keys", "EXPLAIN SELECT 1"],
    )
    def test_pure_queries_still_row_returning(self, sql: str) -> None:
        assert _is_row_returning(sql) is True, sql
