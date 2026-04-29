"""Pin: ``_strip_sql_noise`` strips backtick-quoted identifiers
(MySQL-compat) so a column named `returning` does not trigger
the row-returning classifier substring scan.

SQLite explicitly accepts backticks (``tokenize.c::sqlite3GetToken``
``TK_ID`` branch). The dbapi's classifier regex previously covered
``'...'``, ``"..."``, and ``[...]`` only — backticks were the
asymmetric outlier. Misclassification routed an UPDATE through
``query_raw_typed`` (losing ``lastrowid`` and ``rowcount``); the
client-layer splitter at ``connection.py`` already handled
backticks, so the two parsers in the same project disagreed.

SQLite's identifier-quote escape is **doubled-character**, NOT
backslash: `` `foo``bar` `` parses as the two-token identifier
``foo`bar``. The regex must use the doubled form, parity with the
existing ``"..."`` and ``'...'`` branches.
"""

from __future__ import annotations

from dqlitedbapi.cursor import (
    _is_dml_with_returning,
    _is_insert_or_replace,
    _is_row_returning,
    _strip_sql_noise,
)


class TestStripSqlNoiseBacktickIdentifier:
    def test_strips_backtick_quoted_identifier(self) -> None:
        cleaned = _strip_sql_noise("UPDATE t SET `returning` = 1 WHERE id = 1")
        # The backtick-quoted token is replaced with whitespace, so
        # the subsequent uppercase + ` RETURNING ` scan does not
        # match.
        assert "`returning`" not in cleaned
        assert "RETURNING" not in cleaned.upper()

    def test_strips_backtick_with_doubled_escape(self) -> None:
        """SQLite uses doubled backticks (`` `` ``) to escape inside
        an identifier. The regex must consume the inner doubled
        backtick as a literal, not terminate the quote."""
        cleaned = _strip_sql_noise("SELECT `foo``bar` FROM t")
        assert "`foo``bar`" not in cleaned
        # Whatever the cleaned form, the column reference is gone
        # and the keyword surface (SELECT, FROM, t) survives in
        # uppercase scan.
        assert "SELECT" in cleaned.upper()
        assert "FROM" in cleaned.upper()


class TestRowReturningIgnoresBacktickQuotedKeyword:
    def test_update_with_backtick_returning_column_not_row_returning(self) -> None:
        assert _is_row_returning("UPDATE t SET `returning` = 1 WHERE id = 1") is False

    def test_insert_into_backtick_returning_table_not_row_returning(self) -> None:
        sql = "INSERT INTO `returning_q` (v) VALUES (1)"
        assert _is_row_returning(sql) is False
        # Confirm INSERT verb is still recognised.
        assert _is_insert_or_replace(sql) is True

    def test_update_with_backtick_returning_column_not_dml_with_returning(self) -> None:
        sql = "UPDATE t SET `returning` = 1 WHERE id = 1 RETURNING id"
        # Real RETURNING outside the backticks must still match —
        # the classifier should now correctly see the ACTUAL
        # RETURNING clause after the backtick-quoted column name is
        # stripped.
        assert _is_dml_with_returning(sql) is True

    def test_genuine_returning_clause_still_matches(self) -> None:
        """Sanity: real RETURNING (not inside identifier quotes)
        still classifies correctly as row-returning."""
        assert _is_row_returning("INSERT INTO t (v) VALUES (1) RETURNING id") is True
        assert _is_row_returning("UPDATE t SET v = 1 RETURNING id") is True
