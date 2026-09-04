"""``_strip_sql_noise`` strips backtick-quoted identifiers so a column named `returning`
does not trip the classifier. SQLite escapes inner backticks by doubling, not backslash."""

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
        assert "`returning`" not in cleaned
        assert "RETURNING" not in cleaned.upper()

    def test_strips_backtick_with_doubled_escape(self) -> None:
        """Doubled inner backticks are a literal escape, not a quote terminator."""
        cleaned = _strip_sql_noise("SELECT `foo``bar` FROM t")
        assert "`foo``bar`" not in cleaned
        assert "SELECT" in cleaned.upper()
        assert "FROM" in cleaned.upper()


class TestRowReturningIgnoresBacktickQuotedKeyword:
    def test_update_with_backtick_returning_column_not_row_returning(self) -> None:
        assert _is_row_returning("UPDATE t SET `returning` = 1 WHERE id = 1") is False

    def test_insert_into_backtick_returning_table_not_row_returning(self) -> None:
        sql = "INSERT INTO `returning_q` (v) VALUES (1)"
        assert _is_row_returning(sql) is False
        assert _is_insert_or_replace(sql) is True

    def test_update_with_backtick_returning_column_not_dml_with_returning(self) -> None:
        sql = "UPDATE t SET `returning` = 1 WHERE id = 1 RETURNING id"
        assert _is_dml_with_returning(sql) is True

    def test_genuine_returning_clause_still_matches(self) -> None:
        assert _is_row_returning("INSERT INTO t (v) VALUES (1) RETURNING id") is True
        assert _is_row_returning("UPDATE t SET v = 1 RETURNING id") is True
