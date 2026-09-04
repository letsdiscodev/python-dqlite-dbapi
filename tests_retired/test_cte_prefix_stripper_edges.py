"""Pin ``_strip_leading_with_clause`` / ``_is_dml_with_returning`` edge
branches uncovered by ``pytest --cov``.

The stripper takes ALREADY-NORMALIZED SQL (uppercase, single spaces,
leading whitespace stripped); tests pass that post-normalization shape.
"""

from __future__ import annotations

from dqlitedbapi.cursor import _is_dml_with_returning, _strip_leading_with_clause


class TestStripLeadingWithClauseEdges:
    def test_recursive_keyword_is_skipped_after_with(self) -> None:
        """``WITH RECURSIVE ... DELETE`` — pin the RECURSIVE-skip branch."""
        normalized = (
            "WITH RECURSIVE C(N) AS (SELECT 1 UNION SELECT N+1 FROM C) "
            "DELETE FROM T WHERE ID IN (SELECT N FROM C)"
        )
        body = _strip_leading_with_clause(normalized)
        assert body.startswith("DELETE FROM T")

    def test_as_paren_no_space_is_handled(self) -> None:
        """``WITH C AS(...) DELETE`` — pin the ``AS(`` (no-space) precedence path."""
        normalized = "WITH C AS(SELECT 1) DELETE FROM T"
        body = _strip_leading_with_clause(normalized)
        assert body.startswith("DELETE FROM T")

    def test_malformed_no_as_falls_back_to_input(self) -> None:
        """No ``AS`` keyword — pin the ``as_idx == -1`` malformed-CTE fallback."""
        normalized = "WITH C (SELECT 1) FROM T"
        assert _strip_leading_with_clause(normalized) == normalized

    def test_malformed_as_without_following_paren_falls_back(self) -> None:
        """``AS`` with no following ``(`` — pin the ``body_paren == -1`` fallback."""
        normalized = "WITH C AS SELECT 1 FROM T"
        assert _strip_leading_with_clause(normalized) == normalized

    def test_unbalanced_parens_fall_back_to_input(self) -> None:
        """Unclosed CTE body — pin the ``depth != 0`` unbalanced-paren fallback."""
        normalized = "WITH C AS (SELECT 1, (2) DELETE FROM T"
        assert _strip_leading_with_clause(normalized) == normalized

    def test_comma_separated_multi_cte_strips_all(self) -> None:
        """Multi-CTE ``WITH A AS (...), B AS (...)`` — pin the comma-continuation branch."""
        normalized = "WITH A AS (SELECT 1), B AS (SELECT 2) DELETE FROM T"
        body = _strip_leading_with_clause(normalized)
        assert body.startswith("DELETE FROM T")


class TestIsDmlWithReturningCteShapes:
    """Each shape the stripper accepts must be admitted as DML by ``_is_dml_with_returning``."""

    def test_with_recursive_dml_is_admitted(self) -> None:
        sql = (
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION SELECT n+1 FROM c) "
            "DELETE FROM t WHERE id IN (SELECT n FROM c)"
        )
        assert _is_dml_with_returning(sql) is True

    def test_multi_cte_dml_is_admitted(self) -> None:
        sql = "WITH a AS (SELECT 1), b AS (SELECT 2) DELETE FROM t"
        assert _is_dml_with_returning(sql) is True

    def test_with_as_paren_no_space_dml_is_admitted(self) -> None:
        sql = "WITH c AS(SELECT 1) DELETE FROM t"
        assert _is_dml_with_returning(sql) is True

    def test_malformed_with_is_not_admitted_as_dml(self) -> None:
        """Malformed CTE: stripper returns input unchanged, so ``WITH`` is not DML."""
        assert _is_dml_with_returning("WITH c (SELECT 1) FROM t") is False
