"""Pin ``_strip_leading_with_clause`` and ``_is_dml_with_returning``
edge branches uncovered by ``pytest --cov``:

- ``cursor.py:481-483`` — RECURSIVE keyword skip after WITH.
- ``cursor.py:494`` — ``AS(`` (no space before paren) precedence.
- ``cursor.py:496`` — malformed-CTE fallback (no AS found).
- ``cursor.py:500`` — malformed-CTE fallback (AS without
  following paren).
- ``cursor.py:510`` — unbalanced-paren fallback.
- ``cursor.py:518-521`` — comma-separated multi-CTE iteration.

The CTE parser admits ``WITH ... DELETE/INSERT/UPDATE`` shapes
through ``executemany`` (per a prior commit). The uncovered edges
silently apply the wrong fallback; without tests, a refactor that
"tightens" the parser could break valid CTE shapes (RECURSIVE,
multi-CTE) silently.

The stripper takes ALREADY-NORMALIZED SQL (uppercase, single
spaces, leading whitespace stripped — see callers). Tests pass
the post-normalization shape directly.
"""

from __future__ import annotations

from dqlitedbapi.cursor import _is_dml_with_returning, _strip_leading_with_clause


class TestStripLeadingWithClauseEdges:
    def test_recursive_keyword_is_skipped_after_with(self) -> None:
        """``WITH RECURSIVE c(n) AS (...) DELETE ...`` — pin the
        RECURSIVE skip at cursor.py:481-483."""
        normalized = (
            "WITH RECURSIVE C(N) AS (SELECT 1 UNION SELECT N+1 FROM C) "
            "DELETE FROM T WHERE ID IN (SELECT N FROM C)"
        )
        body = _strip_leading_with_clause(normalized)
        assert body.startswith("DELETE FROM T")

    def test_as_paren_no_space_is_handled(self) -> None:
        """``WITH C AS(SELECT 1) DELETE ...`` — pin the ``AS(``
        precedence path at cursor.py:494."""
        normalized = "WITH C AS(SELECT 1) DELETE FROM T"
        body = _strip_leading_with_clause(normalized)
        assert body.startswith("DELETE FROM T")

    def test_malformed_no_as_falls_back_to_input(self) -> None:
        """``WITH C (SELECT 1) FROM T`` — no ``AS`` keyword. Stripper
        returns the input unchanged. Pin the fallback at
        cursor.py:496."""
        normalized = "WITH C (SELECT 1) FROM T"
        assert _strip_leading_with_clause(normalized) == normalized

    def test_malformed_as_without_following_paren_falls_back(self) -> None:
        """``WITH C AS SELECT 1 FROM T`` — AS without following
        ``(``. Stripper returns the input unchanged. Pin the
        fallback at cursor.py:500."""
        normalized = "WITH C AS SELECT 1 FROM T"
        assert _strip_leading_with_clause(normalized) == normalized

    def test_unbalanced_parens_fall_back_to_input(self) -> None:
        """An unclosed CTE body — depth never returns to 0. Stripper
        returns the input unchanged. Pin cursor.py:510."""
        normalized = "WITH C AS (SELECT 1, (2) DELETE FROM T"
        assert _strip_leading_with_clause(normalized) == normalized

    def test_comma_separated_multi_cte_strips_all(self) -> None:
        """``WITH A AS (...), B AS (...) DELETE ...`` — multiple
        comma-separated CTEs. Stripper iterates the loop body.
        Pin cursor.py:518-521."""
        normalized = "WITH A AS (SELECT 1), B AS (SELECT 2) DELETE FROM T"
        body = _strip_leading_with_clause(normalized)
        assert body.startswith("DELETE FROM T")


class TestIsDmlWithReturningCteShapes:
    """Higher-level pins via the public callers. Each shape that
    succeeds at the stripper above must be admitted as DML by
    ``_is_dml_with_returning`` (the gate ``executemany`` uses)."""

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
        """Stripper returns input unchanged on malformed CTE; the
        downstream check sees ``WITH ...`` as the leading token,
        which is not DML."""
        assert _is_dml_with_returning("WITH c (SELECT 1) FROM t") is False
