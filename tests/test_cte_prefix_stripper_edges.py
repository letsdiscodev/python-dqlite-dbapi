"""Pin ``_strip_leading_with_clause`` and ``_is_dml_with_returning``
edge branches uncovered by ``pytest --cov``. Each pin is anchored by
the symbol/branch inside ``_strip_leading_with_clause`` rather than a
line number so the citations don't rot when surrounding code shifts
(prior line-number citations drifted by ~500 lines as ``cursor.py``
grew above the helper):

- ``_strip_leading_with_clause`` RECURSIVE-skip branch
  (``if normalized[pos:].startswith("RECURSIVE")``).
- ``_strip_leading_with_clause`` ``AS(`` (no space before paren)
  precedence (``as_idx_paren`` selection over ``as_idx``).
- ``_strip_leading_with_clause`` malformed-CTE fallback when no
  ``AS`` is found (``if as_idx == -1: return normalized``).
- ``_strip_leading_with_clause`` malformed-CTE fallback when ``AS``
  has no following ``(`` (``if body_paren == -1: return normalized``).
- ``_strip_leading_with_clause`` unbalanced-paren fallback
  (``if depth != 0: return normalized``).
- ``_strip_leading_with_clause`` comma-separated multi-CTE iteration
  (the ``if pos < len(normalized) and normalized[pos] == ","`` branch
  that loops back to the next CTE).

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
        ``_strip_leading_with_clause`` RECURSIVE-skip branch
        (``if normalized[pos:].startswith("RECURSIVE")``)."""
        normalized = (
            "WITH RECURSIVE C(N) AS (SELECT 1 UNION SELECT N+1 FROM C) "
            "DELETE FROM T WHERE ID IN (SELECT N FROM C)"
        )
        body = _strip_leading_with_clause(normalized)
        assert body.startswith("DELETE FROM T")

    def test_as_paren_no_space_is_handled(self) -> None:
        """``WITH C AS(SELECT 1) DELETE ...`` — pin the
        ``_strip_leading_with_clause`` ``AS(`` precedence path
        (``as_idx_paren`` selected over ``as_idx`` when the former
        is closer / the latter is -1)."""
        normalized = "WITH C AS(SELECT 1) DELETE FROM T"
        body = _strip_leading_with_clause(normalized)
        assert body.startswith("DELETE FROM T")

    def test_malformed_no_as_falls_back_to_input(self) -> None:
        """``WITH C (SELECT 1) FROM T`` — no ``AS`` keyword. Stripper
        returns the input unchanged. Pin the
        ``_strip_leading_with_clause`` ``if as_idx == -1: return
        normalized`` malformed-CTE fallback."""
        normalized = "WITH C (SELECT 1) FROM T"
        assert _strip_leading_with_clause(normalized) == normalized

    def test_malformed_as_without_following_paren_falls_back(self) -> None:
        """``WITH C AS SELECT 1 FROM T`` — AS without following
        ``(``. Stripper returns the input unchanged. Pin the
        ``_strip_leading_with_clause`` ``if body_paren == -1: return
        normalized`` fallback."""
        normalized = "WITH C AS SELECT 1 FROM T"
        assert _strip_leading_with_clause(normalized) == normalized

    def test_unbalanced_parens_fall_back_to_input(self) -> None:
        """An unclosed CTE body — depth never returns to 0. Stripper
        returns the input unchanged. Pin the
        ``_strip_leading_with_clause`` ``if depth != 0: return
        normalized`` fallback."""
        normalized = "WITH C AS (SELECT 1, (2) DELETE FROM T"
        assert _strip_leading_with_clause(normalized) == normalized

    def test_comma_separated_multi_cte_strips_all(self) -> None:
        """``WITH A AS (...), B AS (...) DELETE ...`` — multiple
        comma-separated CTEs. Stripper iterates the loop body. Pin
        the ``_strip_leading_with_clause`` comma-continuation branch
        (``if pos < len(normalized) and normalized[pos] == ","``)."""
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
