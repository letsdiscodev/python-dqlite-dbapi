"""Pin: ``_is_row_returning`` classifies RETURNING when preceded or
followed by non-space whitespace (newline, tab) and not just the
literal-space form.

Previously the heuristic was a literal-space substring scan
(`` RETURNING `` and `` RETURNING`` at end), which missed multi-line
SQL formatting — the dominant style produced by sqlfluff,
pgFormatter, hand-written queries spanning multiple lines, etc.
Result: ``INSERT INTO t VALUES (?)\\nRETURNING id`` silently
classified as exec-only; rows were discarded; cursor.description
became None; cursor.fetchall() returned []. No exception. Pure data
loss.

Pin the regex word-boundary fix on every plausible whitespace shape.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.cursor import _is_row_returning


@pytest.mark.parametrize(
    "sql",
    [
        # Newline before RETURNING — the dominant multi-line idiom
        "INSERT INTO t VALUES (?)\nRETURNING id",
        "INSERT INTO t VALUES (?)\n  RETURNING id",
        "UPDATE t SET x=1\nRETURNING id",
        "DELETE FROM t WHERE id = ?\nRETURNING id",
        # Tab whitespace
        "INSERT INTO t VALUES (?)\tRETURNING id",
        "UPDATE t SET x=1\tRETURNING id",
        # CR / CRLF
        "INSERT INTO t VALUES (?)\r\nRETURNING id",
        "INSERT INTO t VALUES (?)\rRETURNING id",
        # Newline AFTER RETURNING (between RETURNING and the column list)
        "INSERT INTO t VALUES (?) RETURNING\nid",
        "INSERT INTO t VALUES (?) RETURNING\n  id",
        # Tab after RETURNING
        "INSERT INTO t VALUES (?) RETURNING\tid",
        # Mixed: leading comment + newline-prefixed RETURNING
        "-- header comment\nINSERT INTO t VALUES (?)\nRETURNING id",
        "/* block header */\nINSERT INTO t VALUES (?)\nRETURNING id",
        # Block-comment IMMEDIATELY before RETURNING
        "INSERT INTO t VALUES (?)/* comment */RETURNING id",
        # CTE-prefixed DML on multiple lines
        "WITH x AS (SELECT 1)\nINSERT INTO t SELECT * FROM x\nRETURNING id",
        # Multiple spaces are fine (the old literal-space scan tolerated this);
        # pin negatively against regression.
        "INSERT INTO t VALUES (?)   RETURNING id",
    ],
)
def test_returning_with_non_space_whitespace_classified_as_row_returning(sql: str) -> None:
    assert _is_row_returning(sql) is True, (
        f"multi-line RETURNING shape was mis-dispatched as exec-only: {sql!r}"
    )


@pytest.mark.parametrize(
    "sql",
    [
        # Non-SELECT-prefix shapes — pin the regex scan directly
        # (not short-circuited by the prefix check) by using DML.
        "INSERT INTO t (RETURNING_col) VALUES (1)",
        "UPDATE t SET RETURNING_col = 1",
        "UPDATE t SET v = RETURNING_id WHERE x = 1",
        "DELETE FROM t WHERE RETURNING_flag = 1",
        # Quoted identifier containing the word RETURNING — the noise
        # stripper neutralises it before the scan runs.
        "UPDATE t SET v = 1 WHERE name = 'I have RETURNING in my data'",
        'UPDATE t SET v = 1 WHERE alias = "my returning column"',
    ],
)
def test_returning_as_substring_of_identifier_not_misclassified(sql: str) -> None:
    """Negative pin: the word boundary must NOT match RETURNING when
    it's part of a larger identifier. The noise stripper neutralises
    string-literal occurrences before the scan runs. All cases here
    are non-SELECT-prefix DML, so the scan path is actually exercised
    (not short-circuited by the prefix check)."""
    assert _is_row_returning(sql) is False, (
        f"RETURNING-substring-of-identifier mis-classified as row-returning: {sql!r}"
    )


def test_returning_in_executemany_dml_classification() -> None:
    """Integration with ``_is_dml_with_returning`` (used by
    executemany's reject-list). The HIGH-severity audit found a
    secondary path where multiline RETURNING in executemany would
    also mis-route. Verify _is_dml_with_returning correctly detects
    multi-line RETURNING."""
    from dqlitedbapi.cursor import _is_dml_with_returning

    # All these should classify as "DML with RETURNING".
    multi_line_returnings = [
        "INSERT INTO t VALUES (?)\nRETURNING id",
        "UPDATE t SET x = ?\nRETURNING id",
        "DELETE FROM t WHERE id = ?\nRETURNING id",
    ]
    for sql in multi_line_returnings:
        assert _is_dml_with_returning(sql) is True, (
            f"_is_dml_with_returning mis-routes multi-line: {sql!r}"
        )


def test_single_line_returning_still_classified() -> None:
    """Negative pin: the legacy literal-space form still classifies
    after the fix. Pins against an over-tightening regression."""
    assert _is_row_returning("INSERT INTO t VALUES (1) RETURNING id") is True
    assert _is_row_returning("UPDATE t SET x = 1 RETURNING id") is True
    assert _is_row_returning("DELETE FROM t WHERE id = 1 RETURNING id") is True
