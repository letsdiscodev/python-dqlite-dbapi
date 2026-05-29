"""Pin: ``_is_row_returning`` classifies RETURNING across any whitespace
(newline, tab, CR) not just literal spaces. The old literal-space scan
silently classified multi-line RETURNING as exec-only, causing pure data
loss (rows discarded, no exception)."""

from __future__ import annotations

import pytest

from dqlitedbapi.cursor import _is_row_returning


@pytest.mark.parametrize(
    "sql",
    [
        # Whitespace before RETURNING: newline, tab, CR/CRLF
        "INSERT INTO t VALUES (?)\nRETURNING id",
        "INSERT INTO t VALUES (?)\n  RETURNING id",
        "UPDATE t SET x=1\nRETURNING id",
        "DELETE FROM t WHERE id = ?\nRETURNING id",
        "INSERT INTO t VALUES (?)\tRETURNING id",
        "UPDATE t SET x=1\tRETURNING id",
        "INSERT INTO t VALUES (?)\r\nRETURNING id",
        "INSERT INTO t VALUES (?)\rRETURNING id",
        # Whitespace after RETURNING (before the column list)
        "INSERT INTO t VALUES (?) RETURNING\nid",
        "INSERT INTO t VALUES (?) RETURNING\n  id",
        "INSERT INTO t VALUES (?) RETURNING\tid",
        # Leading comment + newline-prefixed RETURNING
        "-- header comment\nINSERT INTO t VALUES (?)\nRETURNING id",
        "/* block header */\nINSERT INTO t VALUES (?)\nRETURNING id",
        # Block-comment immediately before RETURNING
        "INSERT INTO t VALUES (?)/* comment */RETURNING id",
        # CTE-prefixed DML on multiple lines
        "WITH x AS (SELECT 1)\nINSERT INTO t SELECT * FROM x\nRETURNING id",
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
        # Non-SELECT-prefix DML so the regex scan runs (not prefix-short-circuited)
        "INSERT INTO t (RETURNING_col) VALUES (1)",
        "UPDATE t SET RETURNING_col = 1",
        "UPDATE t SET v = RETURNING_id WHERE x = 1",
        "DELETE FROM t WHERE RETURNING_flag = 1",
        # Quoted identifier with RETURNING, neutralised by the noise stripper
        "UPDATE t SET v = 1 WHERE name = 'I have RETURNING in my data'",
        'UPDATE t SET v = 1 WHERE alias = "my returning column"',
    ],
)
def test_returning_as_substring_of_identifier_not_misclassified(sql: str) -> None:
    """Negative pin: the word boundary must NOT match RETURNING inside a larger
    identifier (all cases are non-SELECT-prefix DML to exercise the scan)."""
    assert _is_row_returning(sql) is False, (
        f"RETURNING-substring-of-identifier mis-classified as row-returning: {sql!r}"
    )


def test_returning_in_executemany_dml_classification() -> None:
    """_is_dml_with_returning (executemany's reject-list) must also detect
    multi-line RETURNING; an audit found this secondary mis-route path."""
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
    """Negative pin: the legacy literal-space form still classifies (guards
    against an over-tightening regression)."""
    assert _is_row_returning("INSERT INTO t VALUES (1) RETURNING id") is True
    assert _is_row_returning("UPDATE t SET x = 1 RETURNING id") is True
    assert _is_row_returning("DELETE FROM t WHERE id = 1 RETURNING id") is True
