"""``_classify_caller_sql`` pre-flight catches three caller-side
mistakes (empty/comment-only SQL, multi-statement SQL, wrong ``?``
count) as ProgrammingError before the wire round-trip."""

from collections.abc import Iterator

import pytest

from dqlitedbapi.cursor import _classify_caller_sql
from dqlitedbapi.exceptions import ProgrammingError


@pytest.mark.parametrize(
    "sql",
    [
        "",
        "   ",
        "\t\n",
        "-- only a comment\n",
        "/* block-only */",
        "﻿",  # BOM-only
    ],
)
def test_empty_sql_rejected(sql: str) -> None:
    with pytest.raises(ProgrammingError, match="empty statement"):
        _classify_caller_sql(sql, None)


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)",
        "CREATE TABLE a (x); CREATE TABLE b (y)",
        "SELECT 1;   SELECT 2",
        "SELECT 1;; SELECT 2",
    ],
)
def test_multi_statement_rejected(sql: str) -> None:
    with pytest.raises(ProgrammingError, match="one statement at a time"):
        _classify_caller_sql(sql, None)


def test_wrong_param_count_rejected() -> None:
    with pytest.raises(ProgrammingError, match="Incorrect number of bindings"):
        _classify_caller_sql("SELECT ?, ?", [1])


def test_wrong_param_count_too_many_rejected() -> None:
    with pytest.raises(ProgrammingError, match="Incorrect number of bindings"):
        _classify_caller_sql("SELECT ?", [1, 2])


def test_correct_param_count_accepted() -> None:
    _classify_caller_sql("SELECT ?, ?", [1, 2])


def test_no_placeholders_no_params_accepted() -> None:
    _classify_caller_sql("SELECT 1", None)
    _classify_caller_sql("SELECT 1", [])


def test_question_mark_inside_string_literal_not_counted() -> None:
    _classify_caller_sql("SELECT '?'", [])


def test_question_mark_inside_comment_not_counted() -> None:
    _classify_caller_sql("SELECT 1 -- ?", [])


def test_semicolon_inside_string_literal_not_counted() -> None:
    _classify_caller_sql("INSERT INTO t VALUES (';')", [])


def test_non_sized_iterable_silently_skips_count_check() -> None:
    """``len()`` on a generator raises ``TypeError``; classifier catches
    it and defers to the binding layer (a ProgrammingError) so the
    rejection stays inside ``except dqlitedbapi.Error:``."""

    def gen() -> Iterator[int]:
        yield 1
        yield 2

    _classify_caller_sql("SELECT ?, ?", gen())  # type: ignore[arg-type]
    _classify_caller_sql("SELECT ?", iter([1]))  # type: ignore[arg-type]
