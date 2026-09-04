"""Pin _is_multi_statement on ``;;`` and ``; <comment> ;`` tails. dqlite's
prepare silently drops everything past the first ``;``, so this classifier is
the security guard against stacked-query smuggling."""

from dqlitedbapi.cursor import _is_multi_statement


def test_double_semicolon_at_end_classified_as_multi() -> None:
    """Conservative classification: ``;;`` is treated as multi-statement.
    False-positive, but safer than masking real ``; <noise> ; INSERT``."""
    assert _is_multi_statement("INSERT INTO t VALUES(1);;") is True


def test_semicolon_then_comment_then_semicolon_classified_as_multi() -> None:
    """A trailing ``;`` after the first (comments/whitespace between) trips the
    classifier; acceptable false-positive that prevents smuggling."""
    assert _is_multi_statement("INSERT INTO t VALUES(1); -- end\n;") is True
    assert _is_multi_statement("INSERT INTO t VALUES(1); /* */ ;") is True


def test_double_semicolon_then_real_second_statement_IS_multi() -> None:
    assert _is_multi_statement("INSERT INTO t VALUES(1);; INSERT INTO t VALUES(2)") is True


def test_trailing_whitespace_after_single_semicolon_is_not_multi() -> None:
    """A single trailing ``;`` followed only by whitespace/comments is NOT
    multi."""
    assert _is_multi_statement("INSERT INTO t VALUES(1);   ") is False
    assert _is_multi_statement("INSERT INTO t VALUES(1); -- comment\n") is False
    assert _is_multi_statement("INSERT INTO t VALUES(1); /* trailing */") is False


def test_single_statement_is_not_multi() -> None:
    assert _is_multi_statement("SELECT 1") is False
    assert _is_multi_statement("SELECT 1;") is False


def test_real_multi_statement_classified_correctly() -> None:
    assert _is_multi_statement("SELECT 1; SELECT 2") is True
    assert _is_multi_statement("INSERT INTO t VALUES(1); SELECT * FROM t") is True
