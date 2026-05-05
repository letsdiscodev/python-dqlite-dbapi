"""Pin: ``_is_multi_statement`` consecutive-semicolon walker
correctly handles ``;;`` (empty statement) and ``; <comment> ;``
(only-comment statement) without misclassifying them as
multi-statement.

dqlite's prepare silently drops everything past the first ``;``,
so the classifier is the security guard preventing stacked-query
smuggling. A regression that collapses the walker would either:
  - Reject valid SQL terminated with ``;;`` (false positive), OR
  - Smuggle multi-statement SQL with comment-only tails (false
    negative).

Direct unit-tests of the walker at cursor.py:533-536. Companion
to the existing leading-semicolon and tracker-desync coverage.
"""

from dqlitedbapi.cursor import _is_multi_statement


def test_double_semicolon_at_end_classified_as_multi() -> None:
    """Conservative classification: ``;;`` produces a non-empty tail
    after the first ``;`` (the second ``;`` itself), and the walker
    treats it as multi-statement. False-positive, but safer than
    false-negative since dqlite's prepare path silently drops
    everything past the first ``;``. A regression that makes this
    return False would also mask real ``; <noise> ; INSERT`` as
    single-statement."""
    assert _is_multi_statement("INSERT INTO t VALUES(1);;") is True


def test_semicolon_then_comment_then_semicolon_classified_as_multi() -> None:
    """Same conservative posture: a trailing ``;`` after the first
    ``;`` (with only comments / whitespace between) trips the
    classifier. The walker is the security guard — false-positive
    here is acceptable and prevents stacked-query smuggling."""
    assert _is_multi_statement("INSERT INTO t VALUES(1); -- end\n;") is True
    assert _is_multi_statement("INSERT INTO t VALUES(1); /* */ ;") is True


def test_double_semicolon_then_real_second_statement_IS_multi() -> None:
    """Definitive multi-statement: trailing INSERT after ``;;``."""
    assert _is_multi_statement("INSERT INTO t VALUES(1);; INSERT INTO t VALUES(2)") is True


def test_trailing_whitespace_after_single_semicolon_is_not_multi() -> None:
    """A single trailing ``;`` followed only by whitespace / comments
    must NOT be classified as multi — that's the docstring's
    explicit contract."""
    assert _is_multi_statement("INSERT INTO t VALUES(1);   ") is False
    assert _is_multi_statement("INSERT INTO t VALUES(1); -- comment\n") is False
    assert _is_multi_statement("INSERT INTO t VALUES(1); /* trailing */") is False


def test_single_statement_is_not_multi() -> None:
    assert _is_multi_statement("SELECT 1") is False
    assert _is_multi_statement("SELECT 1;") is False


def test_real_multi_statement_classified_correctly() -> None:
    assert _is_multi_statement("SELECT 1; SELECT 2") is True
    assert _is_multi_statement("INSERT INTO t VALUES(1); SELECT * FROM t") is True
