"""Pin: ``_strip_leading_comments`` is byte-equivalent in dbapi and client.

The helper is duplicated in both packages (per the explicit comment at
``dqliteclient/connection.py:84-92`` — chosen to avoid an inter-package
dependency on the dbapi). Without a parity test, future drift surfaces
silently as a transaction-tracker desync (one helper recognises a new
comment style, the other doesn't).

The test lives in dbapi because dbapi already runtime-depends on
dqliteclient — importing both helpers from a single test file does not
introduce a new dependency edge.
"""

from __future__ import annotations

import pytest

from dqliteclient.connection import _strip_leading_comments as client_strip
from dqlitedbapi.cursor import _strip_leading_comments as dbapi_strip


@pytest.mark.parametrize(
    "sql",
    [
        # Plain SQL — no comments.
        "SELECT 1",
        "BEGIN",
        "  SAVEPOINT sp",
        # Single line comment.
        "-- header\nSELECT 1",
        "-- header",
        # Block comment.
        "/* annotation */ SELECT 1",
        "/* annotation */SELECT 1",
        "/* multi\nline\ncomment */ SELECT 1",
        # Consecutive line comments.
        "-- one\n-- two\nSELECT 1",
        # Block then line.
        "/* block */-- line\nSELECT 1",
        # Line then block.
        "-- line\n/* block */SELECT 1",
        # SQLite does NOT support nested block comments — both
        # implementations should agree on the (non-nested) interpretation.
        # `/* outer /* inner */` is the comment; ` */ SELECT 1` is the tail.
        "/* outer /* inner */ */ SELECT 1",
        # Whitespace handling.
        "  /* with leading ws */ SELECT 1  ",
        "\n\n/* leading newlines */SELECT 1",
        "\t/* leading tab */SELECT 1",
        # Empty / comment-only inputs.
        "",
        "-- only a comment",
        "/* */",
        "/* */-- ",
        # Unterminated block comment (defensive — both should agree).
        "/* never closed",
        "/* unterminated\nspans lines",
        # Unterminated `--` (mirror): consumes everything.
        "-- comment without newline",
        # CR-only line endings: SQLite's tokenizer requires \n to end
        # ``--`` comments. Both helpers MUST agree (CR is NOT a line
        # terminator) for parity with SQLite.
        "-- x\rfoo",
        "-- x\r\nfoo",  # CRLF: \n still ends the comment
    ],
)
def test_strip_leading_comments_parity(sql: str) -> None:
    """Both helpers must agree on every input. Drift is the bug."""
    assert client_strip(sql) == dbapi_strip(sql), (
        f"client and dbapi _strip_leading_comments diverged for {sql!r}: "
        f"client={client_strip(sql)!r} dbapi={dbapi_strip(sql)!r}"
    )


def test_unterminated_block_comment_returns_empty() -> None:
    """An unterminated ``/*`` consumes everything, mirroring the
    ``--`` branch's behavior. Pin so a future regression that
    returned the input verbatim would break this test.

    Unterminated comments are SQLite parse errors; the helper's job
    in that case is to surface "no usable verb" — empty-string return
    is the canonical signal across the helpers' callers."""
    for impl in (client_strip, dbapi_strip):
        assert impl("/* never closed") == ""
        assert impl("/* unterminated\nspans lines") == ""
        assert impl("/* close */ /* unterminated") == ""


def test_cr_only_does_not_terminate_line_comment() -> None:
    """SQLite's tokenizer terminates ``--`` only on ``\\n``. Pin
    agreement: ``-- x\\rfoo`` consumes everything (CR is part of the
    comment, no terminator)."""
    for impl in (client_strip, dbapi_strip):
        assert impl("-- x\rfoo") == ""
        # CRLF: the \n DOES end the comment normally.
        assert impl("-- x\r\nfoo") == "foo"
