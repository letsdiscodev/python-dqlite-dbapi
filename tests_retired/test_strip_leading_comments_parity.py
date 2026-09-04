"""``_strip_leading_comments`` is byte-equivalent in dbapi and client (duplicated to avoid
an inter-package dependency); drift surfaces as a transaction-tracker desync."""

from __future__ import annotations

import pytest

from dqliteclient.connection import _strip_leading_comments as client_strip
from dqlitedbapi.cursor import _strip_leading_comments as dbapi_strip


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "BEGIN",
        "  SAVEPOINT sp",
        "-- header\nSELECT 1",
        "-- header",
        "/* annotation */ SELECT 1",
        "/* annotation */SELECT 1",
        "/* multi\nline\ncomment */ SELECT 1",
        "-- one\n-- two\nSELECT 1",
        "/* block */-- line\nSELECT 1",
        "-- line\n/* block */SELECT 1",
        # SQLite has no nested block comments: the comment ends at the first */.
        "/* outer /* inner */ */ SELECT 1",
        "  /* with leading ws */ SELECT 1  ",
        "\n\n/* leading newlines */SELECT 1",
        "\t/* leading tab */SELECT 1",
        "",
        "-- only a comment",
        "/* */",
        "/* */-- ",
        "/* never closed",
        "/* unterminated\nspans lines",
        "-- comment without newline",
        # SQLite ends -- comments only on \n, so CR alone does not terminate.
        "-- x\rfoo",
        "-- x\r\nfoo",
    ],
)
def test_strip_leading_comments_parity(sql: str) -> None:
    assert client_strip(sql) == dbapi_strip(sql), (
        f"client and dbapi _strip_leading_comments diverged for {sql!r}: "
        f"client={client_strip(sql)!r} dbapi={dbapi_strip(sql)!r}"
    )


def test_unterminated_block_comment_returns_empty() -> None:
    """An unterminated /* consumes everything, returning "" (no usable verb)."""
    for impl in (client_strip, dbapi_strip):
        assert impl("/* never closed") == ""
        assert impl("/* unterminated\nspans lines") == ""
        assert impl("/* close */ /* unterminated") == ""


def test_cr_only_does_not_terminate_line_comment() -> None:
    """SQLite ends -- comments only on \\n, so CR alone is part of the comment."""
    for impl in (client_strip, dbapi_strip):
        assert impl("-- x\rfoo") == ""
        assert impl("-- x\r\nfoo") == "foo"
