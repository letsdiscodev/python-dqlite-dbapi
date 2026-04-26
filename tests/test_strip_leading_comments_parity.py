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
    ],
)
def test_strip_leading_comments_parity(sql: str) -> None:
    """Both helpers must agree on every input. Drift is the bug."""
    assert client_strip(sql) == dbapi_strip(sql), (
        f"client and dbapi _strip_leading_comments diverged for {sql!r}: "
        f"client={client_strip(sql)!r} dbapi={dbapi_strip(sql)!r}"
    )
