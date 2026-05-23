"""Pin: ``executemany``'s PRAGMA-specific diagnostic fires even when
the SQL has a leading SQL comment.

The diagnostic at the row-returning reject site used the raw
``operation.lstrip().upper()`` instead of the already-computed
comment-stripped ``head_normalised``. So a user writing ``-- comment
\\nPRAGMA foreign_keys`` got the generic "use execute() for
SELECT / VALUES / PRAGMA / EXPLAIN / WITH" message rather than the
PRAGMA-specific "per-call semantics" guidance. Inconsistent UX
between equivalent SQL inputs.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError


def _make_cursor() -> Cursor:
    """Build a Cursor whose guards are tame enough for the synchronous
    executemany pre-flight to run."""
    cursor = Cursor.__new__(Cursor)
    cursor._closed = False
    cursor.messages = []
    cursor._arraysize = 1
    cursor._description = None
    cursor._rowcount = -1
    cursor._rows = []
    cursor._row_index = 0
    cursor._lastrowid = None
    cursor._row_factory = None
    cursor._completed_iterations = 0
    conn = MagicMock()
    conn._creator_thread = None
    conn._creator_pid = -1
    conn._check_thread = lambda: None
    conn._check_pid = lambda: None
    conn._closed = False
    conn._pool_released = False
    cursor._connection = conn
    return cursor


@pytest.mark.parametrize(
    "sql",
    [
        "-- comment\nPRAGMA foreign_keys",
        "/* multi\nline */\nPRAGMA journal_mode",
        "-- a\n-- b\nPRAGMA wal_checkpoint",
    ],
)
def test_executemany_pragma_after_comment_emits_pragma_diagnostic(sql: str) -> None:
    cursor = _make_cursor()
    with pytest.raises(ProgrammingError, match="PRAGMA"):
        cursor.executemany(sql, [(1,), (2,)])


def test_executemany_pragma_with_no_comment_still_pragma_specific() -> None:
    """Sanity: the comment-free form continues to hit the PRAGMA-
    specific arm."""
    cursor = _make_cursor()
    with pytest.raises(ProgrammingError, match="per-call semantics"):
        cursor.executemany("PRAGMA foreign_keys", [(1,)])


def test_executemany_select_falls_through_to_generic_message() -> None:
    """Negative: a non-PRAGMA row-returning verb still takes the
    generic diagnostic."""
    cursor = _make_cursor()
    with pytest.raises(ProgrammingError, match="DML statements"):
        cursor.executemany("SELECT 1", [(1,)])
