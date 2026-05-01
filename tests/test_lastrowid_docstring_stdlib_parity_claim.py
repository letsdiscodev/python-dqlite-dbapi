"""Pin: ``Cursor.lastrowid`` and ``AsyncCursor.lastrowid`` docstrings
must NOT claim cursor-scoped semantics is divergent from stdlib —
stdlib ``sqlite3.Cursor.lastrowid`` is also cursor-scoped, so the
"Unlike ``sqlite3.Cursor.lastrowid``" framing inverts the truth.

Verifies the docstring is positively framed (parity, not divergence)
on the cursor-scope dimension. The separate ``INSERT ... RETURNING``
divergence remains accurately documented as a divergence.
"""

from __future__ import annotations

import sqlite3

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def test_stdlib_lastrowid_is_per_cursor_not_per_connection() -> None:
    """Anchor: stdlib ``sqlite3.Cursor.lastrowid`` is cursor-scoped.

    A sibling cursor that has not run an INSERT keeps ``None`` —
    proving the docstring's "Unlike stdlib" framing is wrong.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    c1 = conn.cursor()
    c2 = conn.cursor()
    c1.execute("INSERT INTO t (v) VALUES ('a')")
    assert c1.lastrowid == 1
    assert c2.lastrowid is None


def test_sync_cursor_lastrowid_docstring_does_not_claim_unlike_stdlib() -> None:
    doc = Cursor.lastrowid.__doc__ or ""
    assert "Unlike ``sqlite3.Cursor.lastrowid``" not in doc, (
        "Docstring inverts truth: stdlib lastrowid IS cursor-scoped"
    )
    assert "matching stdlib" in doc.lower() or "matches stdlib" in doc.lower(), (
        "Docstring should positively frame the cursor-scoped parity with stdlib"
    )


def test_async_cursor_lastrowid_docstring_does_not_claim_unlike_stdlib() -> None:
    doc = AsyncCursor.lastrowid.__doc__ or ""
    assert "Unlike ``sqlite3.Cursor.lastrowid``" not in doc, (
        "Docstring inverts truth: stdlib lastrowid IS cursor-scoped"
    )
    assert "matching stdlib" in doc.lower() or "matches stdlib" in doc.lower(), (
        "Docstring should positively frame the cursor-scoped parity with stdlib"
    )


def test_sync_lastrowid_docstring_keeps_returning_divergence_note() -> None:
    """The third-paragraph divergence (``INSERT ... RETURNING``) is
    accurate and load-bearing. Pin so a future docstring rewrite
    does not strip it."""
    doc = Cursor.lastrowid.__doc__ or ""
    assert "INSERT ... RETURNING" in doc
    assert "divergence" in doc.lower()


def test_async_lastrowid_docstring_keeps_returning_divergence_note() -> None:
    doc = AsyncCursor.lastrowid.__doc__ or ""
    assert "INSERT ... RETURNING" in doc
    assert "divergence" in doc.lower()
