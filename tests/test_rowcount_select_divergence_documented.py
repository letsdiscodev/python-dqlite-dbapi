"""Pin: ``Cursor.rowcount`` after SELECT returns the buffered row
count (a deliberate divergence from stdlib ``sqlite3.Cursor`` /
psycopg2 / aiosqlite which all return ``-1`` because they cannot
know the count without consuming). dqlite's wire layer buffers
the entire result set up front, and the RETURNING path (DML +
RETURNING is classified as row-returning) relies on this real
count for SQLAlchemy's insertmanyvalues contract.

PEP 249 §6.1.2 explicitly permits a real count for DQL:
"the number of rows that the last execute*() produced (for DQL
statements like SELECT)" — so this is divergence-from-peers,
not spec violation. The inline comment in the SELECT branch
documents the divergence and points cross-driver porters at the
portable idiom (``cur.fetchone() is not None``).
"""

from __future__ import annotations

import inspect


def test_select_rowcount_divergence_documented_in_sync_cursor() -> None:
    """The SELECT-branch inline comment must call out the divergence
    from stdlib / psycopg2 / aiosqlite so a future stdlib-parity
    refactor lands as a deliberate choice rather than a silent
    flip.
    """
    import dqlitedbapi.cursor as cursor_mod

    source = inspect.getsource(cursor_mod)
    assert "divergence from stdlib" in source.lower() or "diverges from stdlib" in source.lower()
    assert "len(rows)" in source
    assert "RETURNING" in source or "insertmanyvalues" in source


def test_select_rowcount_divergence_documented_in_async_cursor() -> None:
    """The aio sibling must carry an equivalent caveat (pointing at
    the sync sibling is fine).
    """
    import dqlitedbapi.aio.cursor as aio_cursor_mod

    source = inspect.getsource(aio_cursor_mod)
    # Either an inline pin or a "see sync sibling" reference.
    assert "rowcount" in source.lower()
    assert (
        "buffer" in source.lower() or "RETURNING" in source or "see sync sibling" in source.lower()
    )
