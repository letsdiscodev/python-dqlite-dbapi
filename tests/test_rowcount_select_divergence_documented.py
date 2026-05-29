"""``Cursor.rowcount`` after SELECT returns the buffered row count, a deliberate
divergence from peers (which return -1) that RETURNING relies on; the source
SELECT branch must document it."""

from __future__ import annotations

import inspect


def test_select_rowcount_divergence_documented_in_sync_cursor() -> None:
    """The SELECT-branch comment must call out the divergence from peers."""
    import dqlitedbapi.cursor as cursor_mod

    source = inspect.getsource(cursor_mod)
    assert "divergence from stdlib" in source.lower() or "diverges from stdlib" in source.lower()
    assert "len(rows)" in source
    assert "RETURNING" in source or "insertmanyvalues" in source


def test_select_rowcount_divergence_documented_in_async_cursor() -> None:
    """The aio sibling must carry an equivalent caveat (or point at the sync one)."""
    import dqlitedbapi.aio.cursor as aio_cursor_mod

    source = inspect.getsource(aio_cursor_mod)
    assert "rowcount" in source.lower()
    assert (
        "buffer" in source.lower() or "RETURNING" in source or "see sync sibling" in source.lower()
    )
