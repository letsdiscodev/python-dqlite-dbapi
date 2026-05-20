"""Pin: ``Cursor.__next__`` / ``AsyncCursor.__anext__`` preserve the
row index on a non-``StopIteration`` row_factory raise.

This is a CONSCIOUS divergence from stdlib ``sqlite3`` (which
consumes the row before applying the factory). The driver applies
the factory BEFORE advancing ``_row_index`` so ``fetchmany``'s
snapshot/restore retry semantic (``snapshot + len(result)`` on
cancel) does not silently lose the un-delivered row. The
``__next__`` / ``__anext__`` paths inherit that property via the
shared ``_next_row_unlocked`` helper.

The trade-off: after a factory raise, the SAME row is retried on the
next ``__next__`` / ``__anext__``. Callers that want to skip past
the bad row must drop and re-fetch via a fresh ``execute``. The
docstring on both methods documents the divergence; this pin
catches a future "fix" that flips the order in ``_next_row_unlocked``
and breaks ``fetchmany``'s contract.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def _prime_sync_cursor(rows: list[tuple[Any, ...]]) -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._rows = rows
    cur._row_index = 0
    cur._description = (("col0", None, None, None, None, None, None),)
    cur._row_factory = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._arraysize = 1
    cur.messages = []
    conn = MagicMock()
    conn._check_thread = MagicMock()
    cur._connection = conn
    return cur


def _prime_async_cursor(rows: list[tuple[Any, ...]]) -> AsyncCursor:
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._rows = rows
    cur._row_index = 0
    cur._description = (("col0", None, None, None, None, None, None),)
    cur._row_factory = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._arraysize = 1
    cur._executing_task = None
    cur._completed_iterations = 0
    cur.messages = []
    conn = MagicMock()
    conn._check_loop_binding = MagicMock()
    cur._connection = conn
    return cur


def test_sync_next_factory_raise_does_not_advance_index() -> None:
    """``__next__`` on a row that the factory rejects must leave the
    index unchanged so the un-delivered row remains pointed to."""
    cur = _prime_sync_cursor([(1,), (42,), (100,)])

    def factory(_c: object, row: tuple[Any, ...]) -> tuple[Any, ...]:
        if row[0] == 42:
            raise ValueError("don't like 42")
        return row

    cur._row_factory = factory

    # First row consumes cleanly.
    assert next(cur) == (1,)
    assert cur._row_index == 1

    # Second row trips the factory.
    with pytest.raises(ValueError, match="don't like 42"):
        next(cur)
    assert cur._row_index == 1, (
        "row_index must NOT advance on factory raise — fetchmany's "
        "snapshot/restore retry semantic depends on it"
    )

    # The SAME row is retried — confirms the documented divergence.
    with pytest.raises(ValueError, match="don't like 42"):
        next(cur)
    assert cur._row_index == 1


async def test_async_anext_factory_raise_does_not_advance_index() -> None:
    """Async sibling of the sync pin — ``__anext__`` must preserve
    the row index on factory raise so the un-delivered row remains
    available."""
    cur = _prime_async_cursor([(1,), (42,), (100,)])

    def factory(_c: object, row: tuple[Any, ...]) -> tuple[Any, ...]:
        if row[0] == 42:
            raise ValueError("don't like 42")
        return row

    cur._row_factory = factory

    # First row consumes cleanly.
    assert await anext(cur) == (1,)
    assert cur._row_index == 1

    # Second row trips the factory.
    with pytest.raises(ValueError, match="don't like 42"):
        await anext(cur)
    assert cur._row_index == 1, (
        "row_index must NOT advance on factory raise — fetchmany's "
        "snapshot/restore retry semantic depends on it"
    )

    # The SAME row is retried — confirms the documented divergence.
    with pytest.raises(ValueError, match="don't like 42"):
        await anext(cur)
    assert cur._row_index == 1


def test_sync_next_docstring_documents_replay_divergence() -> None:
    """The ``__next__`` docstring must call out the row-factory
    replay divergence from stdlib ``sqlite3`` so cross-driver code
    that retries on factory raise has a discoverable explanation."""
    doc = Cursor.__next__.__doc__ or ""
    assert "row_factory" in doc.lower()
    assert "row index" in doc.lower() or "row_index" in doc.lower()


def test_async_anext_docstring_documents_replay_divergence() -> None:
    """The ``__anext__`` docstring must call out the row-factory
    replay divergence from stdlib ``sqlite3`` / aiosqlite convention."""
    doc = AsyncCursor.__anext__.__doc__ or ""
    assert "row_factory" in doc.lower()
    assert "row index" in doc.lower() or "row_index" in doc.lower()
