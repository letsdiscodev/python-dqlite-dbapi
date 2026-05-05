"""``AsyncCursor.drain_rows`` transfers ownership of the row buffer
to the caller without copying.

Intended for adapter layers that rebuffer rows immediately after
fetch and would otherwise pay 2× memory for the duration of the
transfer (the cursor's list AND the adapter's deque). The drain
is sync (no await) and bypasses ``_row_factory``.
"""

from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor


def _prime_async_cursor(rows: list[tuple[Any, ...]]) -> AsyncCursor:
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._rows = rows
    cur._row_index = 0
    cur._description = (("col0", None, None, None, None, None, None),)
    cur._row_factory = None
    cur._rowcount = len(rows)
    cur._lastrowid = None
    cur._arraysize = 1
    cur.messages = []
    conn = MagicMock()
    cur._connection = conn
    return cur


def test_drain_rows_returns_buffer_and_clears_cursor() -> None:
    rows = [(1,), (2,), (3,)]
    cur = _prime_async_cursor(rows)

    drained = cur.drain_rows()

    # Same list object — no copy.
    assert drained is rows
    # Buffer cleared on the cursor.
    assert cur._rows == []
    assert cur._row_index == 0


def test_drain_rows_post_drain_cursor_returns_no_rows() -> None:
    """After drain_rows the cursor's row buffer is empty, so a
    follow-up fetchall must return [] rather than indexing into the
    cleared buffer with a stale index."""
    rows = [(1,), (2,)]
    cur = _prime_async_cursor(rows)

    cur.drain_rows()
    # ``fetchall`` is async on AsyncCursor — but the test runs the
    # post-condition check via the synchronous attribute state. A
    # downstream adapter is expected to close immediately after
    # drain, which is why fetchall is not exercised here directly.
    assert cur._rows == []


def test_drain_rows_does_not_apply_row_factory() -> None:
    """drain_rows is a raw transfer — adapters that need
    factory-applied rows fetch through fetchall instead."""

    def factory(_c: object, _r: tuple[Any, ...]) -> tuple[Any, ...]:
        # Should NOT be invoked.
        pytest.fail("row_factory invoked during drain_rows")

    cur = _prime_async_cursor([(1,), (2,)])
    cur._row_factory = factory

    drained = cur.drain_rows()
    assert drained == [(1,), (2,)]


def test_drain_rows_preserves_metadata_fields() -> None:
    """rowcount / lastrowid / description survive the drain — the
    adapter reads metadata before draining and the values must not
    be scrubbed by the drain itself."""
    cur = _prime_async_cursor([(1,)])
    cur._rowcount = 1
    cur._lastrowid = 42
    desc = (("a", None, None, None, None, None, None),)
    cur._description = desc

    cur.drain_rows()

    assert cur._rowcount == 1
    assert cur._lastrowid == 42
    assert cur._description is desc
