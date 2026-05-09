"""Pin: ``AsyncCursor.__aenter__`` translates ``ReferenceError`` from a
GC'd parent ``AsyncConnection`` to ``InterfaceError``, mirroring
``__aiter__``'s discipline.

Without this pin, ``async with cur:`` on a closed cursor whose parent
has been garbage-collected raises bare ``ReferenceError`` (outside the
PEP 249 ``Error`` hierarchy) when the proxy attribute access fails.
"""

from __future__ import annotations

import gc

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection


async def test_aenter_on_closed_cursor_with_gc_parent_raises_interface_error() -> None:
    """A weakref.proxy from a GC'd AsyncConnection must not leak
    ``ReferenceError`` out of ``__aenter__``."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    await cur.close()
    # Drop the connection ref and force GC so the proxy referent
    # disappears.
    del conn
    gc.collect()
    with pytest.raises(dqlitedbapi.InterfaceError):
        async with cur:
            pass
