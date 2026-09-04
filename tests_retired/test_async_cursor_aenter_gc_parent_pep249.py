"""Pin: ``AsyncCursor.__aenter__`` translates ``ReferenceError`` from a GC'd parent into
``InterfaceError`` (inside the PEP 249 hierarchy), mirroring ``__aiter__``."""

from __future__ import annotations

import gc

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection


async def test_aenter_on_closed_cursor_with_gc_parent_raises_interface_error() -> None:
    """A weakref.proxy from a GC'd AsyncConnection must not leak ``ReferenceError``."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    cur.close()
    del conn
    gc.collect()
    with pytest.raises(dqlitedbapi.InterfaceError):
        async with cur:
            pass
