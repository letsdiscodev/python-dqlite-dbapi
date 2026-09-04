"""``AsyncCursor.__repr__`` must not leak ``ReferenceError`` for a GC'd parent.

After close, ``_connection`` is a weakref.proxy; once the parent is GC'd the
proxy's attribute access raises ReferenceError (before getattr's default fires),
and ReferenceError is outside the ``dbapi.Error`` hierarchy.
"""

from __future__ import annotations

import gc
import weakref

from dqlitedbapi.aio import AsyncConnection


async def test_async_cursor_repr_does_not_leak_referenceerror_on_gced_parent() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    cur.close()
    # Re-wrap to ensure the proxy is dead even if a strong ref lingered.
    cur._connection = weakref.proxy(conn)
    del conn
    gc.collect()

    text = repr(cur)  # must NOT raise ReferenceError

    assert "AsyncCursor" in text
    assert "closed" in text
    assert "address='?'" in text or 'address="?"' in text
