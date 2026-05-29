"""Pin: ``AsyncCursor.__repr__`` does NOT leak ``ReferenceError`` when
the parent ``AsyncConnection`` has been garbage-collected.

After ``AsyncCursor.close()``, ``self._connection`` is swapped to a
``weakref.proxy``. When the parent ``AsyncConnection`` is later GC'd,
every attribute access on the proxy raises ``ReferenceError`` — and
``__repr__`` reads ``getattr(self._connection, "_address", "?")``: the
``getattr`` default fires only for a MISSING attribute, so the proxy
raises ``ReferenceError`` before the default is consulted.

``ReferenceError`` is outside the ``dbapi.Error`` hierarchy — every
``except dbapi.Error:`` block misses it, and ``repr()`` is called by
debuggers / loggers / pytest output. The sync sibling
``Cursor.__repr__`` already guards this case; the async repr must
mirror the discipline.
"""

from __future__ import annotations

import gc
import weakref

from dqlitedbapi.aio import AsyncConnection


async def test_async_cursor_repr_does_not_leak_referenceerror_on_gced_parent() -> None:
    """Close the cursor (swapping ``_connection`` to a ``weakref.proxy``),
    drop the parent ref and force GC, then ``repr(cur)`` must not raise.
    """
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    cur.close()
    # Belt-and-suspenders: ensure the proxy is dead even if a strong
    # ref lingered — re-wrap and drop, matching the sync sibling test.
    cur._connection = weakref.proxy(conn)
    del conn
    gc.collect()

    text = repr(cur)  # must NOT raise ReferenceError

    assert "AsyncCursor" in text
    assert "closed" in text
    # Address falls back to the canonical '?' placeholder, matching the
    # missing-attribute path and the sync sibling.
    assert "address='?'" in text or 'address="?"' in text
