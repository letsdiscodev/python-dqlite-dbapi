"""Pin: ``Cursor.close()`` / ``AsyncCursor.close()`` must release
the strong back-reference to the parent ``Connection`` /
``AsyncConnection`` so a closed cursor that the user retains
does not pin the connection — and its daemon event-loop thread,
``weakref.finalize`` registration, and asyncio primitives —
beyond the user's intended lifetime.

The connection's ``_cursors`` is already a ``WeakSet`` (cursor
falls out cleanly when the user drops it). The reverse direction
was strong: a closed cursor held in a debugger frame /
class-level cache / pytest fixture cache prevented the parent
connection from being GC'd. Mirror the ``WeakSet`` decoupling on
the closed-cursor side via ``weakref.proxy`` (preserves the
``cursor.connection`` API for as long as the connection is
alive — falls back to ``ReferenceError`` once the connection
is genuinely gone).
"""

from __future__ import annotations

import gc
import weakref

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


def test_sync_cursor_close_releases_connection_pin() -> None:
    conn = Connection("localhost:9001", timeout=1.0)
    cur = conn.cursor()
    conn_ref = weakref.ref(conn)

    cur.close()
    del conn
    gc.collect()

    # Without the fix, the closed cursor's strong _connection
    # ref keeps the Connection alive. With weakref.proxy on
    # close, the strong ref is dropped and the Connection can
    # be GC'd as soon as the user drops their own reference.
    assert conn_ref() is None, (
        "Closed cursor must not pin its parent Connection — the "
        "_connection back-reference is strong by default; close() "
        "must replace it with a weakref proxy so the Connection's "
        "daemon loop thread can be reaped promptly."
    )


@pytest.mark.asyncio
async def test_async_cursor_close_releases_connection_pin() -> None:
    aconn = AsyncConnection("localhost:9001")
    cur = aconn.cursor()
    aconn_ref = weakref.ref(aconn)

    await cur.close()
    del aconn
    gc.collect()

    assert aconn_ref() is None, (
        "Closed AsyncCursor must not pin its parent AsyncConnection "
        "— the _connection back-reference must be replaced with a "
        "weakref proxy on close so the connection's loop-bound "
        "primitives are not held alive past the user's intended "
        "lifetime."
    )
