"""close() must drop the strong back-reference to the parent connection
(via weakref.proxy) so a retained closed cursor does not pin the
connection and its daemon loop thread."""

from __future__ import annotations

import gc
import weakref

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


def test_sync_cursor_close_releases_connection_pin() -> None:
    conn = Connection("localhost:9001", timeout=1.0)
    cur = conn.cursor()
    conn_ref = weakref.ref(conn)

    cur.close()
    del conn
    gc.collect()

    assert conn_ref() is None, (
        "Closed cursor must not pin its parent Connection — the "
        "_connection back-reference is strong by default; close() "
        "must replace it with a weakref proxy so the Connection's "
        "daemon loop thread can be reaped promptly."
    )


async def test_async_cursor_close_releases_connection_pin() -> None:
    aconn = AsyncConnection("localhost:9001")
    cur = aconn.cursor()
    aconn_ref = weakref.ref(aconn)

    cur.close()
    del aconn
    gc.collect()

    assert aconn_ref() is None, (
        "Closed AsyncCursor must not pin its parent AsyncConnection "
        "— the _connection back-reference must be replaced with a "
        "weakref proxy on close so the connection's loop-bound "
        "primitives are not held alive past the user's intended "
        "lifetime."
    )
