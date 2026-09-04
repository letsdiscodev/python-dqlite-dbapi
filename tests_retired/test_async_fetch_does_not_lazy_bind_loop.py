"""``AsyncCursor.fetch*`` and ``executescript`` use the non-binding loop helper
``_check_loop_binding`` (not ``_ensure_locks``) so a fresh cursor's first call
does not lazy-bind the connection's loop before the result-set guard fires.
"""

from __future__ import annotations

import asyncio
import contextlib


def test_fresh_cursor_fetch_does_not_bind_connection_loop() -> None:
    """A fresh cursor's first call being fetchone() raises without lazy-binding the loop."""
    from dqlitedbapi.aio.connection import AsyncConnection
    from dqlitedbapi.aio.cursor import AsyncCursor
    from dqlitedbapi.exceptions import ProgrammingError

    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._loop_ref = None  # not bound
    aconn._async_conn = None
    aconn._creator_pid = 0  # bypass fork check below
    import os

    aconn._creator_pid = os.getpid()

    cursor = AsyncCursor(aconn)
    cursor._description = None  # no result set
    cursor._rows = []

    loop = asyncio.new_event_loop()
    try:

        async def _drive() -> None:
            with contextlib.suppress(ProgrammingError):
                await cursor.fetchone()
            assert aconn._loop_ref is None, "fetchone on a fresh cursor must not lazy-bind the loop"

        loop.run_until_complete(_drive())
    finally:
        loop.close()
