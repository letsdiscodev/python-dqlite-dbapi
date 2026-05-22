"""Pin: ``AsyncConnection.in_transaction`` returns ``False`` for a
closed connection read from a foreign loop. Mirrors the sync
sibling's closed-then-thread ordering fix.

The docstring documents the property as "safe to use in shutdown
paths that need to decide whether to commit or rollback before
close, without an extra closed-state try / except scaffold."
Before the fix, ``_check_loop_only()`` ran BEFORE the closed
short-circuit; a foreign-loop reader of a closed connection got
``LoopError`` instead of the documented ``False``.

Sync sibling pin: ``tests/test_in_transaction_closed_foreign_thread.py``.
"""

from __future__ import annotations

import asyncio
import threading
import weakref

from dqlitedbapi.aio.connection import AsyncConnection


def test_in_transaction_returns_false_on_closed_async_conn_from_foreign_loop() -> None:
    """Open the connection on loop A, close it, then read
    ``in_transaction`` from a separate event loop B running on a
    different thread. The closed short-circuit must precede the
    loop-affinity check so the read returns ``False`` rather than
    raising.
    """

    loop_a = asyncio.new_event_loop()

    async def _bind_then_close() -> AsyncConnection:
        conn = AsyncConnection.__new__(AsyncConnection)
        conn._closed = True
        conn._async_conn = None
        # Capture loop A as the bound loop, mirroring the real
        # `_capture_running_loop` discipline (weakref.ref).
        conn._loop_ref = weakref.ref(asyncio.get_running_loop())
        return conn

    conn = loop_a.run_until_complete(_bind_then_close())

    # Read from loop B on a different thread.
    result: dict[str, object] = {}

    def reader_in_loop_b() -> None:
        loop_b = asyncio.new_event_loop()
        asyncio.set_event_loop(loop_b)

        async def _read() -> None:
            try:
                result["value"] = conn.in_transaction
            except BaseException as exc:
                result["error"] = exc

        try:
            loop_b.run_until_complete(_read())
        finally:
            loop_b.close()

    t = threading.Thread(target=reader_in_loop_b)
    t.start()
    t.join()

    loop_a.close()

    assert "error" not in result, (
        "AsyncConnection.in_transaction on a closed connection must "
        f"return False from a foreign loop; got error {result.get('error')!r}. "
        "The closed short-circuit must run BEFORE _check_loop_only so "
        "closed-state behaviour is loop-independent."
    )
    assert result.get("value") is False
