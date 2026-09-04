"""Pin: ``AsyncConnection.in_transaction`` returns ``False`` for a closed
connection read from a foreign loop — the closed short-circuit must run
before ``_check_loop_only()``. Mirrors the sync sibling's ordering fix."""

from __future__ import annotations

import asyncio
import threading
import weakref

from dqlitedbapi.aio.connection import AsyncConnection


def test_in_transaction_returns_false_on_closed_async_conn_from_foreign_loop() -> None:
    """Closed on loop A, read ``in_transaction`` from loop B on another thread:
    the closed short-circuit precedes the loop check, so it returns False."""

    loop_a = asyncio.new_event_loop()

    async def _bind_then_close() -> AsyncConnection:
        conn = AsyncConnection.__new__(AsyncConnection)
        conn._closed = True
        conn._async_conn = None
        conn._loop_ref = weakref.ref(asyncio.get_running_loop())
        return conn

    conn = loop_a.run_until_complete(_bind_then_close())

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
