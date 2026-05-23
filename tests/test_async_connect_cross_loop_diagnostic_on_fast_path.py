"""Pin: ``AsyncConnection.connect()`` surfaces the cross-loop
binding diagnostic directly on its own call frame, not deferred to
the next ``cursor()`` / ``execute()``.

The documented use of ``await aconn.connect()`` is an eager-TCP-open
/ fail-fast health probe (SA's pool_pre_ping-style pattern). Before
the fix, ``_ensure_connection``'s fast-path return on an
already-bound connection skipped ``_check_loop_binding``, so a
cross-loop ``await aconn.connect()`` succeeded silently and the
diagnostic surfaced only on a later cursor/execute -- defeating the
fail-fast shape.

The fix moves ``_check_loop_binding`` ahead of the fast-path return.
``transaction()``'s symmetric pre-call site is left in place
(idempotent under double invocation).
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import ProgrammingError


def _invoke_on_other_loop(conn: AsyncConnection, action: Any) -> BaseException | None:
    """Run ``action(conn)`` on a fresh event loop on a worker thread
    so the cross-loop diagnostic fires. Returns the exception (or
    None on success)."""
    captured: list[BaseException] = []

    def _runner() -> None:
        loop = asyncio.new_event_loop()
        try:
            try:
                loop.run_until_complete(action(conn))
            except BaseException as e:  # noqa: BLE001
                captured.append(e)
        finally:
            loop.close()

    t = threading.Thread(target=_runner)
    t.start()
    t.join()
    return captured[0] if captured else None


async def test_connect_on_already_bound_conn_from_foreign_loop_raises() -> None:
    """The eager-connect health probe must surface the cross-loop
    binding diagnostic at its own call frame, NOT silently return."""
    conn = AsyncConnection("127.0.0.1:9001")
    conn._ensure_locks()  # bind to this (the test's) loop

    # Pretend the connection is already-connected so the fast-path
    # return arm fires without needing a real wire dial.
    sentinel = object()
    conn._async_conn = sentinel  # type: ignore[assignment]

    err = _invoke_on_other_loop(conn, lambda c: c.connect())
    assert isinstance(err, ProgrammingError), (
        f"cross-loop connect must surface ProgrammingError at its own call frame; got {err!r}"
    )
    assert "different event loop" in str(err)
