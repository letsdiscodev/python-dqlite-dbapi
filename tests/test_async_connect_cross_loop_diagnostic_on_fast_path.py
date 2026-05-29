"""connect() surfaces the cross-loop binding diagnostic on its own call frame
(not deferred to a later cursor/execute), preserving the fail-fast shape of the
eager-connect health probe: _check_loop_binding runs ahead of the fast-path return."""

from __future__ import annotations

import asyncio
import threading
from typing import Any

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import ProgrammingError


def _invoke_on_other_loop(conn: AsyncConnection, action: Any) -> BaseException | None:
    """Run action(conn) on a fresh loop in a worker thread; return the exception."""
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
    conn = AsyncConnection("127.0.0.1:9001")
    conn._ensure_locks()  # bind to this (the test's) loop

    # Pretend already-connected so the fast-path return arm fires without a dial.
    sentinel = object()
    conn._async_conn = sentinel  # type: ignore[assignment]

    err = _invoke_on_other_loop(conn, lambda c: c.connect())
    assert isinstance(err, ProgrammingError), (
        f"cross-loop connect must surface ProgrammingError at its own call frame; got {err!r}"
    )
    assert "different event loop" in str(err)
