"""``autocommit``/``isolation_level``/``text_factory`` setters enforce loop-binding even on
their no-op accept-paths; cross-loop callers must surface ``ProgrammingError``."""

from __future__ import annotations

import asyncio
import threading

from dqlitedbapi import ProgrammingError
from dqlitedbapi.aio.connection import AsyncConnection


def _run_on_other_loop_setter(
    conn: AsyncConnection, setter: str, value: object
) -> BaseException | None:
    captured: list[BaseException] = []

    def _runner() -> None:
        async def _invoke() -> None:
            try:
                setattr(conn, setter, value)
            except BaseException as e:
                captured.append(e)

        asyncio.run(_invoke())

    t = threading.Thread(target=_runner)
    t.start()
    t.join()
    return captured[0] if captured else None


async def test_autocommit_setter_rejects_cross_loop_call() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    conn._ensure_locks()  # prime loop binding
    err = _run_on_other_loop_setter(conn, "autocommit", True)
    assert isinstance(err, ProgrammingError)


async def test_isolation_level_setter_rejects_cross_loop_call() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    conn._ensure_locks()
    err = _run_on_other_loop_setter(conn, "isolation_level", None)
    assert isinstance(err, ProgrammingError)


async def test_text_factory_setter_rejects_cross_loop_call() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    conn._ensure_locks()
    err = _run_on_other_loop_setter(conn, "text_factory", str)
    assert isinstance(err, ProgrammingError)


async def test_setters_accept_same_loop_call() -> None:
    """Sanity: same-loop setter calls must NOT raise."""
    conn = AsyncConnection("127.0.0.1:9001")
    conn._ensure_locks()
    conn.autocommit = True
    conn.isolation_level = None
    conn.text_factory = str
