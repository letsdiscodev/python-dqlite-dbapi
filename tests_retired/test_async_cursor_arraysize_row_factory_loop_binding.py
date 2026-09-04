"""``arraysize`` / ``row_factory`` setters enforce loop-binding affinity: a cross-loop call
raises ``ProgrammingError`` rather than silently mutating the bound loop's behaviour."""

from __future__ import annotations

import asyncio
import threading

from dqlitedbapi import ProgrammingError
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


async def test_arraysize_setter_rejects_cross_loop_call() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()  # prime binding on the outer loop

    errors: list[BaseException] = []

    def _on_other_loop() -> None:
        async def _invoke() -> None:
            try:
                cur.arraysize = 2
            except BaseException as e:
                errors.append(e)

        asyncio.run(_invoke())

    t = threading.Thread(target=_on_other_loop)
    t.start()
    t.join()
    assert errors, "expected a ProgrammingError from the other loop's setter call"
    assert isinstance(errors[0], ProgrammingError)


async def test_row_factory_setter_rejects_cross_loop_call() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()

    errors: list[BaseException] = []

    def _on_other_loop() -> None:
        async def _invoke() -> None:
            try:
                cur.row_factory = lambda c, r: dict(
                    zip([d[0] for d in c.description], r, strict=False)
                )
            except BaseException as e:
                errors.append(e)

        asyncio.run(_invoke())

    t = threading.Thread(target=_on_other_loop)
    t.start()
    t.join()
    assert errors, "expected a ProgrammingError from the other loop's setter call"
    assert isinstance(errors[0], ProgrammingError)


async def test_setters_accept_same_loop_call() -> None:
    """The binding check must NOT reject a call from the same loop."""
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()
    cur.arraysize = 5
    cur.row_factory = None
