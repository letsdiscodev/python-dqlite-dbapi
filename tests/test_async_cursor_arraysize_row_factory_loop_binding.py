"""``AsyncCursor.arraysize`` and ``AsyncCursor.row_factory`` setters
must enforce the parent connection's loop-binding affinity contract:
a state-mutating setter call from a different event loop surfaces
``ProgrammingError`` rather than silently changing the bound loop's
behaviour.

Mirror of the sync sibling pin in
``test_thread_safety_enforcement.py``. The Connection class
docstring claims every public method on a Connection-allocated
cursor enforces the affinity contract; ``arraysize`` and
``row_factory`` setters were the missing surfaces.
"""

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
    """Sanity: the binding check must NOT reject a call from the same
    loop the connection was first used on."""
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()
    cur.arraysize = 5
    cur.row_factory = None
