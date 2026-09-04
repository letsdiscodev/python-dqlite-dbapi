"""Fetch / ``__aiter__`` route through ``_ensure_locks()`` so a cross-loop call raises
``ProgrammingError`` up front rather than silently reading pre-buffered rows."""

from __future__ import annotations

import asyncio
import threading

from dqlitedbapi import ProgrammingError
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


def _invoke_on_fresh_loop_in_thread(coro_factory) -> list[BaseException]:
    """Run ``await coro_factory()`` on a fresh ``asyncio.run`` in a thread; return errors."""
    errors: list[BaseException] = []

    def _runner() -> None:
        async def _invoke() -> None:
            try:
                await coro_factory()
            except BaseException as e:  # noqa: BLE001
                errors.append(e)

        asyncio.run(_invoke())

    t = threading.Thread(target=_runner)
    t.start()
    t.join()
    return errors


async def test_fetchone_rejects_cross_loop_call() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    # Seed a buffered result set so the call would silently succeed without the loop-binding check.
    conn._ensure_locks()
    cur._description = (("col", None, None, None, None, None, None),)
    cur._rows = [(1,), (2,)]
    cur._row_index = 0

    errors = _invoke_on_fresh_loop_in_thread(lambda: cur.fetchone())
    assert errors, "expected a ProgrammingError from the other loop's call"
    assert isinstance(errors[0], ProgrammingError)


async def test_fetchmany_rejects_cross_loop_call() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()
    cur._description = (("col", None, None, None, None, None, None),)
    cur._rows = [(1,), (2,)]
    cur._row_index = 0

    errors = _invoke_on_fresh_loop_in_thread(lambda: cur.fetchmany(2))
    assert errors, "expected a ProgrammingError from the other loop's call"
    assert isinstance(errors[0], ProgrammingError)


async def test_fetchall_rejects_cross_loop_call() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()
    cur._description = (("col", None, None, None, None, None, None),)
    cur._rows = [(1,), (2,)]
    cur._row_index = 0

    errors = _invoke_on_fresh_loop_in_thread(lambda: cur.fetchall())
    assert errors, "expected a ProgrammingError from the other loop's call"
    assert isinstance(errors[0], ProgrammingError)


async def test_aiter_rejects_cross_loop_call() -> None:
    """``__aiter__`` is sync; the binding check fires at ``async for``, not in ``__anext__``."""
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()

    errors: list[BaseException] = []

    def _runner() -> None:
        async def _invoke() -> None:
            try:
                cur.__aiter__()  # sync; call directly to isolate the loop-binding behaviour
            except BaseException as e:  # noqa: BLE001
                errors.append(e)

        asyncio.run(_invoke())

    t = threading.Thread(target=_runner)
    t.start()
    t.join()
    assert errors, "expected a ProgrammingError from __aiter__ on the other loop"
    assert isinstance(errors[0], ProgrammingError)


async def test_fetch_methods_accept_same_loop_call() -> None:
    """The binding check must NOT reject a call from the same loop."""
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()
    cur._description = (("col", None, None, None, None, None, None),)
    cur._rows = [(1,), (2,), (3,)]
    cur._row_index = 0

    row = await cur.fetchone()
    assert row == (1,)
    rows = await cur.fetchmany(1)
    assert rows == [(2,)]
    rows = await cur.fetchall()
    assert rows == [(3,)]


async def test_aiter_accepts_same_loop_call() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()
    cur._description = (("col", None, None, None, None, None, None),)
    cur._rows = [(1,), (2,)]
    cur._row_index = 0

    collected = []
    async for row in cur:
        collected.append(row)
    assert collected == [(1,), (2,)]
