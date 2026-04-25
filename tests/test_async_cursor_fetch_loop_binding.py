"""``AsyncCursor`` fetch / ``__aiter__`` calls now route through
``_ensure_locks()`` so a call from a different event loop surfaces the
loop-binding mismatch up front.

The existing pattern in ``setinputsizes`` / ``setoutputsize`` raises a
clean ``ProgrammingError("AsyncConnection is bound to a different
event loop. ...")`` synchronously. The fetch path historically did
not — a wrong-loop ``fetchone`` simply read pre-buffered rows and
succeeded silently, hiding the misuse until the next awaited
operation that DID acquire a loop primitive.

Pin the up-front check across all cursor accessors so the diagnostic
shape is consistent and misuses surface at the call site.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from dqlitedbapi import ProgrammingError
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor


def _invoke_on_fresh_loop_in_thread(coro_factory) -> list[BaseException]:
    """Run ``await coro_factory()`` on a fresh ``asyncio.run`` in a
    background thread; return any caught exception."""
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


@pytest.mark.asyncio
async def test_fetchone_rejects_cross_loop_call() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    # Prime binding on the outer loop and seed a fake result set so
    # the call would silently succeed if not for the loop-binding
    # check (the rows are already buffered).
    conn._ensure_locks()
    cur._description = (("col", None, None, None, None, None, None),)
    cur._rows = [(1,), (2,)]
    cur._row_index = 0

    errors = _invoke_on_fresh_loop_in_thread(lambda: cur.fetchone())
    assert errors, "expected a ProgrammingError from the other loop's call"
    assert isinstance(errors[0], ProgrammingError)


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_aiter_rejects_cross_loop_call() -> None:
    """``__aiter__`` is synchronous; the loop-binding check fires at
    the ``async for cursor:`` site rather than one await deeper in
    ``__anext__``.
    """
    conn = AsyncConnection("127.0.0.1:9001")
    cur = AsyncCursor(conn)
    conn._ensure_locks()

    errors: list[BaseException] = []

    def _runner() -> None:
        async def _invoke() -> None:
            try:
                # __aiter__ is sync — call directly to isolate the
                # loop-binding behaviour.
                cur.__aiter__()
            except BaseException as e:  # noqa: BLE001
                errors.append(e)

        asyncio.run(_invoke())

    t = threading.Thread(target=_runner)
    t.start()
    t.join()
    assert errors, "expected a ProgrammingError from __aiter__ on the other loop"
    assert isinstance(errors[0], ProgrammingError)


@pytest.mark.asyncio
async def test_fetch_methods_accept_same_loop_call() -> None:
    """Sanity: the binding check must NOT reject a call from the same
    loop the connection was first used on."""
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


@pytest.mark.asyncio
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
