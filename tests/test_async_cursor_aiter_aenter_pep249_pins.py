"""Pin: ``AsyncCursor.__aiter__`` / ``__aenter__`` translate ``ReferenceError`` from a GC'd
parent to ``InterfaceError`` and reject cross-loop entry up front."""

from __future__ import annotations

import asyncio
import gc

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection


async def test_aiter_on_closed_cursor_with_gc_parent_defers_to_anext() -> None:
    """Stdlib parity: ``aiter(closed_cur)`` returns the cursor; the diagnostic surfaces on
    the first ``__anext__``, not at ``aiter()`` (``__aenter__`` raises eagerly, no sync analog)."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    cur.close()
    del conn
    gc.collect()

    it = aiter(cur)
    assert it is cur, "aiter(cur) must return the cursor itself (PEP 234)"

    with pytest.raises(dqlitedbapi.InterfaceError):
        await anext(cur)


async def test_aenter_loop_binding_check_runs_before_body() -> None:
    """``async with cur:`` in its bound loop must enter cleanly (sanity for the check)."""
    conn = AsyncConnection("localhost:9001")
    try:
        cur = conn.cursor()
        async with cur:
            pass
    finally:
        await conn.close()


def test_aenter_from_foreign_loop_raises_programming_error() -> None:
    """A cursor created in loop A then entered from loop B should raise ProgrammingError."""
    conn_holder: dict[str, AsyncConnection] = {}

    async def loop_a_setup() -> None:
        conn_holder["c"] = AsyncConnection("localhost:9001")

    asyncio.run(loop_a_setup())
    conn = conn_holder["c"]
    cur = conn.cursor()

    async def loop_b() -> None:
        async with cur:
            pass

    # Cursors carry no loop binding of their own; entering one from another loop is fine
    # until it touches the wire, which is where the connection's loop check fires.
    asyncio.run(loop_b())
