"""Pin: ``AsyncCursor.__aiter__`` returns ``self`` even on
a closed cursor / closed connection (PEP 234 / PEP 492
parity with the sync ``Cursor.__iter__`` design); the
closed-state diagnostic is deferred to the first
``__anext__``.

Also pins: ``aiter(cur) is cur`` invariance — sibling
classes (sync ``Cursor``, SA ``AsyncAdaptedCursor``) have
explicit pins; the dbapi ``AsyncCursor`` did not.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


@pytest.mark.asyncio
async def test_aiter_returns_self_invariance() -> None:
    """PEP 492: ``aiter(obj) is obj`` for an iterator that returns
    itself from ``__aiter__``. Pin against a future refactor that
    accidentally returns a wrapper / generator."""
    aconn = AsyncConnection("127.0.0.1:9999", database="x")
    cur = aconn.cursor()
    assert cur.__aiter__() is cur
    assert aiter(cur) is cur


@pytest.mark.asyncio
async def test_aiter_on_closed_cursor_does_not_raise() -> None:
    """``async for`` over a closed cursor must NOT raise at the
    ``__aiter__`` step. Sync ``Cursor.__iter__`` is bare
    ``return self``; the async sibling now mirrors. The closed-
    state diagnostic is deferred to ``__anext__`` (which delegates
    to ``fetchone`` and raises ``InterfaceError("Cursor is closed")``)."""
    aconn = AsyncConnection("127.0.0.1:9999", database="x")
    cur = aconn.cursor()
    await cur.close()

    # Must NOT raise. Pre-fix this raised InterfaceError because
    # ``_check_loop_binding`` ran a closed-state check before
    # returning self.
    same = cur.__aiter__()
    assert same is cur
