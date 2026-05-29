"""Pin: ``AsyncConnection.in_transaction`` raises ``ProgrammingError`` on
foreign-loop access, symmetric with the sync sibling's ``_check_thread()``.
Without it the property silently read loop-A primitives from loop B."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import ProgrammingError


def _make_loop_bound_async_connection(loop: asyncio.AbstractEventLoop) -> AsyncConnection:
    """AsyncConnection with ``_loop_ref`` bound to ``loop``."""
    import weakref

    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._loop_ref = weakref.ref(loop)
    aconn._async_conn = MagicMock()
    aconn._async_conn.in_transaction = True
    return aconn


def test_in_transaction_raises_on_foreign_loop_read() -> None:
    """A read from a different event loop must raise ProgrammingError."""
    bound_loop = asyncio.new_event_loop()
    foreign_loop = asyncio.new_event_loop()
    try:
        aconn = _make_loop_bound_async_connection(bound_loop)

        async def read_from_foreign() -> None:
            with pytest.raises(ProgrammingError):
                _ = aconn.in_transaction

        foreign_loop.run_until_complete(read_from_foreign())
    finally:
        bound_loop.close()
        foreign_loop.close()


def test_in_transaction_does_not_lazy_bind_loop_on_first_read() -> None:
    """Reading ``in_transaction`` on a fresh connection must NOT lazy-bind the
    loop (``_check_loop_only`` is the non-binding variant)."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._loop_ref = None
    aconn._async_conn = None
    loop = asyncio.new_event_loop()
    try:

        async def read_unbound() -> None:
            _ = aconn.in_transaction
            assert aconn._loop_ref is None, "in_transaction read must not lazy-bind the loop"

        loop.run_until_complete(read_unbound())
    finally:
        loop.close()
