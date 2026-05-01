"""Pin: ``AsyncConnection.in_transaction`` raises ``ProgrammingError``
on foreign-loop access, symmetric with the sync sibling's
``_check_thread()`` raise on cross-thread access.

Without the loop-binding check, the property silently read through to
the underlying client-layer ``DqliteConnection.in_transaction``,
returning a value computed against loop-A primitives from loop B —
the very same loop-affinity violation that ``commit`` / ``rollback`` /
``cursor`` raise on. A SA-engine cleanup branch that probes
``if dbapi_conn.in_transaction: dbapi_conn.rollback()`` therefore
returned True (silently) and then raised ProgrammingError on the
rollback, defeating the property's documented "cleanup discriminator"
purpose.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import ProgrammingError


def _make_loop_bound_async_connection(loop: asyncio.AbstractEventLoop) -> AsyncConnection:
    """Build a minimal AsyncConnection with ``_loop_ref`` already
    bound to ``loop`` so a foreign-loop read of ``in_transaction``
    triggers the ``_check_loop_only`` raise."""
    import weakref

    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._loop_ref = weakref.ref(loop)
    aconn._async_conn = MagicMock()
    aconn._async_conn.in_transaction = True
    return aconn


def test_in_transaction_raises_on_foreign_loop_read() -> None:
    """A read from a different event loop must raise ProgrammingError
    matching the sync sibling's cross-thread behaviour."""
    bound_loop = asyncio.new_event_loop()
    foreign_loop = asyncio.new_event_loop()
    try:
        aconn = _make_loop_bound_async_connection(bound_loop)

        async def read_from_foreign() -> None:
            # Inside foreign_loop now; bound to bound_loop.
            with pytest.raises(ProgrammingError):
                _ = aconn.in_transaction

        foreign_loop.run_until_complete(read_from_foreign())
    finally:
        bound_loop.close()
        foreign_loop.close()


def test_in_transaction_does_not_lazy_bind_loop_on_first_read() -> None:
    """``_check_loop_only`` is the non-binding variant: a fresh
    AsyncConnection (``_loop_ref is None``) reading ``in_transaction``
    must NOT bind the loop. Pin so a future refactor that swaps to
    ``_check_loop_binding`` (or ``_ensure_locks``) doesn't re-introduce
    the lazy-bind footgun.
    """
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._loop_ref = None  # not yet bound
    aconn._async_conn = None
    loop = asyncio.new_event_loop()
    try:

        async def read_unbound() -> None:
            _ = aconn.in_transaction
            assert aconn._loop_ref is None, "in_transaction read must not lazy-bind the loop"

        loop.run_until_complete(read_unbound())
    finally:
        loop.close()
