"""Pin: ``AsyncConnection._check_loop_only`` raises
``InterfaceError`` (the canonical post-fork diagnostic class) before
running the loop-binding check, mirroring the layering already used
by ``_check_loop_binding`` (closed → pid → loop).

Without this ordering, a forked child whose parent had bound the
loop_ref would either:

* Hit ``_check_loop_only``'s loop comparison first — raising
  ``ProgrammingError`` (the loop-affinity diagnostic). Cross-driver
  retry middleware catching ``InterfaceError`` to drive a reconnect
  (psycopg parity) does NOT catch ``ProgrammingError`` and would
  not re-establish the connection in the child.
* If the parent never exercised the cursor on a loop
  (``_loop_ref is None``), the silently-return arm would defer to
  the first wire call, but ``__aiter__`` itself returns ``self`` —
  the loop-binding fence is the only sync surface where the
  diagnostic class matters.

Pid is the strictly stronger condition than loop affinity (an
inherited ``_loop_ref`` weakref may still resolve in the child's
address space) so the pid check must fire first.
"""

from __future__ import annotations

import asyncio
import weakref

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


def _build_loop_bound_async_connection(
    loop: asyncio.AbstractEventLoop,
    pid: int,
) -> AsyncConnection:
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._loop_ref = weakref.ref(loop)
    aconn._async_conn = None
    aconn._creator_pid = pid
    return aconn


def test_check_loop_only_raises_interface_error_post_fork() -> None:
    """A forked-child caller (pid mismatch) sees ``InterfaceError``
    — the canonical post-fork diagnostic — not the
    ``ProgrammingError`` that a loop-affinity-first ordering would
    surface."""
    import os

    loop = asyncio.new_event_loop()
    try:
        # Simulate "this AsyncConnection was created in another pid"
        # by stamping a fake creator pid different from the current
        # process. The check uses ``get_current_pid()`` against the
        # stamped value, so the inequality fires immediately without
        # actually forking.
        aconn = _build_loop_bound_async_connection(loop, pid=os.getpid() + 1)

        async def reading_call() -> None:
            with pytest.raises(InterfaceError, match="after fork"):
                aconn._check_loop_only()

        loop.run_until_complete(reading_call())
    finally:
        loop.close()


def test_check_loop_only_without_creator_pid_does_not_raise_attribute_error() -> None:
    """Belt-and-suspenders against test fixtures that construct via
    ``__new__`` and forget to set ``_creator_pid``. Such fixtures
    exist in the suite (see ``test_async_in_transaction_loop_binding``
    et al). The guard must tolerate a missing attribute by treating
    it as "no pid stamp captured yet" — the legitimate
    fresh-from-``__init__`` invariant is enforced by ``__init__``
    itself."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._loop_ref = None
    aconn._async_conn = None
    # No ``_creator_pid`` attribute set on purpose — must not
    # AttributeError.
    aconn._check_loop_only()
