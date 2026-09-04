"""``_check_loop_only`` checks pid before loop (closed -> pid -> loop), so a forked child
gets the post-fork ``InterfaceError`` that reconnect middleware catches, not the
loop-affinity ``ProgrammingError``. Pid is the stronger condition: an inherited
``_loop_ref`` weakref may still resolve in the child's address space."""

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
    """A pid-mismatch caller sees ``InterfaceError``, not ``ProgrammingError``."""
    import os

    loop = asyncio.new_event_loop()
    try:
        # Fake creator pid != current pid fires the inequality without actually forking.
        aconn = _build_loop_bound_async_connection(loop, pid=os.getpid() + 1)

        async def reading_call() -> None:
            with pytest.raises(InterfaceError, match="after fork"):
                aconn._check_loop_only()

        loop.run_until_complete(reading_call())
    finally:
        loop.close()


def test_check_loop_only_without_creator_pid_does_not_raise_attribute_error() -> None:
    """A missing ``_creator_pid`` (from ``__new__`` fixtures) must be tolerated as "no pid
    stamp yet", not raise AttributeError."""
    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._loop_ref = None
    aconn._async_conn = None
    # No _creator_pid set on purpose.
    aconn._check_loop_only()
