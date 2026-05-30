"""__exit__/__aexit__ short-circuit on ``_closed is True`` (not just ``_async_conn is None``)
so a foreign-thread force_close_transport race doesn't make commit raise a closed-state error."""

from __future__ import annotations

import os
import threading
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.connection import Connection


def _bare_sync_connection() -> Any:
    """Construct a Connection without dialing, mimicking a mid-with connected shape."""
    conn = cast(Any, Connection.__new__(Connection))
    conn._closed = False
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn._cursors = []
    conn._async_conn = MagicMock()  # mid-with, was connected
    return conn


def test_sync_exit_short_circuits_when_closed_mid_with() -> None:
    """Foreign-thread close (sets _closed and _async_conn=None) mid-with short-circuits cleanly."""
    conn = _bare_sync_connection()

    conn._closed = True
    conn._async_conn = None

    assert conn.__exit__(None, None, None) is None


def test_sync_exit_short_circuits_when_force_close_set_closed_but_async_conn_still_alive() -> None:
    """_closed alone short-circuits even if the _async_conn null-write hasn't landed yet."""
    conn = _bare_sync_connection()

    conn._closed = True

    assert conn.__exit__(None, None, None) is None


def test_sync_exit_with_body_exception_short_circuits_when_closed() -> None:
    """Body raised and foreign thread closed: __exit__ short-circuits, body exception propagates."""
    conn = _bare_sync_connection()
    conn._closed = True
    conn._async_conn = None

    body_exc = ValueError("body sentinel")
    # Falsy return => the body exception propagates instead of being suppressed.
    assert conn.__exit__(type(body_exc), body_exc, None) is None


@pytest.mark.asyncio
async def test_async_exit_short_circuits_when_closed_mid_with() -> None:
    """Async sibling: same short-circuit when force_close set _closed mid-async-with."""
    from dqlitedbapi.aio.connection import AsyncConnection

    aconn = cast(Any, AsyncConnection.__new__(AsyncConnection))
    aconn._closed = False
    aconn._creator_pid = os.getpid()
    aconn._loop_ref = None
    aconn._async_conn = MagicMock()
    aconn._closed = True
    aconn._async_conn = None

    assert await aconn.__aexit__(None, None, None) is None
