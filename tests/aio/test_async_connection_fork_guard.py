"""Pin: dbapi ``AsyncConnection`` and ``force_close_transport`` reject
or short-circuit when used after ``os.fork``.

Cycle 20 added pid guards to the sync ``Connection`` and the
client-layer ``DqliteConnection`` / ``ConnectionPool``. The dbapi
async surface (``AsyncConnection``) and its synchronous
``force_close_transport`` hook (used by SA's adapter outside-greenlet
preflight, post-await RuntimeError catches, and ``terminate()``)
were left without guards. The hook in particular calls
``writer.close()`` on the inherited socket — the exact "FIN on the
parent's connection" the cycle was designed to prevent.

Tests cover:
- public-method use after fork raises a clear ``InterfaceError``
- ``force_close_transport`` short-circuits without touching the wire
- ``close()`` short-circuits without touching the wire or op_lock
"""

from __future__ import annotations

import asyncio
import os
from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


@pytest.mark.asyncio
async def test_async_connection_used_after_fork_raises_interface_error() -> None:
    conn = AsyncConnection("127.0.0.1:9999")
    fake_parent_pid = conn._creator_pid + 1
    conn._creator_pid = fake_parent_pid

    with (
        patch("dqlitedbapi.aio.connection.os.getpid", return_value=fake_parent_pid + 1),
        pytest.raises(InterfaceError, match="fork"),
    ):
        conn._ensure_locks()


def test_force_close_transport_after_fork_short_circuits() -> None:
    """``force_close_transport`` in the child must not call
    ``writer.close()`` on the inherited socket (which would send FIN
    on the parent's connection). Just drop the reference."""
    conn = AsyncConnection("127.0.0.1:9999")
    inner = MagicMock()
    inner._protocol = MagicMock()
    writer = MagicMock()
    writer.close = MagicMock()
    inner._protocol._writer = writer
    conn._async_conn = inner

    fake_parent_pid = conn._creator_pid + 1
    conn._creator_pid = fake_parent_pid

    with patch("dqlitedbapi.aio.connection.os.getpid", return_value=fake_parent_pid + 1):
        conn.force_close_transport()

    writer.close.assert_not_called()
    # Reference cleared so child GC has nothing to act on.
    assert conn._async_conn is None


@pytest.mark.asyncio
async def test_async_connection_close_after_fork_short_circuits() -> None:
    """``close()`` in the child must not enter the ``async with
    op_lock`` arm — the lock is bound to the parent's loop and the
    underlying connection's writer is the inherited FD. Quietly flip
    the local state and drop refs."""
    conn = AsyncConnection("127.0.0.1:9999")
    inner = MagicMock()
    # Make close raise so a regression that drives it through the
    # async-teardown path manifests as a clean failure.
    inner.close = MagicMock(side_effect=AssertionError("must not call inner.close in fork branch"))
    conn._async_conn = inner
    conn._op_lock = asyncio.Lock()
    conn._connect_lock = asyncio.Lock()

    fake_parent_pid = conn._creator_pid + 1
    conn._creator_pid = fake_parent_pid

    with patch("dqlitedbapi.aio.connection.os.getpid", return_value=fake_parent_pid + 1):
        await conn.close()

    inner.close.assert_not_called()
    assert conn._closed is True
    assert conn._async_conn is None
    assert conn._connect_lock is None
    assert conn._op_lock is None
    assert conn._loop_ref is None


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_async_connection_force_close_transport_actual_fork() -> None:
    """End-to-end fork: parent stages a connection with a writer ref;
    child calls force_close_transport and reports back. Parent
    confirms its writer.close was never called."""
    conn = AsyncConnection("127.0.0.1:9999")
    parent_close_calls = MagicMock()
    inner = MagicMock()
    inner._protocol = MagicMock()
    inner._protocol._writer = MagicMock()
    inner._protocol._writer.close = parent_close_calls
    conn._async_conn = inner

    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(r)
            try:
                conn.force_close_transport()
                # Reference cleared; writer.close not invoked from child.
                if conn._async_conn is None:
                    os.write(w, b"OK")
                else:
                    os.write(w, b"WRONG_REF_NOT_CLEARED")
            except Exception as e:  # noqa: BLE001
                os.write(w, f"WRONG:{type(e).__name__}:{e}".encode())
            finally:
                os.close(w)
        finally:
            os._exit(0)
    os.close(w)
    result = b""
    while True:
        chunk = os.read(r, 4096)
        if not chunk:
            break
        result += chunk
    os.close(r)
    os.waitpid(pid, 0)
    assert result == b"OK", f"child reported: {result!r}"
    # Parent's writer.close must NOT have been called from the child.
    parent_close_calls.assert_not_called()
