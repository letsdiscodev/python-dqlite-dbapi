"""Pin: ``AsyncConnection`` and ``force_close_transport`` reject or
short-circuit after ``os.fork`` — calling ``writer.close()`` on the
inherited socket would send FIN on the parent's connection."""

from __future__ import annotations

import asyncio
import os
from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


async def test_async_connection_used_after_fork_raises_interface_error() -> None:
    conn = AsyncConnection("127.0.0.1:9999")
    fake_parent_pid = conn._creator_pid + 1
    conn._creator_pid = fake_parent_pid

    with (
        patch("dqliteclient.connection.os.getpid", return_value=fake_parent_pid + 1),
        pytest.raises(InterfaceError, match="fork"),
    ):
        conn._ensure_locks()


def test_force_close_transport_after_fork_short_circuits() -> None:
    """In the child, must drop the reference without calling
    ``writer.close()`` (which would FIN the parent's connection)."""
    conn = AsyncConnection("127.0.0.1:9999")
    inner = MagicMock()
    inner._protocol = MagicMock()
    writer = MagicMock()
    writer.close = MagicMock()
    inner._protocol._writer = writer
    conn._async_conn = inner

    fake_parent_pid = conn._creator_pid + 1
    conn._creator_pid = fake_parent_pid

    with patch("dqliteclient.connection.os.getpid", return_value=fake_parent_pid + 1):
        conn.force_close_transport()

    writer.close.assert_not_called()
    assert conn._async_conn is None


async def test_async_connection_close_after_fork_short_circuits() -> None:
    """In the child, ``close()`` must not enter the ``async with
    op_lock`` arm — the lock is bound to the parent's loop and the
    writer is the inherited FD. Flip local state and drop refs."""
    conn = AsyncConnection("127.0.0.1:9999")
    inner = MagicMock()
    # Raise on close so a regression through the async-teardown path fails loudly.
    inner.close = MagicMock(side_effect=AssertionError("must not call inner.close in fork branch"))
    conn._async_conn = inner
    conn._op_lock = asyncio.Lock()
    conn._connect_lock = asyncio.Lock()

    fake_parent_pid = conn._creator_pid + 1
    conn._creator_pid = fake_parent_pid

    with patch("dqliteclient.connection.os.getpid", return_value=fake_parent_pid + 1):
        await conn.close()

    inner.close.assert_not_called()
    assert conn._closed is True
    assert conn._async_conn is None
    assert conn._connect_lock is None
    assert conn._op_lock is None
    assert conn._loop_ref is None


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_async_connection_force_close_transport_actual_fork() -> None:
    """End-to-end fork: child calls force_close_transport; parent
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
    parent_close_calls.assert_not_called()
