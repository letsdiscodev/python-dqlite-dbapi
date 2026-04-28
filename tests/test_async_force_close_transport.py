"""Pin: ``AsyncConnection.force_close_transport`` is a public,
synchronous, idempotent, never-raising last-resort cleanup hook.

The SA dialect's async adapter calls this from its non-greenlet
finalize path (GC sweep with no event loop). Walking the
underlying client connection's private ``_protocol._writer``
chain from outside this package broke silently when the chain
shape changed; this hook is the single supported access boundary.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dqlitedbapi.aio.connection import AsyncConnection


def test_force_close_transport_calls_writer_close() -> None:
    """The hook walks _async_conn → _protocol → _writer and calls
    writer.close()."""
    conn = AsyncConnection("localhost:9001", database="x")
    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    conn._async_conn = inner

    conn.force_close_transport()

    writer.close.assert_called_once_with()


def test_force_close_transport_is_idempotent() -> None:
    """Multiple invocations are safe; the writer's close() may be
    called repeatedly."""
    conn = AsyncConnection("localhost:9001", database="x")
    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    conn._async_conn = inner

    conn.force_close_transport()
    conn.force_close_transport()
    conn.force_close_transport()

    assert writer.close.call_count == 3


def test_force_close_transport_handles_missing_async_conn() -> None:
    """A connection that was never opened (or already closed and
    nulled) absorbs the call without raising."""
    conn = AsyncConnection("localhost:9001", database="x")
    assert conn._async_conn is None  # never connected
    conn.force_close_transport()  # must not raise


def test_force_close_transport_handles_missing_protocol() -> None:
    """An inner connection without ``_protocol`` (mid-construction
    or already torn down) absorbs the call."""
    conn = AsyncConnection("localhost:9001", database="x")
    inner = MagicMock(spec=[])  # no attributes
    conn._async_conn = inner
    conn.force_close_transport()  # must not raise


def test_force_close_transport_swallows_writer_close_exception() -> None:
    """``writer.close()`` raising must not propagate — last-resort
    cleanup must always finish."""
    conn = AsyncConnection("localhost:9001", database="x")
    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    writer.close.side_effect = OSError("transport already closed")
    proto._writer = writer
    inner._protocol = proto
    conn._async_conn = inner

    conn.force_close_transport()  # must not raise
    writer.close.assert_called_once_with()
