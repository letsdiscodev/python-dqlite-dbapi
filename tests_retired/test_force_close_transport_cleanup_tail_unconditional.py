"""``_force_close_transport`` runs the cleanup tail (``_pending_drain`` reap and
``_async_conn = None``) unconditionally, even when ``_protocol`` or ``_writer`` is None
(post-``_invalidate`` or partially-constructed inner) — the old early-return leaked both."""

from __future__ import annotations

from unittest.mock import MagicMock

from dqlitedbapi.aio.connection import AsyncConnection


def _make_async_connection_with_inner(
    *, proto_present: bool, writer_present: bool
) -> tuple[AsyncConnection, MagicMock, MagicMock]:
    """``AsyncConnection`` with a configurable ``_protocol``/``_writer`` shape on its inner."""
    import os

    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._creator_pid = os.getpid()
    aconn._loop_ref = None
    aconn._closed_flag = [False]

    inner = MagicMock()
    pending = MagicMock()
    pending.done.return_value = False
    pending.cancel = MagicMock()
    inner._pending_drain = pending

    if proto_present:
        proto = MagicMock()
        if writer_present:
            writer = MagicMock()
            writer.close = MagicMock()
            proto._writer = writer
        else:
            proto._writer = None
        inner._protocol = proto
    else:
        inner._protocol = None

    aconn._async_conn = inner
    return aconn, inner, pending


def test_cleanup_tail_runs_when_protocol_is_none() -> None:
    """``_protocol`` cleared (post-``_invalidate``): reap and null-out still run."""
    aconn, inner, pending = _make_async_connection_with_inner(
        proto_present=False, writer_present=False
    )
    aconn.force_close_transport()
    pending.cancel.assert_called_once()
    assert aconn._async_conn is None


def test_cleanup_tail_runs_when_writer_is_none() -> None:
    """Partially-constructed inner: ``_writer`` is None on the
    protocol. Cleanup tail still runs."""
    aconn, inner, pending = _make_async_connection_with_inner(
        proto_present=True, writer_present=False
    )
    aconn.force_close_transport()
    pending.cancel.assert_called_once()
    assert aconn._async_conn is None


def test_cleanup_tail_runs_on_normal_path() -> None:
    """Positive control: protocol + writer both present.
    writer.close() is called, then the cleanup tail runs."""
    aconn, inner, pending = _make_async_connection_with_inner(
        proto_present=True, writer_present=True
    )
    aconn.force_close_transport()
    inner._protocol._writer.close.assert_called_once()
    pending.cancel.assert_called_once()
    assert aconn._async_conn is None


def test_pending_drain_already_done_no_cancel() -> None:
    """If the pending drain task already completed, do not redundantly
    cancel it; the null-out still runs."""
    aconn, inner, pending = _make_async_connection_with_inner(
        proto_present=True, writer_present=True
    )
    pending.done.return_value = True
    aconn.force_close_transport()
    pending.cancel.assert_not_called()
    assert aconn._async_conn is None
