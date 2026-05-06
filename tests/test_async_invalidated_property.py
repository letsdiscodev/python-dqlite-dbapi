"""Pin: ``AsyncConnection.invalidated`` reports True when the inner
client connection has been invalidated; ``AsyncConnection.closed``
ORs invalidated state so cross-driver code branching on
``conn.closed`` to drive reconnect heuristics works correctly.

Without this, an invalidated connection still reported
``closed == False`` while every operation surfaced
``InterfaceError("Not connected")`` — a parity gap with asyncpg's
``is_closed()`` and psycopg's ``connection.broken``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dqlitedbapi.aio import AsyncConnection


def _make_conn(*, closed: bool = False, inner: object | None = None) -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = closed
    conn._async_conn = inner  # type: ignore[assignment]
    return conn


def test_invalidated_false_when_not_connected() -> None:
    conn = _make_conn()
    assert conn.invalidated is False
    assert conn.closed is False


def test_invalidated_false_when_alive() -> None:
    inner = MagicMock()
    inner._protocol = object()  # alive
    conn = _make_conn(inner=inner)
    assert conn.invalidated is False
    assert conn.closed is False


def test_invalidated_true_when_inner_protocol_is_none() -> None:
    """Cancel-mid-execute / leader-flip clears _protocol on the inner
    client connection. The dbapi wrapper sees this as invalidated."""
    inner = MagicMock()
    inner._protocol = None
    conn = _make_conn(inner=inner)
    assert conn.invalidated is True
    # closed ORs invalidated → True.
    assert conn.closed is True


def test_invalidated_false_after_explicit_close() -> None:
    """Once the connection has been explicitly closed, ``invalidated``
    returns False — ``closed`` is the canonical signal then."""
    inner = MagicMock()
    inner._protocol = None
    conn = _make_conn(closed=True, inner=inner)
    assert conn.invalidated is False
    assert conn.closed is True
