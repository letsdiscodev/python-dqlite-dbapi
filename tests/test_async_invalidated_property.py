"""Pin: ``AsyncConnection.invalidated`` reports True when the inner client
connection is invalidated, and ``closed`` ORs invalidated state (parity with
asyncpg ``is_closed()`` / psycopg ``connection.broken``)."""

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
    inner._protocol = object()
    conn = _make_conn(inner=inner)
    assert conn.invalidated is False
    assert conn.closed is False


def test_invalidated_true_when_inner_protocol_is_none() -> None:
    """Cancel-mid-execute / leader-flip clears _protocol; wrapper sees invalidated."""
    inner = MagicMock()
    inner._protocol = None
    conn = _make_conn(inner=inner)
    assert conn.invalidated is True
    assert conn.closed is True


def test_invalidated_false_after_explicit_close() -> None:
    """After explicit close, ``invalidated`` is False; ``closed`` is canonical."""
    inner = MagicMock()
    inner._protocol = None
    conn = _make_conn(closed=True, inner=inner)
    assert conn.invalidated is False
    assert conn.closed is True
