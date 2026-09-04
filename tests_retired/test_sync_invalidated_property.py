"""Sync ``Connection.invalidated`` reports True when the inner connection is invalidated;
``Connection.closed`` ORs invalidated state so reconnect heuristics on ``conn.closed`` work."""

from __future__ import annotations

from unittest.mock import MagicMock

from dqlitedbapi import Connection


def _make_conn(*, closed: bool = False, inner: object | None = None) -> Connection:
    conn = Connection.__new__(Connection)
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
    """Cancel-mid-execute / leader-flip clears _protocol; the wrapper sees this as invalidated."""
    inner = MagicMock()
    inner._protocol = None
    conn = _make_conn(inner=inner)
    assert conn.invalidated is True
    assert conn.closed is True


def test_invalidated_false_after_explicit_close() -> None:
    """After explicit close, ``invalidated`` is False — ``closed`` is the canonical signal."""
    inner = MagicMock()
    inner._protocol = None
    conn = _make_conn(closed=True, inner=inner)
    assert conn.invalidated is False
    assert conn.closed is True
