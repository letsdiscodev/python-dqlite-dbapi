"""Pin: in_transaction docstrings document the divergence from stdlib, and
closed connections return False rather than raising."""

from __future__ import annotations

from dqlitedbapi.connection import Connection


def test_sync_closed_connection_in_transaction_returns_false() -> None:
    """Behaviour regression guard: closed connection returns False."""
    conn = Connection("127.0.0.1:9999")
    conn._closed = True
    assert conn.in_transaction is False
