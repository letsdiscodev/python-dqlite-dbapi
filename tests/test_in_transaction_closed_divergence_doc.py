"""Pin: in_transaction docstrings document the divergence from stdlib, and
closed connections return False rather than raising."""

from __future__ import annotations

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


def _prop_doc(cls: type, name: str) -> str:
    """Pull the docstring off a property descriptor via cls.__dict__ so it
    stays a property (not a bound bool) and __doc__ is reachable."""
    desc = cls.__dict__[name]
    assert isinstance(desc, property)
    doc = desc.__doc__
    assert doc is not None
    return doc


def test_sync_in_transaction_doc_does_not_claim_mirrors_stdlib() -> None:
    doc = _prop_doc(Connection, "in_transaction")
    assert "Mirrors stdlib" not in doc


def test_async_in_transaction_doc_does_not_claim_mirrors_stdlib() -> None:
    doc = _prop_doc(AsyncConnection, "in_transaction")
    assert "Mirrors stdlib" not in doc


def test_sync_closed_connection_in_transaction_returns_false() -> None:
    """Behaviour regression guard: closed connection returns False."""
    conn = Connection("127.0.0.1:9999")
    conn._closed = True
    assert conn.in_transaction is False
