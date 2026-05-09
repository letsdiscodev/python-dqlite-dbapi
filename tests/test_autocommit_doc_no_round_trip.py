"""Pin: ``Connection.autocommit`` getter docstring acknowledges that
the setter accepts ``-1`` but the getter never returns it (no round
trip). Behaviour pin: the getter is fixed-True regardless of setter.
"""

from __future__ import annotations

import sqlite3

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


def _prop_doc(cls: type, name: str) -> str:
    desc = cls.__dict__[name]
    assert isinstance(desc, property)
    doc = desc.__doc__
    assert doc is not None
    return doc


def test_sync_autocommit_doc_calls_out_no_round_trip() -> None:
    doc = _prop_doc(Connection, "autocommit")
    assert "Always returns" in doc
    assert "LEGACY_TRANSACTION_CONTROL" in doc


def test_async_autocommit_doc_calls_out_no_round_trip() -> None:
    doc = _prop_doc(AsyncConnection, "autocommit")
    assert "Always returns" in doc
    assert "LEGACY_TRANSACTION_CONTROL" in doc


def test_sync_autocommit_setter_minus_one_does_not_round_trip() -> None:
    """Behaviour regression guard: setter accepts -1 but getter is True."""
    conn = Connection("127.0.0.1:9999")
    try:
        conn.autocommit = sqlite3.LEGACY_TRANSACTION_CONTROL
        assert conn.autocommit is True
    finally:
        conn._closed = True


def test_sync_autocommit_setter_true_round_trips_as_true() -> None:
    conn = Connection("127.0.0.1:9999")
    try:
        conn.autocommit = True
        assert conn.autocommit is True
    finally:
        conn._closed = True
