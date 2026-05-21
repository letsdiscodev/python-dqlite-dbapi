"""Pin: ``Connection.autocommit`` getter / setter round-trip.

Updated from the previous "always returns True" pin: the driver now
stores the setter input on ``self._autocommit_value`` so the stdlib
3.12+ ``conn.autocommit = sqlite3.LEGACY_TRANSACTION_CONTROL; assert
conn.autocommit == sqlite3.LEGACY_TRANSACTION_CONTROL`` idiom round-
trips on this driver. Both ``True`` and ``-1`` no-op the wire layer
(dqlite is fixed-mode autocommit) but the property reflects the
caller's last input — the module exports
``LEGACY_TRANSACTION_CONTROL = -1`` precisely so the stdlib idiom
works.
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


def test_sync_autocommit_doc_calls_out_setter_getter_round_trip() -> None:
    doc = _prop_doc(Connection, "autocommit")
    assert "round-trip" in doc.lower()
    assert "LEGACY_TRANSACTION_CONTROL" in doc


def test_async_autocommit_doc_calls_out_setter_getter_round_trip() -> None:
    doc = _prop_doc(AsyncConnection, "autocommit")
    assert "round-trip" in doc.lower()
    assert "LEGACY_TRANSACTION_CONTROL" in doc


def test_sync_autocommit_setter_minus_one_round_trips() -> None:
    """Behaviour pin: the stdlib idiom round-trips on this driver."""
    conn = Connection("127.0.0.1:9999")
    try:
        conn.autocommit = sqlite3.LEGACY_TRANSACTION_CONTROL
        assert conn.autocommit == sqlite3.LEGACY_TRANSACTION_CONTROL
        assert conn.autocommit == -1
    finally:
        conn._closed = True


def test_sync_autocommit_setter_true_round_trips_as_true() -> None:
    conn = Connection("127.0.0.1:9999")
    try:
        conn.autocommit = True
        assert conn.autocommit is True
    finally:
        conn._closed = True


def test_sync_autocommit_default_is_true_before_any_set() -> None:
    """Fresh connection (setter never called) returns ``True``
    — the documented default."""
    conn = Connection("127.0.0.1:9999")
    try:
        assert conn.autocommit is True
    finally:
        conn._closed = True
