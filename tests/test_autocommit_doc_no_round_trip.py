"""Pin: ``Connection.autocommit`` getter/setter round-trip. Both True and -1 no-op
the wire (dqlite is fixed-mode), but the property reflects the caller's last input."""

from __future__ import annotations

import sqlite3

from dqlitedbapi.connection import Connection


def test_sync_autocommit_setter_minus_one_round_trips() -> None:
    conn = Connection("127.0.0.1:9999")
    try:
        conn.autocommit = sqlite3.LEGACY_TRANSACTION_CONTROL
        assert conn.autocommit == sqlite3.LEGACY_TRANSACTION_CONTROL
        assert conn.autocommit == -1
    finally:
        conn.close()


def test_sync_autocommit_setter_true_round_trips_as_true() -> None:
    conn = Connection("127.0.0.1:9999")
    try:
        conn.autocommit = True
        assert conn.autocommit is True
    finally:
        conn.close()


def test_sync_autocommit_default_is_true_before_any_set() -> None:
    """Fresh connection (setter never called) returns the documented True default."""
    conn = Connection("127.0.0.1:9999")
    try:
        assert conn.autocommit is True
    finally:
        conn.close()
