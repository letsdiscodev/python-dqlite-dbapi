"""``LEGACY_TRANSACTION_CONTROL == -1`` is exposed on both surfaces (stdlib parity)."""

from __future__ import annotations

import sqlite3

import dqlitedbapi
import dqlitedbapi.aio
from dqlitedbapi.connection import Connection


def test_sync_module_exposes_legacy_transaction_control() -> None:
    assert dqlitedbapi.LEGACY_TRANSACTION_CONTROL == -1
    assert dqlitedbapi.LEGACY_TRANSACTION_CONTROL == sqlite3.LEGACY_TRANSACTION_CONTROL


def test_async_module_exposes_legacy_transaction_control() -> None:
    assert dqlitedbapi.aio.LEGACY_TRANSACTION_CONTROL == -1
    assert dqlitedbapi.aio.LEGACY_TRANSACTION_CONTROL == sqlite3.LEGACY_TRANSACTION_CONTROL


def test_legacy_transaction_control_in_sync_all() -> None:
    assert "LEGACY_TRANSACTION_CONTROL" in dqlitedbapi.__all__


def test_legacy_transaction_control_in_async_all() -> None:
    assert "LEGACY_TRANSACTION_CONTROL" in dqlitedbapi.aio.__all__


def test_legacy_transaction_control_setter_round_trip() -> None:
    """The exposed constant is the value the autocommit setter accepts."""
    conn = Connection("127.0.0.1:9999")
    try:
        conn.autocommit = dqlitedbapi.LEGACY_TRANSACTION_CONTROL
    finally:
        conn._closed = True
