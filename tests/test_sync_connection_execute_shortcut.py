"""Sync ``Connection.execute`` exists (parity with sqlite3 / the async sibling): SA's
connect-event idiom calls ``dbapi_connection.execute("PRAGMA ...")``."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import dqlitedbapi
from dqlitedbapi.connection import Connection


def test_sync_connection_has_execute_method() -> None:
    assert hasattr(dqlitedbapi.Connection, "execute"), (
        "dqlitedbapi.Connection should expose execute() — stdlib "
        "sqlite3.Connection.execute, AsyncAdaptedConnection.execute "
        "and SA's reference connector all have it."
    )


def test_sync_connection_execute_returns_cursor_and_calls_through() -> None:
    """execute() opens a cursor, forwards to ``cur.execute(...)``, and returns the cursor."""
    conn = Connection("localhost:9001", timeout=1.0)
    fake_cur = MagicMock()
    conn.cursor = MagicMock(return_value=fake_cur)

    result = conn.execute("SELECT 1")

    assert result is fake_cur
    fake_cur.execute.assert_called_once_with("SELECT 1")
    fake_cur.close.assert_not_called()


def test_sync_connection_execute_passes_parameters() -> None:
    conn = Connection("localhost:9001", timeout=1.0)
    fake_cur = MagicMock()
    conn.cursor = MagicMock(return_value=fake_cur)

    conn.execute("SELECT ?", [1])

    fake_cur.execute.assert_called_once_with("SELECT ?", [1])


def test_sync_connection_execute_closes_cursor_on_synchronous_raise() -> None:
    """A synchronous ``cur.execute(...)`` failure must close the cursor before re-raising."""
    conn = Connection("localhost:9001", timeout=1.0)
    fake_cur = MagicMock()
    fake_cur.execute.side_effect = RuntimeError("simulated execute failure")
    conn.cursor = MagicMock(return_value=fake_cur)

    with pytest.raises(RuntimeError, match="simulated execute failure"):
        conn.execute("SELECT 1")

    fake_cur.close.assert_called_once_with()
