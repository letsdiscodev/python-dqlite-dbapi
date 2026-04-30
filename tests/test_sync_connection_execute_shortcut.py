"""Pin: sync ``Connection.execute(operation, parameters=None)``
exists and matches the discipline of the async sibling.

Stdlib ``sqlite3.Connection.execute`` is the parity target;
the async ``AsyncAdaptedConnection.execute`` was added in
cycle 21 for the same reason — SA's ``connect``-event listener
idiom calls ``dbapi_connection.execute("PRAGMA ...")`` and
the missing method surfaces as a confusing ``AttributeError``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import dqlitedbapi
from dqlitedbapi.connection import Connection


def test_sync_connection_has_execute_method() -> None:
    assert hasattr(dqlitedbapi.Connection, "execute"), (
        "dqlitedbapi.Connection should expose execute() — stdlib "
        "sqlite3.Connection.execute, AsyncAdaptedConnection.execute "
        "(cycle 21) and SA's reference connector all have it."
    )


def test_sync_connection_execute_returns_cursor_and_calls_through() -> None:
    """Stub the cursor so we can assert execute() opens it,
    forwards to ``cur.execute(...)``, and returns the cursor.
    Mirrors the cycle-21 async-side parity test."""
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
    """Cleanup-on-raise discipline (cycle-22 ISSUE-1119 sibling):
    a synchronous failure of ``cur.execute(...)`` must close the
    freshly-opened cursor before re-raising, so the caller's
    exception path does not leak an unowned cursor."""
    conn = Connection("localhost:9001", timeout=1.0)
    fake_cur = MagicMock()
    fake_cur.execute.side_effect = RuntimeError("simulated execute failure")
    conn.cursor = MagicMock(return_value=fake_cur)

    with pytest.raises(RuntimeError, match="simulated execute failure"):
        conn.execute("SELECT 1")

    fake_cur.close.assert_called_once_with()
