"""Connection.__enter__ eagerly connects (matching AsyncConnection), running close() on
failure since Python skips __exit__ when __enter__ raises."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import OperationalError


def test_enter_calls_connect_on_entry() -> None:
    conn = Connection("localhost:9001", timeout=0.1)
    with patch.object(Connection, "connect") as mock_connect, conn:
        pass
    mock_connect.assert_called_once()
    conn.close()


def test_enter_failure_propagates_without_returning_connection() -> None:
    conn = Connection("localhost:9001", timeout=0.1)
    with (
        patch.object(Connection, "connect", side_effect=OperationalError("boom")),
        pytest.raises(OperationalError, match="boom"),
        conn,
    ):
        raise AssertionError("body must not run on connect failure")
    assert conn._closed
