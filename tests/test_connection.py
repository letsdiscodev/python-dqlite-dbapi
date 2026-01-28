"""Tests for Connection class."""

import pytest

from dqlitedbapi import Connection, connect
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import InterfaceError


class TestConnection:
    def test_connect_function(self) -> None:
        conn = connect("localhost:9001", database="test", timeout=5.0)
        assert isinstance(conn, Connection)

    def test_cursor_returns_cursor(self) -> None:
        conn = Connection("localhost:9001")
        cursor = conn.cursor()
        assert isinstance(cursor, Cursor)

    def test_close_marks_connection_closed(self) -> None:
        conn = Connection("localhost:9001")
        conn.close()
        assert conn._closed

    def test_cursor_on_closed_connection_raises(self) -> None:
        conn = Connection("localhost:9001")
        conn.close()

        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn.cursor()

    def test_context_manager(self) -> None:
        with Connection("localhost:9001") as conn:
            assert isinstance(conn, Connection)
        assert conn._closed
