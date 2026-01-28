"""Tests for Cursor class."""

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import InterfaceError


class TestCursor:
    def test_description_initially_none(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        assert cursor.description is None

    def test_rowcount_initially_minus_one(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        assert cursor.rowcount == -1

    def test_arraysize_default(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        assert cursor.arraysize == 1

    def test_arraysize_setter(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        cursor.arraysize = 10
        assert cursor.arraysize == 10

    def test_close_marks_cursor_closed(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        cursor.close()
        assert cursor._closed

    def test_fetchone_on_closed_cursor_raises(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        cursor.close()

        with pytest.raises(InterfaceError, match="Cursor is closed"):
            cursor.fetchone()

    def test_fetchone_no_results(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        result = cursor.fetchone()
        assert result is None

    def test_fetchall_no_results(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        result = cursor.fetchall()
        assert result == []

    def test_fetchmany_no_results(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        result = cursor.fetchmany(5)
        assert result == []

    def test_context_manager(self) -> None:
        conn = Connection("localhost:9001")
        with Cursor(conn) as cursor:
            assert not cursor._closed
        assert cursor._closed

    def test_iterator(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        cursor._rows = [(1, "a"), (2, "b"), (3, "c")]

        results = list(cursor)
        assert results == [(1, "a"), (2, "b"), (3, "c")]

    def test_setinputsizes_noop(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        # Should not raise
        cursor.setinputsizes([None, None])

    def test_setoutputsize_noop(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        # Should not raise
        cursor.setoutputsize(100, 0)
