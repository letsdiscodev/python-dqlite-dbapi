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

    def test_lastrowid_initially_none(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        assert cursor.lastrowid is None

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

    def test_close_is_idempotent(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        cursor.close()
        cursor.close()  # must not raise
        assert cursor._closed

    def test_connection_property(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        assert cursor.connection is conn

    def test_fetchone_on_closed_cursor_raises(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        cursor.close()

        with pytest.raises(InterfaceError, match="Cursor is closed"):
            cursor.fetchone()

    def test_fetchone_without_execute_raises(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        with pytest.raises(InterfaceError, match="No result set"):
            cursor.fetchone()

    def test_fetchall_without_execute_raises(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        with pytest.raises(InterfaceError, match="No result set"):
            cursor.fetchall()

    def test_fetchmany_without_execute_raises(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        with pytest.raises(InterfaceError, match="No result set"):
            cursor.fetchmany(5)

    def test_context_manager(self) -> None:
        conn = Connection("localhost:9001")
        with Cursor(conn) as cursor:
            assert not cursor._closed
        assert cursor._closed

    def test_iterator(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        cursor._rows = [(1, "a"), (2, "b"), (3, "c")]
        cursor._description = [("id", None, None, None, None, None, None)]

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


class TestOptionalCursorMethodsRaise:
    def test_callproc_raises_not_supported(self) -> None:
        from dqlitedbapi.exceptions import NotSupportedError

        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        with pytest.raises(NotSupportedError):
            cursor.callproc("some_proc")

    def test_nextset_raises_not_supported(self) -> None:
        from dqlitedbapi.exceptions import NotSupportedError

        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        with pytest.raises(NotSupportedError):
            cursor.nextset()

    def test_scroll_raises_not_supported(self) -> None:
        from dqlitedbapi.exceptions import NotSupportedError

        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        with pytest.raises(NotSupportedError):
            cursor.scroll(0)
