"""__repr__ returns useful, non-default strings."""

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor


class TestConnectionRepr:
    def test_connection_repr_includes_address(self) -> None:
        conn = Connection("localhost:19001", database="x", timeout=2.0)
        try:
            r = repr(conn)
            assert "Connection" in r
            assert "localhost:19001" in r
            assert not r.startswith("<dqlitedbapi.connection.Connection object at ")
        finally:
            conn.close()

    def test_async_connection_repr(self) -> None:
        conn = AsyncConnection("localhost:19001", database="x")
        r = repr(conn)
        assert "AsyncConnection" in r
        assert "localhost:19001" in r


class TestCursorRepr:
    def test_cursor_repr(self) -> None:
        conn = Connection("localhost:19001", timeout=2.0)
        try:
            c = Cursor(conn)
            r = repr(c)
            assert "Cursor" in r
            assert "rowcount" in r
        finally:
            conn.close()

    def test_async_cursor_repr(self) -> None:
        conn = AsyncConnection("localhost:19001")
        c = AsyncCursor(conn)
        r = repr(c)
        assert "AsyncCursor" in r
        assert "rowcount" in r

    def test_cursor_repr_state_transition(self) -> None:
        """Repr reports ``open`` before close and ``closed`` after."""
        conn = Connection("localhost:19001", timeout=2.0)
        try:
            c = Cursor(conn)
            assert "open" in repr(c)
            c.close()
            assert "closed" in repr(c)
        finally:
            conn.close()

    def test_async_cursor_repr_state_transition(self) -> None:
        conn = AsyncConnection("localhost:19001")
        c = AsyncCursor(conn)
        assert "open" in repr(c)
        # ``AsyncCursor.close`` is sync by design (see its docstring).
        c.close()
        assert "closed" in repr(c)
