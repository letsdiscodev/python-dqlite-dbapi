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

    def test_fetchmany_on_closed_cursor_raises(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        cursor.close()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            cursor.fetchmany(5)

    def test_fetchall_on_closed_cursor_raises(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        cursor.close()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            cursor.fetchall()

    def test_fetchone_without_execute_returns_none(self) -> None:
        """Stdlib parity: fetchone on a never-executed / DML-only
        cursor returns None rather than raising. fetchmany / fetchall
        return an empty list on the same path (also stdlib-parity)."""
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        assert cursor.fetchone() is None

    def test_fetchall_without_execute_returns_empty_list(self) -> None:
        """Stdlib parity: ``sqlite3.Cursor.fetchall()`` on a never-
        executed / DML-only cursor returns ``[]``. Symmetric with
        ``fetchone`` returning ``None``."""
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        assert cursor.fetchall() == []

    def test_fetchmany_without_execute_returns_empty_list(self) -> None:
        """Stdlib parity: ``sqlite3.Cursor.fetchmany()`` on a never-
        executed / DML-only cursor returns ``[]``. Symmetric with
        ``fetchone`` and ``fetchall``."""
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        assert cursor.fetchmany(5) == []

    def test_context_manager(self) -> None:
        conn = Connection("localhost:9001")
        with Cursor(conn) as cursor:
            assert not cursor._closed
        assert cursor._closed

    def test_context_manager_propagates_body_exception(self) -> None:
        """PEP 343 contract: __exit__ returning falsy must NOT suppress
        the body exception. Cursor.__exit__ delegates to close() which
        returns None, so a body exception must propagate AND the cursor
        must still be closed afterwards."""
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        with pytest.raises(ValueError, match="body raised"), cursor:
            raise ValueError("body raised")
        assert cursor._closed

    def test_iterator(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        cursor._rows = [(1, "a"), (2, "b"), (3, "c")]
        cursor._description = [("id", None, None, None, None, None, None)]  # type: ignore[assignment]

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


class TestCursorDescriptionIdentity:
    """Pin the `Cursor.description` same-object-per-access contract.

    The property returns ``self._description`` unchanged (matching
    stdlib ``sqlite3.Cursor.description``). Storage is a tuple of
    7-tuples — structurally immutable, so a defensive copy is not
    needed to keep the cursor's internal state safe from caller
    mutation. A regression that reintroduced a ``list(...)`` wrap
    would fail these tests.
    """

    def _make_cursor_with_description(self) -> Cursor:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        cursor._description = (
            ("a", 4, None, None, None, None, None),
            ("b", 4, None, None, None, None, None),
        )
        return cursor

    def test_description_returns_same_object_per_call(self) -> None:
        cursor = self._make_cursor_with_description()
        desc1 = cursor.description
        desc2 = cursor.description
        assert desc1 is not None
        assert desc2 is not None
        # Same tuple object each call — matches stdlib sqlite3.
        assert desc1 is desc2

    def test_description_is_the_internal_tuple(self) -> None:
        cursor = self._make_cursor_with_description()
        desc = cursor.description
        assert desc is cursor._description

    def test_description_is_tuple(self) -> None:
        cursor = self._make_cursor_with_description()
        desc = cursor.description
        assert isinstance(desc, tuple)

    def test_description_empty_tuple_is_same_object(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        cursor._description = ()
        desc1 = cursor.description
        desc2 = cursor.description
        assert desc1 == ()
        assert desc2 == ()
        assert desc1 is desc2


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


class TestConnectionCloseResetsLock:
    def test_close_nulls_connect_lock(self) -> None:
        """After close(), the asyncio connect lock must be reset so it
        doesn't outlive its owning loop (symmetry with the async side)."""
        import asyncio

        conn = Connection("localhost:9001")
        # Simulate the state a lazy _get_async_connection would have left:
        # a background loop running and an asyncio.Lock created on it.
        loop = conn._ensure_loop()

        async def _make_lock() -> asyncio.Lock:
            return asyncio.Lock()

        conn._connect_lock = asyncio.run_coroutine_threadsafe(_make_lock(), loop).result(timeout=5)
        assert conn._connect_lock is not None

        conn.close()
        assert conn._connect_lock is None
