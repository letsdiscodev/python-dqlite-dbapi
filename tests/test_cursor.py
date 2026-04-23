"""Tests for Cursor class."""

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import InterfaceError, ProgrammingError


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

    def test_fetchone_without_execute_raises(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        with pytest.raises(ProgrammingError, match="no results to fetch"):
            cursor.fetchone()

    def test_fetchall_without_execute_raises(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        with pytest.raises(ProgrammingError, match="no results to fetch"):
            cursor.fetchall()

    def test_fetchmany_without_execute_raises(self) -> None:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        with pytest.raises(ProgrammingError, match="no results to fetch"):
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


class TestCursorDescriptionFreshCopy:
    """Pin the `Cursor.description` fresh-copy-per-access contract.

    The property returns `list(self._description)` so that a caller
    mutating the returned list (e.g. `.clear()`, `.append(...)`) cannot
    corrupt the cursor's internal state. PEP 249 does not prescribe
    immutability, so the contract is a project convention; these tests
    freeze it against an accidental regression to
    `return self._description` (which would re-alias the internal list).
    """

    def _make_cursor_with_description(self) -> Cursor:
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        cursor._description = [
            ("a", 4, None, None, None, None, None),
            ("b", 4, None, None, None, None, None),
        ]
        return cursor

    def test_description_returns_fresh_list_per_call(self) -> None:
        cursor = self._make_cursor_with_description()
        desc1 = cursor.description
        desc2 = cursor.description
        assert desc1 is not None
        assert desc2 is not None
        # Fresh list each call — distinct objects, same contents.
        assert desc1 is not desc2
        assert desc1 == desc2

    def test_description_returned_list_is_not_the_internal_list(self) -> None:
        cursor = self._make_cursor_with_description()
        desc = cursor.description
        # The returned list must not be aliased to the cursor's internal
        # _description list. A regression that dropped the `list(...)`
        # wrap would fail this.
        assert desc is not cursor._description

    def test_description_mutation_does_not_affect_internal_state(self) -> None:
        cursor = self._make_cursor_with_description()
        desc = cursor.description
        assert desc is not None
        desc.clear()
        # Second access returns a full-length list again — the first
        # caller's .clear() did not corrupt internal state.
        desc2 = cursor.description
        assert desc2 is not None
        assert len(desc2) == 2

    def test_description_empty_list_is_fresh_copy(self) -> None:
        # Non-None-but-empty _description must still return a fresh
        # empty list per call, not the same object. A regression like
        # `return self._description or []` would re-alias on the
        # non-empty path and silently break this on the first mutation.
        conn = Connection("localhost:9001")
        cursor = Cursor(conn)
        cursor._description = []
        desc1 = cursor.description
        desc2 = cursor.description
        assert desc1 == []
        assert desc2 == []
        assert desc1 is not desc2
        assert desc1 is not cursor._description


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
