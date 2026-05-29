"""Tests for AsyncCursor class."""

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import InterfaceError


class TestAsyncCursor:
    def test_description_initially_none(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert cursor.description is None

    def test_rowcount_initially_minus_one(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert cursor.rowcount == -1

    def test_arraysize_default(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert cursor.arraysize == 1

    def test_arraysize_setter(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor.arraysize = 10
        assert cursor.arraysize == 10

    def test_lastrowid_initially_none(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert cursor.lastrowid is None

    async def test_close_marks_cursor_closed(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor.close()
        assert cursor._closed

    async def test_close_is_idempotent(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor.close()
        cursor.close()
        assert cursor._closed

    def test_connection_property(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert cursor.connection is conn

    async def test_fetchone_on_closed_cursor_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor.close()

        with pytest.raises(InterfaceError, match="Cursor is closed"):
            await cursor.fetchone()

    async def test_fetchmany_on_closed_cursor_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor.close()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            await cursor.fetchmany(5)

    async def test_fetchall_on_closed_cursor_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor.close()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            await cursor.fetchall()

    async def test_fetchone_without_execute_returns_none(self) -> None:
        """Stdlib parity: fetchone on a never-executed cursor returns None."""
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert await cursor.fetchone() is None

    async def test_fetchmany_without_execute_returns_empty_list(self) -> None:
        """Stdlib parity: fetchmany on a never-executed cursor returns ``[]``."""
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert await cursor.fetchmany(5) == []

    async def test_fetchall_without_execute_returns_empty_list(self) -> None:
        """Stdlib parity: fetchall on a never-executed cursor returns ``[]``."""
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert await cursor.fetchall() == []

    async def test_fetchone_no_rows_returns_none(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor._description = [("id", None, None, None, None, None, None)]  # type: ignore[assignment]
        cursor._rows = []
        result = await cursor.fetchone()
        assert result is None

    async def test_fetchall_no_rows_returns_empty(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor._description = [("id", None, None, None, None, None, None)]  # type: ignore[assignment]
        cursor._rows = []
        result = await cursor.fetchall()
        assert result == []

    async def test_context_manager(self) -> None:
        conn = AsyncConnection("localhost:9001")
        async with AsyncCursor(conn) as cursor:
            assert not cursor._closed
        assert cursor._closed

    async def test_context_manager_propagates_body_exception(self) -> None:
        """PEP 343: __aexit__ returning falsy must NOT suppress the body exception."""
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        with pytest.raises(ValueError, match="body raised"):  # noqa: SIM117
            async with cursor:
                raise ValueError("body raised")
        assert cursor._closed

    async def test_async_iterator(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor._rows = [(1, "a"), (2, "b"), (3, "c")]
        cursor._description = [("id", None, None, None, None, None, None)]  # type: ignore[assignment]

        results = [row async for row in cursor]
        assert results == [(1, "a"), (2, "b"), (3, "c")]

    async def test_setinputsizes_noop(self) -> None:
        # Needs a running loop: setinputsizes routes through _ensure_locks().
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor.setinputsizes([None, None])

    async def test_setoutputsize_noop(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor.setoutputsize(100, 0)


class TestAsyncCursorDescriptionIdentity:
    """``description`` returns the stored tuple unchanged (mirror of the sync sibling)."""

    def _make_cursor_with_description(self) -> AsyncCursor:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
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
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor._description = ()
        desc1 = cursor.description
        desc2 = cursor.description
        assert desc1 == ()
        assert desc2 == ()
        assert desc1 is desc2


class TestOptionalAsyncCursorMethodsRaise:
    async def test_callproc_raises_not_supported(self) -> None:
        from dqlitedbapi.exceptions import NotSupportedError

        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        with pytest.raises(NotSupportedError):
            cursor.callproc("some_proc")

    def test_callproc_nextset_scroll_are_sync(self) -> None:
        """callproc/nextset/scroll stay sync so a bare ``try/except`` catches their
        NotSupportedError instead of getting an unawaited coroutine."""
        import inspect

        assert not inspect.iscoroutinefunction(AsyncCursor.callproc)
        assert not inspect.iscoroutinefunction(AsyncCursor.nextset)
        assert not inspect.iscoroutinefunction(AsyncCursor.scroll)

    async def test_nextset_raises_not_supported(self) -> None:
        from dqlitedbapi.exceptions import NotSupportedError

        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        with pytest.raises(NotSupportedError):
            cursor.nextset()

    async def test_scroll_raises_not_supported(self) -> None:
        from dqlitedbapi.exceptions import NotSupportedError

        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        with pytest.raises(NotSupportedError):
            cursor.scroll(0)

    async def test_execute_rechecks_closed_inside_op_lock(self) -> None:
        """A cursor closed after the fast-path check but before the inner execute work
        must still raise the sharper "Cursor is closed" error, via the op-lock re-check."""
        import asyncio
        from unittest.mock import patch

        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)

        close_allowed = asyncio.Event()
        ensure_entered = asyncio.Event()

        async def fake_ensure(self_arg):
            ensure_entered.set()
            await close_allowed.wait()
            return object()

        async def run_execute() -> None:
            with patch.object(AsyncConnection, "_ensure_connection", fake_ensure):
                await cursor.execute("SELECT 1")

        task = asyncio.create_task(run_execute())
        await ensure_entered.wait()
        cursor.close()
        close_allowed.set()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            await task
