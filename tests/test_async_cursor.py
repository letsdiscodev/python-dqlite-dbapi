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

    @pytest.mark.asyncio
    async def test_close_marks_cursor_closed(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        await cursor.close()
        assert cursor._closed

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        await cursor.close()
        await cursor.close()  # must not raise
        assert cursor._closed

    def test_connection_property(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert cursor.connection is conn

    @pytest.mark.asyncio
    async def test_fetchone_on_closed_cursor_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        await cursor.close()

        with pytest.raises(InterfaceError, match="Cursor is closed"):
            await cursor.fetchone()

    @pytest.mark.asyncio
    async def test_fetchmany_on_closed_cursor_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        await cursor.close()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            await cursor.fetchmany(5)

    @pytest.mark.asyncio
    async def test_fetchall_on_closed_cursor_raises(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        await cursor.close()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            await cursor.fetchall()

    @pytest.mark.asyncio
    async def test_fetchone_without_execute_returns_none(self) -> None:
        """Stdlib parity: fetchone on a never-executed cursor returns
        None rather than raising. See sync sibling for rationale."""
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_fetchmany_without_execute_returns_empty_list(self) -> None:
        """Stdlib parity: returns ``[]`` rather than raising —
        symmetric with ``fetchone`` returning ``None``. See sync
        sibling for rationale."""
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert await cursor.fetchmany(5) == []

    @pytest.mark.asyncio
    async def test_fetchall_without_execute_returns_empty_list(self) -> None:
        """Stdlib parity: returns ``[]`` rather than raising. See
        sync sibling for rationale."""
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        assert await cursor.fetchall() == []

    @pytest.mark.asyncio
    async def test_fetchone_no_rows_returns_none(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        # Simulate a query that returned zero rows
        cursor._description = [("id", None, None, None, None, None, None)]  # type: ignore[assignment]
        cursor._rows = []
        result = await cursor.fetchone()
        assert result is None

    @pytest.mark.asyncio
    async def test_fetchall_no_rows_returns_empty(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor._description = [("id", None, None, None, None, None, None)]  # type: ignore[assignment]
        cursor._rows = []
        result = await cursor.fetchall()
        assert result == []

    @pytest.mark.asyncio
    async def test_context_manager(self) -> None:
        conn = AsyncConnection("localhost:9001")
        async with AsyncCursor(conn) as cursor:
            assert not cursor._closed
        assert cursor._closed

    @pytest.mark.asyncio
    async def test_context_manager_propagates_body_exception(self) -> None:
        """PEP 343 contract: __aexit__ returning falsy must NOT suppress
        the body exception. Mirror of the sync sibling pin."""
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        with pytest.raises(ValueError, match="body raised"):  # noqa: SIM117
            async with cursor:
                raise ValueError("body raised")
        assert cursor._closed

    @pytest.mark.asyncio
    async def test_async_iterator(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor._rows = [(1, "a"), (2, "b"), (3, "c")]
        cursor._description = [("id", None, None, None, None, None, None)]  # type: ignore[assignment]

        results = [row async for row in cursor]
        assert results == [(1, "a"), (2, "b"), (3, "c")]

    @pytest.mark.asyncio
    async def test_setinputsizes_noop(self) -> None:
        # Runs inside a loop because ``setinputsizes`` now routes through
        # ``_ensure_locks()`` (loop-binding check).
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor.setinputsizes([None, None])

    @pytest.mark.asyncio
    async def test_setoutputsize_noop(self) -> None:
        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        cursor.setoutputsize(100, 0)


class TestAsyncCursorDescriptionIdentity:
    """Mirror of ``TestCursorDescriptionIdentity`` in ``test_cursor.py``.

    Both sync and async `description` properties return the stored
    tuple unchanged; a regression on either side would silently drift
    the two branches apart.
    """

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
    @pytest.mark.asyncio
    async def test_callproc_raises_not_supported(self) -> None:
        from dqlitedbapi.exceptions import NotSupportedError

        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        with pytest.raises(NotSupportedError):
            cursor.callproc("some_proc")

    def test_callproc_nextset_scroll_are_sync(self) -> None:
        """These three PEP 249 optional extensions all unconditionally raise
        ``NotSupportedError``. They must stay sync so callers can catch the
        error with a bare ``try: cursor.callproc(...) except ...`` rather
        than accidentally returning a coroutine object that is never awaited.
        The adapter in ``sqlalchemy-dqlite`` exposes the same three as sync.
        """
        import inspect

        assert not inspect.iscoroutinefunction(AsyncCursor.callproc)
        assert not inspect.iscoroutinefunction(AsyncCursor.nextset)
        assert not inspect.iscoroutinefunction(AsyncCursor.scroll)

    @pytest.mark.asyncio
    async def test_nextset_raises_not_supported(self) -> None:
        from dqlitedbapi.exceptions import NotSupportedError

        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        with pytest.raises(NotSupportedError):
            cursor.nextset()

    @pytest.mark.asyncio
    async def test_scroll_raises_not_supported(self) -> None:
        from dqlitedbapi.exceptions import NotSupportedError

        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)
        with pytest.raises(NotSupportedError):
            cursor.scroll(0)

    @pytest.mark.asyncio
    async def test_execute_rechecks_closed_inside_op_lock(self) -> None:
        """A cursor closed after the fast-path check but before the
        inner execute work must still raise ``InterfaceError`` with a
        "Cursor is closed" message. Without the re-check inside the
        op-lock, the caller would see a generic connection error
        instead of the sharper cursor-state error.
        """
        import asyncio
        from unittest.mock import patch

        conn = AsyncConnection("localhost:9001")
        cursor = AsyncCursor(conn)

        close_allowed = asyncio.Event()
        ensure_entered = asyncio.Event()

        async def fake_ensure(self_arg):
            ensure_entered.set()
            await close_allowed.wait()
            # Return a sentinel; we expect the re-check of _closed to
            # fire before this value is ever dereferenced.
            return object()

        async def run_execute() -> None:
            with patch.object(AsyncConnection, "_ensure_connection", fake_ensure):
                await cursor.execute("SELECT 1")

        task = asyncio.create_task(run_execute())
        await ensure_entered.wait()
        await cursor.close()
        close_allowed.set()
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            await task
