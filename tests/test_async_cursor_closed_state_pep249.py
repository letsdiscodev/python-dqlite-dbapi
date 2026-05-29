"""PEP 249 §6.1.2 — closed-cursor operations raise InterfaceError. Also pins that ``close()``
preserves ``rowcount``/``lastrowid`` (stdlib parity) while clearing ``description`` and rows."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi import InterfaceError, NotSupportedError
from dqlitedbapi.aio.cursor import AsyncCursor


def _make_async_cursor() -> AsyncCursor:
    conn = MagicMock()
    conn.messages = []
    conn.close = AsyncMock()
    cur = AsyncCursor(conn)
    return cur


class TestSetinputsizesSetoutputsizeClosedCheck:
    """PEP 249 §6.2: these methods are free to do nothing, including on closed cursors."""

    async def test_setinputsizes_does_not_raise_on_closed_cursor(self) -> None:
        cur = _make_async_cursor()
        cur.close()
        cur.setinputsizes([None])

    async def test_setoutputsize_does_not_raise_on_closed_cursor(self) -> None:
        cur = _make_async_cursor()
        cur.close()
        cur.setoutputsize(4096)


class TestNotSupportedMethodsRaiseClosedFirst:
    async def test_callproc_on_closed_cursor_raises_interfaceerror(self) -> None:
        cur = _make_async_cursor()
        cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            cur.callproc("proc_name")

    async def test_nextset_on_closed_cursor_raises_interfaceerror(self) -> None:
        cur = _make_async_cursor()
        cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            cur.nextset()

    async def test_scroll_on_closed_cursor_raises_interfaceerror(self) -> None:
        cur = _make_async_cursor()
        cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            cur.scroll(0)

    async def test_nextset_on_open_cursor_raises_notsupported(self) -> None:
        cur = _make_async_cursor()
        with pytest.raises(NotSupportedError):
            cur.nextset()


class TestClosePreservesRowcountAndLastrowid:
    async def test_close_preserves_rowcount_and_lastrowid_clears_result_set(self) -> None:
        cur = _make_async_cursor()
        cur._rowcount = 5
        cur._lastrowid = 42
        cur._description = (("c", 3, None, None, None, None, None),)
        cur._rows = [(1,), (2,)]
        cur.close()
        # Result-set cleared; rowcount/lastrowid survive close (stdlib parity).
        assert cur.description is None
        assert cur.rowcount == 5
        assert cur.lastrowid == 42


class TestAsyncIterOnClosedCursor:
    """``async for row in closed_cursor:`` must raise InterfaceError on the first ``__anext__``."""

    async def test_async_for_on_closed_cursor_raises_interface_error(self) -> None:
        cur = _make_async_cursor()
        cur.close()
        rows: list[object] = []
        with pytest.raises(InterfaceError, match="closed"):
            async for row in cur:
                rows.append(row)
        assert rows == []

    async def test_anext_on_closed_cursor_raises_interface_error(self) -> None:
        """Direct ``__anext__`` surfaces InterfaceError, not StopAsyncIteration."""
        cur = _make_async_cursor()
        cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            await cur.__anext__()
