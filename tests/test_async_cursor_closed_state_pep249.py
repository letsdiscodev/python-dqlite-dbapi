"""PEP 249 §6.1.2 — closed-cursor operations raise InterfaceError.

Async mirror of ``tests/test_cursor_closed_state_pep249.py``. Also
pins that ``close()`` scrubs ``rowcount`` and ``lastrowid`` so the
closed-state surface is consistent (not a mix of "reset" and
"last-operation value").

``callproc`` / ``nextset`` / ``scroll`` / ``setinputsizes`` /
``setoutputsize`` are sync methods on ``AsyncCursor`` (see the
docstring rationale at ``aio/cursor.py``) so the invocation is
direct even in async-aware tests.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi import InterfaceError, NotSupportedError
from dqlitedbapi.aio.cursor import AsyncCursor


def _make_async_cursor() -> AsyncCursor:
    conn = MagicMock()
    conn.messages = []
    # ``AsyncCursor.close()`` is async; the MagicMock stand-in exposes
    # no network I/O, so close() just toggles ``_closed`` and scrubs
    # state. AsyncMock on close lets ``await cur.close()`` work even
    # though the mock doesn't back a real connection.
    conn.close = AsyncMock()
    cur = AsyncCursor(conn)
    return cur


class TestSetinputsizesSetoutputsizeClosedCheck:
    """PEP 249 §6.2 says implementations are "free to have these
    methods do nothing" — including on closed cursors."""

    async def test_setinputsizes_does_not_raise_on_closed_cursor(self) -> None:
        cur = _make_async_cursor()
        await cur.close()
        cur.setinputsizes([None])

    async def test_setoutputsize_does_not_raise_on_closed_cursor(self) -> None:
        cur = _make_async_cursor()
        await cur.close()
        cur.setoutputsize(4096)


class TestNotSupportedMethodsRaiseClosedFirst:
    async def test_callproc_on_closed_cursor_raises_interfaceerror(self) -> None:
        cur = _make_async_cursor()
        await cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            cur.callproc("proc_name")

    async def test_nextset_on_closed_cursor_raises_interfaceerror(self) -> None:
        cur = _make_async_cursor()
        await cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            cur.nextset()

    async def test_scroll_on_closed_cursor_raises_interfaceerror(self) -> None:
        cur = _make_async_cursor()
        await cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            cur.scroll(0)

    async def test_nextset_on_open_cursor_raises_notsupported(self) -> None:
        cur = _make_async_cursor()
        with pytest.raises(NotSupportedError):
            cur.nextset()


class TestCloseScrubsAllState:
    async def test_close_resets_rowcount_and_lastrowid(self) -> None:
        cur = _make_async_cursor()
        cur._rowcount = 5
        cur._lastrowid = 42
        cur._description = (("c", 3, None, None, None, None, None),)
        cur._rows = [(1,), (2,)]
        await cur.close()
        assert cur.description is None
        assert cur.rowcount == -1
        assert cur.lastrowid is None


class TestAsyncIterOnClosedCursor:
    """``async for row in closed_cursor:`` must raise InterfaceError
    on the FIRST ``__anext__`` rather than silently exit with
    StopAsyncIteration. Sync mirror is pinned in
    ``test_pep249_misc_pins.py``."""

    async def test_async_for_on_closed_cursor_raises_interface_error(self) -> None:
        cur = _make_async_cursor()
        await cur.close()
        rows: list[object] = []
        with pytest.raises(InterfaceError, match="closed"):
            async for row in cur:
                rows.append(row)
        assert rows == []

    async def test_anext_on_closed_cursor_raises_interface_error(self) -> None:
        """Direct ``__anext__`` invocation surfaces the same
        InterfaceError, not StopAsyncIteration."""
        cur = _make_async_cursor()
        await cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            await cur.__anext__()
