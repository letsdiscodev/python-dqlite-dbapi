"""PEP 249 §6.1.2 — closed-cursor operations raise InterfaceError.

Also pin that ``close()`` scrubs ``rowcount`` and ``lastrowid`` so
the closed-state surface is consistent (not a mix of "reset" and
"last-operation value").
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi import InterfaceError, NotSupportedError
from dqlitedbapi.cursor import Cursor


def _make_cursor() -> Cursor:
    conn = MagicMock()
    conn.messages = []
    conn._check_thread = MagicMock()
    cur = Cursor(conn)
    return cur


class TestSetinputsizesSetoutputsizeClosedCheck:
    def test_setinputsizes_raises_on_closed_cursor(self) -> None:
        cur = _make_cursor()
        cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            cur.setinputsizes([None])

    def test_setoutputsize_raises_on_closed_cursor(self) -> None:
        cur = _make_cursor()
        cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            cur.setoutputsize(4096)


class TestNotSupportedMethodsRaiseClosedFirst:
    def test_callproc_on_closed_cursor_raises_interfaceerror(self) -> None:
        cur = _make_cursor()
        cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            cur.callproc("proc_name")

    def test_nextset_on_closed_cursor_raises_interfaceerror(self) -> None:
        cur = _make_cursor()
        cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            cur.nextset()

    def test_scroll_on_closed_cursor_raises_interfaceerror(self) -> None:
        cur = _make_cursor()
        cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            cur.scroll(0)

    def test_nextset_on_open_cursor_raises_notsupported(self) -> None:
        cur = _make_cursor()
        with pytest.raises(NotSupportedError):
            cur.nextset()


class TestCloseScrubsAllState:
    def test_close_resets_rowcount_and_lastrowid(self) -> None:
        cur = _make_cursor()
        cur._rowcount = 5
        cur._lastrowid = 42
        cur._description = [("c", 3, None, None, None, None, None)]
        cur._rows = [(1,), (2,)]
        cur.close()
        assert cur.description is None
        assert cur.rowcount == -1
        assert cur.lastrowid is None
