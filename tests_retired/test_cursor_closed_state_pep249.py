"""Closed-cursor operations raise InterfaceError (PEP 249 §6.1.2);
close() preserves rowcount/lastrowid (stdlib sqlite3 parity) but clears
description and buffered rows."""

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
    """setinputsizes/setoutputsize may do nothing (PEP 249 §6.2); pin that
    they do not raise on a closed cursor."""

    def test_setinputsizes_does_not_raise_on_closed_cursor(self) -> None:
        cur = _make_cursor()
        cur.close()
        cur.setinputsizes([None])

    def test_setoutputsize_does_not_raise_on_closed_cursor(self) -> None:
        cur = _make_cursor()
        cur.close()
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


class TestClosePreservesRowcountAndLastrowid:
    def test_close_preserves_rowcount_and_lastrowid_clears_result_set(self) -> None:
        cur = _make_cursor()
        cur._rowcount = 5
        cur._lastrowid = 42
        cur._description = [("c", 3, None, None, None, None, None)]  # type: ignore[assignment]
        cur._rows = [(1,), (2,)]
        cur.close()
        assert cur.description is None
        # rowcount / lastrowid survive close, matching stdlib.
        assert cur.rowcount == 5
        assert cur.lastrowid == 42
