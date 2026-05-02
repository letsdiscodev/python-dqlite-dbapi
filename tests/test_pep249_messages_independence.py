"""Pin: PEP 249 §6.1.1 / §6.1.2 — ``Connection.messages`` and
``Cursor.messages`` are independent surfaces. Cursor methods clear
only ``Cursor.messages``; connection methods clear only
``Connection.messages``.

Previously the cursor's secondary methods (``fetchone`` /
``fetchmany`` / ``fetchall`` / ``setinputsizes`` / ``setoutputsize``
/ ``callproc`` / ``nextset`` / ``scroll``) over-cleared
``Connection.messages`` from inside the cursor — defeating PEP 249's
independent-surface contract: a sibling-cursor or direct-connection
inspection of ``connection.messages`` after a cursor call always saw
``[]`` regardless of what events the connection had collected.

This module pins the corrected behaviour: cross-surface reads survive
across cursor / connection method boundaries.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi import NotSupportedError
from dqlitedbapi.cursor import Cursor


def _make_cursor() -> Cursor:
    conn = MagicMock()
    conn.messages = []
    conn._check_thread = MagicMock()
    cur = Cursor(conn)
    cur._rows = [("a",), ("b",), ("c",)]
    cur._description = [("c", 3, None, None, None, None, None)]  # type: ignore[assignment]
    cur._arraysize = 1
    return cur


_SESSION_DIAGNOSTIC = (RuntimeError, "session-level diagnostic")


@pytest.mark.parametrize(
    "method, args",
    [
        ("fetchone", ()),
        ("fetchmany", (1,)),
        ("fetchall", ()),
        ("setinputsizes", ([None],)),
        ("setoutputsize", (4096,)),
    ],
)
def test_cursor_secondary_methods_preserve_connection_messages(
    method: str, args: tuple[object, ...]
) -> None:
    cur = _make_cursor()
    cur._connection.messages.append(_SESSION_DIAGNOSTIC)
    getattr(cur, method)(*args)
    assert cur._connection.messages == [_SESSION_DIAGNOSTIC]


@pytest.mark.parametrize(
    "method, args",
    [
        ("callproc", ("p",)),
        ("nextset", ()),
        ("scroll", (1,)),
    ],
)
def test_cursor_unsupported_methods_preserve_connection_messages(
    method: str, args: tuple[object, ...]
) -> None:
    """The methods that always raise ``NotSupportedError`` also must
    not clear ``Connection.messages``."""
    cur = _make_cursor()
    cur._connection.messages.append(_SESSION_DIAGNOSTIC)
    with pytest.raises(NotSupportedError):
        getattr(cur, method)(*args)
    assert cur._connection.messages == [_SESSION_DIAGNOSTIC]


def test_cursor_methods_still_clear_cursor_messages() -> None:
    """Defence pin: the §6.1.2 contract that cursor methods clear
    ``Cursor.messages`` is preserved — only the over-clear of
    ``Connection.messages`` is removed."""
    cur = _make_cursor()
    cur.messages.append((Warning, "cursor-level diag"))
    cur.fetchone()
    assert cur.messages == []
