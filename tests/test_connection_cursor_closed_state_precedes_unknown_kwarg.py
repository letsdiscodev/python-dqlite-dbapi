"""Pin: ``Connection.cursor(**kwargs)`` and ``AsyncConnection.cursor``
check the closed state BEFORE rejecting unknown kwargs, so a closed
connection diagnostic is not masked by a capability-gap diagnostic.

Stdlib ``sqlite3`` and the in-package ``Cursor.execute`` rationale
(closed-first; see cursor.py's documented precedence) are the
reference.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


def test_sync_cursor_unknown_kwarg_on_closed_raises_interface_error() -> None:
    c = dqlitedbapi.connect("127.0.0.1:9999")
    c._closed = True
    c._closed_flag[0] = True
    with pytest.raises(InterfaceError, match="closed"):
        c.cursor(factory=object)


def test_sync_cursor_unknown_kwarg_on_open_still_raises_not_supported() -> None:
    """Negative-control: on an open connection, the unknown-kwarg
    rejection still fires (with NotSupportedError, after the closed
    check passes)."""
    c = dqlitedbapi.connect("127.0.0.1:9999")
    try:
        with pytest.raises(dqlitedbapi.NotSupportedError, match="kwargs"):
            c.cursor(factory=object)
    finally:
        c._closed_flag[0] = True


def test_async_cursor_unknown_kwarg_on_closed_raises_interface_error() -> None:
    c = AsyncConnection("127.0.0.1:9999")
    c._closed = True
    with pytest.raises(InterfaceError, match="closed"):
        c.cursor(factory=object)
