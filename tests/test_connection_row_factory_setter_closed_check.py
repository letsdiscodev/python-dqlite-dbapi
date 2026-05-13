"""Pin: ``Connection.row_factory.setter`` (sync and async) rejects
writes on a closed connection. Mirrors the ``Cursor.row_factory.setter``
sibling and stdlib ``sqlite3``'s closed-state precedence.

Also pins messages-clear: like every other state-mutating method on
the package's Connection surface, the setter clears
``self.messages`` as the first statement (extension of PEP 249
§6.4).
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


def test_sync_row_factory_setter_closed_raises_interface_error() -> None:
    c = dqlitedbapi.connect("127.0.0.1:9999")
    c._closed = True
    c._closed_flag[0] = True
    sentinel = object()
    c._row_factory = sentinel  # type: ignore[assignment]
    with pytest.raises(InterfaceError, match="closed"):
        c.row_factory = lambda cur, row: row
    assert c._row_factory is sentinel, (
        "closed-conn row_factory.setter must reject the write before mutating state"
    )


def test_async_row_factory_setter_closed_raises_interface_error() -> None:
    c = AsyncConnection("127.0.0.1:9999")
    c._closed = True
    sentinel = object()
    c._row_factory = sentinel  # type: ignore[assignment]
    with pytest.raises(InterfaceError, match="closed"):
        c.row_factory = lambda cur, row: row
    assert c._row_factory is sentinel


def test_sync_row_factory_setter_clears_messages() -> None:
    c = dqlitedbapi.connect("127.0.0.1:9999")
    try:
        c.messages.append((Exception, "stale"))
        c.row_factory = lambda cur, row: row
        assert c.messages == []
    finally:
        c._closed_flag[0] = True


def test_async_row_factory_setter_clears_messages() -> None:
    c = AsyncConnection("127.0.0.1:9999")
    c.messages.append((Exception, "stale"))
    c.row_factory = lambda cur, row: row
    assert c.messages == []
