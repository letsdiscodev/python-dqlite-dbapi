"""row_factory inherits from a Connection/AsyncConnection subclass at construction:
the inheritance check uses ``isinstance`` (not ``__name__``) so subclasses inherit
while MagicMock fakes are still rejected."""

from __future__ import annotations

from unittest.mock import MagicMock

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor


class _TracingConnection(Connection):
    """Representative user-side subclass."""


class _TracingAsyncConnection(AsyncConnection):
    """Async-sibling subclass mirror."""


def _factory(_cur: object, row: tuple[object, ...]) -> list[object]:
    return list(row)


def test_sync_cursor_inherits_row_factory_from_connection() -> None:
    """Baseline: a real ``Connection`` propagates its row_factory."""
    conn = Connection("localhost:9001", timeout=2.0)
    conn._row_factory = _factory
    cur = Cursor(conn)
    assert cur.row_factory is _factory


def test_sync_cursor_inherits_row_factory_from_subclass() -> None:
    """A user subclass inherits row_factory like the parent (was rejected by name check)."""
    conn = _TracingConnection("localhost:9001", timeout=2.0)
    conn._row_factory = _factory
    cur = Cursor(conn)
    assert cur.row_factory is _factory


def test_sync_cursor_rejects_magicmock_row_factory() -> None:
    """A MagicMock fake's auto-magic ``_row_factory`` must NOT propagate."""
    fake = MagicMock()
    cur = Cursor(fake)
    assert cur.row_factory is None


def test_async_cursor_inherits_row_factory_from_connection() -> None:
    conn = AsyncConnection("localhost:9001")
    conn._row_factory = _factory
    cur = AsyncCursor(conn)
    assert cur.row_factory is _factory


def test_async_cursor_inherits_row_factory_from_subclass() -> None:
    """Async sibling: subclasses inherit row_factory."""
    conn = _TracingAsyncConnection("localhost:9001")
    conn._row_factory = _factory
    cur = AsyncCursor(conn)
    assert cur.row_factory is _factory


def test_async_cursor_rejects_magicmock_row_factory() -> None:
    fake = MagicMock()
    cur = AsyncCursor(fake)
    assert cur.row_factory is None
