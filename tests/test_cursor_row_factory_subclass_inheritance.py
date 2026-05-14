"""Pin: row_factory inherits from a Connection / AsyncConnection
**subclass** at cursor construction.

Previously the cursor decided inheritance with
``type(connection).__name__ == "Connection"`` (motivated as a
MagicMock defense). A user ``class TracingConnection(Connection)`` —
a common cross-cutting pattern — has a different ``__name__`` and
silently lost row_factory inheritance, contradicting the
``sqlite3.Cursor.row_factory`` stdlib-parity surface advertised in
the docstring.

Use ``isinstance(connection, Connection)`` so subclasses inherit while
MagicMock-typed test fakes are still rejected.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor


class _TracingConnection(Connection):
    """Representative user-side subclass — cross-cutting tracing /
    metrics is a common dbapi consumer pattern (the same shape as
    ``class MyConn(sqlite3.Connection)``)."""


class _TracingAsyncConnection(AsyncConnection):
    """Async-sibling subclass mirror."""


def _factory(_cur: object, row: tuple[object, ...]) -> list[object]:
    return list(row)


# ---------------- sync cursor inheritance ----------------


def test_sync_cursor_inherits_row_factory_from_connection() -> None:
    """Baseline: a real ``Connection`` propagates its row_factory."""
    conn = Connection("localhost:9001", timeout=2.0)
    conn._row_factory = _factory
    cur = Cursor(conn)
    assert cur.row_factory is _factory


def test_sync_cursor_inherits_row_factory_from_subclass() -> None:
    """A user subclass must inherit row_factory just like the parent
    Connection — stdlib parity. Previously rejected by class-name
    check."""
    conn = _TracingConnection("localhost:9001", timeout=2.0)
    conn._row_factory = _factory
    cur = Cursor(conn)
    assert cur.row_factory is _factory


def test_sync_cursor_rejects_magicmock_row_factory() -> None:
    """MagicMock defense: an auto-magic ``_row_factory`` attribute on a
    MagicMock-typed test fake must NOT propagate."""
    fake = MagicMock()
    cur = Cursor(fake)
    assert cur.row_factory is None


# ---------------- async cursor inheritance ----------------


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
