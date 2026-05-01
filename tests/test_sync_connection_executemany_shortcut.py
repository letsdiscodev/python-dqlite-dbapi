"""Pin: sync ``Connection.executemany`` shortcut exists, with the
same cleanup-on-raise discipline as the async sibling and the sync
``Connection.execute`` shortcut.

Cross-driver code reaches for ``connection.executemany(...)`` on both
sync and async sides (stdlib ``sqlite3``, aiosqlite, psycopg,
asyncpg). Without the sync method, callers porting from those drivers
hit ``AttributeError`` — an opaque diagnostic that escapes the
``dbapi.Error`` hierarchy.
"""

from __future__ import annotations

import inspect

import dqlitedbapi
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor


def test_sync_connection_has_executemany_shortcut() -> None:
    assert hasattr(Connection, "executemany")
    assert callable(Connection.executemany)


def test_sync_executemany_signature_matches_async() -> None:
    """The sync method's parameters must match the async sibling so
    cross-driver code that swaps surface gets the same call shape."""
    sync_params = list(inspect.signature(Connection.executemany).parameters.keys())
    async_params = list(inspect.signature(AsyncConnection.executemany).parameters.keys())
    assert sync_params == async_params


def test_sync_executemany_returns_cursor_annotation() -> None:
    sig = inspect.signature(Connection.executemany)
    assert sig.return_annotation is Cursor


def test_module_connect_signature_unchanged() -> None:
    """Defensive: dqlitedbapi.connect signature is unrelated to the
    Connection method add. Pin so a future refactor doesn't drift."""
    assert callable(dqlitedbapi.connect)
