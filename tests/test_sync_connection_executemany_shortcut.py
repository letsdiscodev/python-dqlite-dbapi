"""Sync ``Connection.executemany`` shortcut exists, matching the async sibling and
``Connection.execute`` (cross-driver code reaches for it on both surfaces)."""

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
    """Sync parameters must match the async sibling so swapping surfaces keeps the shape."""
    sync_params = list(inspect.signature(Connection.executemany).parameters.keys())
    async_params = list(inspect.signature(AsyncConnection.executemany).parameters.keys())
    assert sync_params == async_params


def test_sync_executemany_returns_cursor_annotation() -> None:
    sig = inspect.signature(Connection.executemany)
    assert sig.return_annotation is Cursor


def test_module_connect_signature_unchanged() -> None:
    assert callable(dqlitedbapi.connect)
