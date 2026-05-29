"""Cursor.connection / AsyncCursor.connection translate AttributeError
(partial-init parent lacking .address) into InterfaceError, keeping the
PEP 249 §7 hierarchy boundary. Companion to the ReferenceError arm in
test_cursor_connection_property_after_gc_pep249."""

from __future__ import annotations

import pytest

import dqlitedbapi


def test_sync_cursor_connection_partial_init_raises_interface_error() -> None:
    cur = dqlitedbapi.Cursor.__new__(dqlitedbapi.Cursor)
    # No .address attribute — exercises the AttributeError catch arm.
    cur._connection = object()  # type: ignore[assignment]

    with pytest.raises(dqlitedbapi.InterfaceError) as exc_info:
        _ = cur.connection

    assert isinstance(exc_info.value.__cause__, AttributeError)


def test_async_cursor_connection_partial_init_raises_interface_error() -> None:
    from dqlitedbapi.aio.cursor import AsyncCursor

    cur = AsyncCursor.__new__(AsyncCursor)
    cur._connection = object()  # type: ignore[assignment]

    with pytest.raises(dqlitedbapi.InterfaceError) as exc_info:
        _ = cur.connection

    assert isinstance(exc_info.value.__cause__, AttributeError)


def test_sync_cursor_connection_partial_init_routed_through_dbapi_error() -> None:
    """A generic except dbapi.Error: must match the translated error."""
    cur = dqlitedbapi.Cursor.__new__(dqlitedbapi.Cursor)
    cur._connection = object()  # type: ignore[assignment]

    with pytest.raises(dqlitedbapi.Error):
        _ = cur.connection
