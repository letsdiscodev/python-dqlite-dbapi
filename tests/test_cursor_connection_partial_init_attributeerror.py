"""Pin: ``Cursor.connection`` / ``AsyncCursor.connection`` translate
``AttributeError`` (from a partial-init / mock parent that lacks
``.address``) into ``InterfaceError`` so the PEP 249 §7 hierarchy
boundary is maintained.

Companion to ``test_cursor_connection_property_after_gc_pep249`` which
pins the ``ReferenceError`` arm only. The probe attribute access (``_ =
self._connection.address``) was tolerant only of ``ReferenceError``;
``Cursor.__new__(Cursor); cur._connection = object()`` produced bare
``AttributeError`` outside the hierarchy.
"""

from __future__ import annotations

import pytest

import dqlitedbapi


def test_sync_cursor_connection_partial_init_raises_interface_error() -> None:
    cur = dqlitedbapi.Cursor.__new__(dqlitedbapi.Cursor)
    # No ``.address`` attribute — exercises the AttributeError catch arm.
    cur._connection = object()  # type: ignore[assignment]

    with pytest.raises(dqlitedbapi.InterfaceError) as exc_info:
        _ = cur.connection

    # The InterfaceError chains the AttributeError so the operator
    # can see the root cause.
    assert isinstance(exc_info.value.__cause__, AttributeError)


def test_async_cursor_connection_partial_init_raises_interface_error() -> None:
    from dqlitedbapi.aio.cursor import AsyncCursor

    cur = AsyncCursor.__new__(AsyncCursor)
    cur._connection = object()  # type: ignore[assignment]

    with pytest.raises(dqlitedbapi.InterfaceError) as exc_info:
        _ = cur.connection

    assert isinstance(exc_info.value.__cause__, AttributeError)


def test_sync_cursor_connection_partial_init_routed_through_dbapi_error() -> None:
    """The translation is the load-bearing PEP 249 §7 hierarchy
    defence: a generic ``except dbapi.Error:`` must match."""
    cur = dqlitedbapi.Cursor.__new__(dqlitedbapi.Cursor)
    cur._connection = object()  # type: ignore[assignment]

    with pytest.raises(dqlitedbapi.Error):
        _ = cur.connection
