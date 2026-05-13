"""Pin: ``Connection.Cursor`` and ``AsyncConnection.AsyncCursor`` class
attributes expose the cursor class so cross-driver instrumentation /
adapter code can isinstance-check the cursor type without importing
``dqlitedbapi``.

Mirrors the existing PEP 249 ``Connection.Error`` etc. extension —
the same comment block documents the motivation: keep adapter code
independent of the driver module path. This is the cursor-class
parity follow-up to that pattern, NOT a ``cursor_factory`` feature
hook (no behaviour change at ``cursor()`` time).

The attributes are bound outside the class body (post-class
assignment) to avoid shadowing the ``Cursor`` / ``AsyncCursor`` type
names in method annotations within the class scope. ``getattr`` is
used here so mypy doesn't need to know about the dynamically-bound
class attribute.
"""

from __future__ import annotations

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def test_sync_connection_class_exposes_cursor_attribute() -> None:
    assert getattr(dqlitedbapi.Connection, "Cursor") is Cursor  # noqa: B009


def test_async_connection_class_exposes_cursor_attribute() -> None:
    assert getattr(AsyncConnection, "AsyncCursor") is AsyncCursor  # noqa: B009


def test_sync_connection_instance_attribute_routes_to_class_attribute() -> None:
    """Class attribute lookup, not per-instance state."""
    conn = dqlitedbapi.Connection("localhost:9001")
    assert getattr(conn, "Cursor") is Cursor  # noqa: B009


def test_async_connection_instance_attribute_routes_to_class_attribute() -> None:
    aconn = AsyncConnection("localhost:9001")
    assert getattr(aconn, "AsyncCursor") is AsyncCursor  # noqa: B009
