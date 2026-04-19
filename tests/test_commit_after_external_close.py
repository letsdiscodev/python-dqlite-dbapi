"""commit() / rollback() after an externally-invalidated connection.

PEP 249 requires methods called on a closed connection to raise a
subclass of ``Error``. An "externally invalidated" connection (e.g.
the server closed the socket, the protocol aborted mid-operation,
the pool decided to invalidate the underlying async conn) should
surface as a PEP 249 error class — not bubble up a raw
DqliteConnectionError or leak asyncio internals.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from dqliteclient.exceptions import DqliteConnectionError
from dqlitedbapi import Connection
from dqlitedbapi.exceptions import Error


def _make_connection_with_invalidated_async() -> Connection:
    """Build a sync ``Connection`` whose underlying async conn raises
    ``DqliteConnectionError`` on every wire call — simulating the
    state left behind by an external invalidation."""
    conn = Connection("localhost:9001")
    fake_async = MagicMock()
    fake_async.execute = AsyncMock(side_effect=DqliteConnectionError("connection invalidated"))
    fake_async.close = AsyncMock()
    fake_async._in_use = False
    fake_async._bound_loop = None
    conn._async_conn = fake_async  # Bypass lazy connect
    return conn


def test_commit_on_invalidated_connection_raises_dbapi_error() -> None:
    conn = _make_connection_with_invalidated_async()
    try:
        with pytest.raises(Error):
            conn.commit()
    finally:
        # Mark closed to keep finalizer quiet.
        conn._closed = True


def test_rollback_on_invalidated_connection_raises_dbapi_error() -> None:
    conn = _make_connection_with_invalidated_async()
    try:
        with pytest.raises(Error):
            conn.rollback()
    finally:
        conn._closed = True
