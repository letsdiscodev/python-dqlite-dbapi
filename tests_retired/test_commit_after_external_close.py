"""commit()/rollback() on an externally-invalidated connection must surface a PEP 249 Error
subclass, not a raw DqliteConnectionError or leaked asyncio internals.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from dqliteclient.exceptions import DqliteConnectionError
from dqlitedbapi import Connection
from dqlitedbapi.exceptions import OperationalError


def _make_connection_with_invalidated_async() -> Connection:
    """A Connection whose async conn raises DqliteConnectionError on every wire call."""
    conn = Connection("localhost:9001")
    fake_async = MagicMock()
    fake_async.execute = AsyncMock(side_effect=DqliteConnectionError("connection invalidated"))
    fake_async.close = AsyncMock()
    fake_async._in_use = False
    fake_async._bound_loop = None
    conn._async_conn = fake_async  # bypass lazy connect
    return conn


def test_commit_on_invalidated_connection_raises_dbapi_error() -> None:
    """DqliteConnectionError surfaces as OperationalError; pin the exact class."""
    conn = _make_connection_with_invalidated_async()
    try:
        with pytest.raises(OperationalError):
            conn.commit()
    finally:
        conn._closed = True


def test_rollback_on_invalidated_connection_raises_dbapi_error() -> None:
    conn = _make_connection_with_invalidated_async()
    try:
        with pytest.raises(OperationalError):
            conn.rollback()
    finally:
        conn._closed = True


def test_commit_on_invalidated_connection_raises_operational_error_with_cause() -> None:
    """Pin OperationalError plus the __cause__ chain to DqliteConnectionError: SQLAlchemy's
    is_disconnect classifier branches on the surfaced class."""
    conn = _make_connection_with_invalidated_async()
    try:
        with pytest.raises(OperationalError) as ei:
            conn.commit()
        assert isinstance(ei.value.__cause__, DqliteConnectionError)
        # Class lives in dqlitedbapi, not dqliteclient: the wrap occurred.
        assert ei.value.__class__.__module__.startswith("dqlitedbapi")
    finally:
        conn._closed = True


def test_rollback_on_invalidated_connection_raises_operational_error_with_cause() -> None:
    conn = _make_connection_with_invalidated_async()
    try:
        with pytest.raises(OperationalError) as ei:
            conn.rollback()
        assert isinstance(ei.value.__cause__, DqliteConnectionError)
        assert ei.value.__class__.__module__.startswith("dqlitedbapi")
    finally:
        conn._closed = True
