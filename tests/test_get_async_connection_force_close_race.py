"""Pin: ``_get_async_connection`` snapshots the connect lock and inner conn
to locals so a foreign-thread ``force_close_transport`` nulling either mid-call
surfaces ``InterfaceError`` (inside the PEP 249 tree, classifiable by SA's
``is_disconnect``) rather than a bare ``AttributeError`` / ``TypeError``.
"""

from __future__ import annotations

import asyncio
import os
import threading
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import InterfaceError


def _make_connection() -> Connection:
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._async_conn = None
    conn._connect_lock = None
    conn._address = "host:1234"
    conn._database = "x"
    conn._timeout = 5.0
    conn._max_total_rows = None
    conn._max_continuation_frames = None
    conn._trust_server_heartbeat = False
    conn._close_timeout = 5.0
    conn._inner_finalize_handle = [None]
    conn._creator_pid = os.getpid()
    conn._creator_thread = threading.get_ident()
    return conn


@pytest.mark.asyncio
async def test_get_async_connection_raises_interface_error_on_nulled_lock() -> None:
    """``_connect_lock`` None at the snapshot point raises InterfaceError,
    not AttributeError."""
    conn = _make_connection()
    conn._connect_lock = None

    original_is_none_check = type(conn).__dict__["_get_async_connection"]
    real_lock_cls = asyncio.Lock

    def _null_lock() -> object:
        return None

    # Patch asyncio.Lock to a None factory so the snapshot sees the same
    # shape a foreign-thread force_close_transport would produce.
    asyncio.Lock = _null_lock  # type: ignore[misc,assignment]
    try:
        with pytest.raises(InterfaceError, match="Connection is closed"):
            await conn._get_async_connection()
    finally:
        asyncio.Lock = real_lock_cls  # type: ignore[misc]

    _ = original_is_none_check  # quiet the unused-import nag


@pytest.mark.asyncio
async def test_get_async_connection_raises_interface_error_on_nulled_inner() -> None:
    """``_async_conn`` nulled before the return read raises InterfaceError,
    not delivering None to the caller."""
    conn = _make_connection()

    fake_inner = MagicMock()

    async def fake_build(*_args: object, **_kwargs: object) -> object:
        return fake_inner

    import dqlitedbapi.connection as conn_mod

    orig = conn_mod._build_and_connect

    async def patched_build(*args: object, **kwargs: object) -> object:
        result = await fake_build(*args, **kwargs)
        return result

    conn_mod._build_and_connect = patched_build  # type: ignore[assignment]
    try:
        inner = await conn._get_async_connection()
        assert inner is fake_inner

        # Simulate the race: null _async_conn and the lock so the next
        # call retries the build but encounters our null.
        conn._async_conn = None
        conn._connect_lock = None

        async def race_build(*args: object, **kwargs: object) -> object:
            conn._async_conn = None  # foreign-thread race
            return None  # match: snapshot will be None

        conn_mod._build_and_connect = race_build  # type: ignore[assignment]

        with pytest.raises(InterfaceError, match="Connection is closed"):
            await conn._get_async_connection()
    finally:
        conn_mod._build_and_connect = orig
