"""Pin: ``Connection._get_async_connection`` snapshots the connect
lock and the inner connection to locals so a foreign-thread
``force_close_transport`` racing the daemon-loop coroutine cannot
deliver bare ``AttributeError`` / ``TypeError`` outside the
``dqlitedbapi.Error`` tree.

Two race shapes:
1. ``_connect_lock`` nulled between the existence check and the
   ``async with`` entry — would produce ``AttributeError`` from
   ``async with None:``.
2. ``_async_conn`` nulled between the lock release and the trailing
   ``return self._async_conn`` — would deliver ``None`` to the
   caller whose subsequent attribute access raises bare
   ``AttributeError``.

Both are mapped to ``InterfaceError`` so the diagnostic stays
inside the PEP 249 error hierarchy and SA's ``is_disconnect`` can
classify the failure.
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
    """If ``_connect_lock`` is None at the snapshot point (foreign
    thread nulled it before the async with), raise InterfaceError —
    NOT AttributeError.
    """
    conn = _make_connection()
    # Force the no-lock branch:
    conn._connect_lock = None

    # Bypass the lazy creation: the first ``if`` creates the lock; we
    # then null it immediately to simulate the foreign-thread race.
    original_is_none_check = type(conn).__dict__["_get_async_connection"]

    # Easier: patch out the asyncio.Lock factory so it returns None.
    real_lock_cls = asyncio.Lock

    def _null_lock() -> object:
        # Return None so the snapshot in _get_async_connection sees
        # the same shape a foreign-thread force_close_transport
        # would produce.
        return None

    # The implementation does `self._connect_lock = asyncio.Lock()`
    # then snapshots to a local. We patch asyncio.Lock to a factory
    # that returns None so the snapshot is None.
    asyncio.Lock = _null_lock  # type: ignore[misc,assignment]
    try:
        with pytest.raises(InterfaceError, match="Connection is closed"):
            await conn._get_async_connection()
    finally:
        asyncio.Lock = real_lock_cls  # type: ignore[misc]

    # Quiet the unused-import nag.
    _ = original_is_none_check


@pytest.mark.asyncio
async def test_get_async_connection_raises_interface_error_on_nulled_inner() -> None:
    """If ``_async_conn`` is nulled between the lock release and the
    return read, the snapshot-to-local catches None and raises
    InterfaceError — NOT delivering None to the caller.
    """
    conn = _make_connection()

    # Pre-populate _async_conn via a fake _build_and_connect, then
    # null it just before the snapshot would happen. Easier: mock
    # _build_and_connect to set conn._async_conn to None.
    fake_inner = MagicMock()

    async def fake_build(*_args: object, **_kwargs: object) -> object:
        # Return the fake inner. The implementation then immediately
        # writes ``self._async_conn = await _build_and_connect(...)``.
        # We simulate a foreign-thread force_close_transport nulling
        # ``_async_conn`` between the assignment and the snapshot
        # below by patching the snapshot location externally — but
        # the simplest reliable test sets the inner to None via a
        # post-build hook.
        return fake_inner

    # Patch _build_and_connect in the connection module.
    import dqlitedbapi.connection as conn_mod

    orig = conn_mod._build_and_connect

    async def patched_build(*args: object, **kwargs: object) -> object:
        result = await fake_build(*args, **kwargs)
        # Null after the inner returns. The implementation will assign
        # ``self._async_conn = await ...`` then run the late-publish
        # block, then snapshot. To exercise the foreign-thread race we
        # need the null to happen between the assignment and the
        # snapshot — pre-arrange by patching the snapshot location.
        return result

    conn_mod._build_and_connect = patched_build  # type: ignore[assignment]
    try:
        # First call should succeed.
        inner = await conn._get_async_connection()
        assert inner is fake_inner

        # Now simulate the race: null _async_conn AND clear the lock
        # so the next call retries the build but encounters our null.
        conn._async_conn = None
        conn._connect_lock = None

        # Patch the build to null _async_conn right after assignment
        # (simulating the race after the lock release).
        async def race_build(*args: object, **kwargs: object) -> object:
            conn._async_conn = None  # foreign-thread race
            return None  # match: snapshot will be None

        conn_mod._build_and_connect = race_build  # type: ignore[assignment]

        with pytest.raises(InterfaceError, match="Connection is closed"):
            await conn._get_async_connection()
    finally:
        conn_mod._build_and_connect = orig
