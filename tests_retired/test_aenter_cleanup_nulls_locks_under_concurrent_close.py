"""__aenter__ and aconnect() cleanup arms null the lazy locks even when a concurrent
_closed flip makes close() short-circuit, so reuse on a new loop takes the fresh-connect path."""

from __future__ import annotations

import asyncio
import weakref

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


def _make_dummy_conn() -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._address = "host:1234"
    conn._database = "x"
    conn._closed_flag = [False]
    conn._async_conn = None
    conn._connect_lock = None
    conn._op_lock = None
    conn._loop_ref = None
    conn._transaction_owner = None
    conn._closed = False
    return conn


@pytest.mark.asyncio
async def test_aenter_cleanup_nulls_locks_when_close_short_circuits() -> None:
    conn = _make_dummy_conn()

    # Lazy locks bound to this loop, as _ensure_locks would have done before connect() failed.
    loop = asyncio.get_running_loop()
    conn._connect_lock = asyncio.Lock()
    conn._op_lock = asyncio.Lock()
    conn._loop_ref = weakref.ref(loop)

    original_error = RuntimeError("simulated connect failure")

    async def fail_connect() -> None:
        # Concurrent close flipped _closed=True while this connect was in flight.
        conn._closed = True
        raise original_error

    async def short_circuit_close() -> None:
        # Mimic close()'s top-of-method short-circuit: returns without nulling the locks.
        if conn._closed:
            return

    conn.connect = fail_connect
    conn.close = short_circuit_close

    with pytest.raises(RuntimeError) as ei:
        async with conn:
            pytest.fail("__aenter__ must raise; we never reach this")
    assert ei.value is original_error

    assert conn._connect_lock is None, "leftover _connect_lock leaks loop binding"
    assert conn._op_lock is None, "leftover _op_lock leaks loop binding"
    assert conn._loop_ref is None, "leftover _loop_ref leaks loop binding"


@pytest.mark.asyncio
async def test_aconnect_cleanup_nulls_locks_when_close_short_circuits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dqlitedbapi import aio as aio_pkg

    captured: dict[str, AsyncConnection] = {}
    loop = asyncio.get_running_loop()

    class _FakeAsyncConnection:
        def __init__(self, *args: object, **kwargs: object) -> None:
            real = _make_dummy_conn()
            real._connect_lock = asyncio.Lock()
            real._op_lock = asyncio.Lock()
            real._loop_ref = weakref.ref(loop)
            captured["conn"] = real
            real_ref = real

            async def fail_connect() -> None:
                real_ref._closed = True
                raise RuntimeError("simulated connect failure")

            async def short_circuit_close() -> None:
                if real_ref._closed:
                    return

            real.connect = fail_connect
            real.close = short_circuit_close
            object.__setattr__(self, "_real", real)

        def __getattr__(self, name: str) -> object:
            return getattr(self._real, name)

        def __setattr__(self, name: str, value: object) -> None:
            setattr(self._real, name, value)

    monkeypatch.setattr(aio_pkg, "AsyncConnection", _FakeAsyncConnection)

    with pytest.raises(RuntimeError):
        await aio_pkg.aconnect("host:1234", database="x")

    real = captured["conn"]
    assert real._connect_lock is None, "aconnect cleanup leaked _connect_lock"
    assert real._op_lock is None, "aconnect cleanup leaked _op_lock"
    assert real._loop_ref is None, "aconnect cleanup leaked _loop_ref"
