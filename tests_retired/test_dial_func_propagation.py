"""``dial_func`` is exposed on every dbapi entry point (sync + async, top-level
connect + class constructor), mirroring the client-layer propagation.
"""

from __future__ import annotations

import asyncio

import dqliteclient
import dqlitedbapi
import dqlitedbapi.aio as aio


async def _custom_dial_func(
    address: str, *, timeout: float | None = None
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    raise RuntimeError("never called in unit test")


def test_dbapi_top_level_connect_accepts_dial_func() -> None:
    """``connect(..., dial_func=fn)`` accepts the kwarg (lazy, no live server)."""
    conn = dqlitedbapi.connect(
        "127.0.0.1:9999",
        dial_func=_custom_dial_func,
    )
    try:
        assert getattr(conn, "_dial_func", None) is _custom_dial_func
    finally:
        conn.close()


def test_dbapi_connection_class_accepts_dial_func() -> None:
    conn = dqlitedbapi.Connection(
        "127.0.0.1:9999",
        dial_func=_custom_dial_func,
    )
    try:
        assert conn._dial_func is _custom_dial_func
    finally:
        conn.close()


def test_aio_top_level_connect_accepts_dial_func() -> None:
    conn = aio.connect(
        "127.0.0.1:9999",
        dial_func=_custom_dial_func,
    )
    assert conn._dial_func is _custom_dial_func


def test_aio_async_connection_class_accepts_dial_func() -> None:
    conn = aio.AsyncConnection(
        "127.0.0.1:9999",
        dial_func=_custom_dial_func,
    )
    assert conn._dial_func is _custom_dial_func


def test_dial_func_type_re_exported_from_dbapi() -> None:
    """``dqlitedbapi.DialFunc`` is the same alias the client layer exposes (single SSOT)."""
    assert dqlitedbapi.DialFunc is dqliteclient.DialFunc
    assert "DialFunc" in dqlitedbapi.__all__


def test_dial_func_type_re_exported_from_aio() -> None:
    assert aio.DialFunc is dqliteclient.DialFunc
    assert "DialFunc" in aio.__all__
