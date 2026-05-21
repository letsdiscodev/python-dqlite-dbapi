"""Pin: ``dial_func`` is exposed on every dbapi entry point (sync +
async, top-level connect + class constructor) symmetric with the
client-layer propagation that already supports the knob on every
connection-construction site.

Mirror of the ``dial_timeout`` / ``attempt_timeout`` propagation
precedent. The SA dialect's ``_CONNECT_KWARG_ALLOWED`` extension is
handled in the SA repo by a separate agent.
"""

from __future__ import annotations

import asyncio

import dqliteclient
import dqlitedbapi
import dqlitedbapi.aio as aio


async def _custom_dial_func(
    address: str, *, timeout: float | None = None
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Stub dialer for signature-shape pinning."""
    raise RuntimeError("never called in unit test")


def test_dbapi_top_level_connect_accepts_dial_func() -> None:
    """``dqlitedbapi.connect(..., dial_func=fn)`` does not raise
    ``TypeError`` for the kwarg. The construction is lazy — connect
    is not exercised against a live server."""
    conn = dqlitedbapi.connect(
        "127.0.0.1:9999",
        dial_func=_custom_dial_func,
    )
    try:
        assert getattr(conn, "_dial_func", None) is _custom_dial_func
    finally:
        conn.close()


def test_dbapi_connection_class_accepts_dial_func() -> None:
    """``dqlitedbapi.Connection(..., dial_func=fn)`` constructor
    parity."""
    conn = dqlitedbapi.Connection(
        "127.0.0.1:9999",
        dial_func=_custom_dial_func,
    )
    try:
        assert conn._dial_func is _custom_dial_func
    finally:
        conn.close()


def test_aio_top_level_connect_accepts_dial_func() -> None:
    """``dqlitedbapi.aio.connect(..., dial_func=fn)`` parity."""
    conn = aio.connect(
        "127.0.0.1:9999",
        dial_func=_custom_dial_func,
    )
    assert conn._dial_func is _custom_dial_func


def test_aio_async_connection_class_accepts_dial_func() -> None:
    """``aio.AsyncConnection(..., dial_func=fn)`` constructor parity."""
    conn = aio.AsyncConnection(
        "127.0.0.1:9999",
        dial_func=_custom_dial_func,
    )
    assert conn._dial_func is _custom_dial_func


def test_dial_func_type_re_exported_from_dbapi() -> None:
    """``dqlitedbapi.DialFunc`` is the same type alias the client
    layer exposes — single SSOT."""
    assert dqlitedbapi.DialFunc is dqliteclient.DialFunc
    assert "DialFunc" in dqlitedbapi.__all__


def test_dial_func_type_re_exported_from_aio() -> None:
    """``dqlitedbapi.aio.DialFunc`` parity."""
    assert aio.DialFunc is dqliteclient.DialFunc
    assert "DialFunc" in aio.__all__
