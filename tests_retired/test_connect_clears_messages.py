"""Pin: ``connect()`` clears ``Connection.messages`` first, like every standard
method does (PEP 249 §6.4). connect() is a dqlite extension but follows the same rule.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection

_RUNTIMEERROR_STALE: tuple[type[Exception], Exception] = (RuntimeError, RuntimeError("stale"))


def test_sync_connect_clears_messages_even_when_connect_raises() -> None:
    """connect() must clear messages before its body raises (no real cluster here)."""
    conn = Connection("localhost:9999", timeout=0.1)
    conn.messages.append(_RUNTIMEERROR_STALE)
    assert conn.messages == [_RUNTIMEERROR_STALE]

    with pytest.raises(Exception):  # noqa: PT011, BLE001, B017
        conn.connect()

    assert conn.messages == [], (
        "Connection.connect() must clear messages first; project-wide "
        "uniformity invariant matches PEP 249 §6.4 for the standard methods."
    )


async def test_async_connect_clears_messages_even_when_connect_raises() -> None:
    aconn = AsyncConnection("localhost:9999", database="x")
    aconn.messages.append(_RUNTIMEERROR_STALE)
    assert aconn.messages == [_RUNTIMEERROR_STALE]

    with pytest.raises(Exception):  # noqa: PT011, BLE001, B017
        await aconn.connect()

    assert aconn.messages == [], (
        "AsyncConnection.connect() must clear messages first; mirrors the "
        "sync sibling and the standard-method discipline."
    )
