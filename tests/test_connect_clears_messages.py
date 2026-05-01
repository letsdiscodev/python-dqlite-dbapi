"""Pin: ``Connection.connect()`` and ``AsyncConnection.connect()``
clear ``Connection.messages`` first, like every other public
Connection method does. PEP 249 §6.4 says ``messages`` is cleared
by all standard methods; ``connect()`` is a dqlite extension but
the project-wide uniformity discipline extends here too.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


def test_sync_connect_clears_messages_even_when_connect_raises() -> None:
    """Pre-populate ``messages`` then drive ``connect()``. The
    connect attempt will raise (no real cluster), but ``messages``
    must already be cleared by the time the body raises."""
    conn = Connection("localhost:9999", timeout=0.1)
    conn.messages.append((RuntimeError, "stale"))
    assert conn.messages == [(RuntimeError, "stale")]

    with pytest.raises(Exception):  # noqa: PT011, BLE001, B017
        conn.connect()

    assert conn.messages == [], (
        "Connection.connect() must clear messages first; project-wide "
        "uniformity invariant matches PEP 249 §6.4 for the standard methods."
    )


@pytest.mark.asyncio
async def test_async_connect_clears_messages_even_when_connect_raises() -> None:
    aconn = AsyncConnection("localhost:9999", database="x")
    aconn.messages.append((RuntimeError, "stale"))
    assert aconn.messages == [(RuntimeError, "stale")]

    with pytest.raises(Exception):  # noqa: PT011, BLE001, B017
        await aconn.connect()

    assert aconn.messages == [], (
        "AsyncConnection.connect() must clear messages first; mirrors the "
        "sync sibling and the standard-method discipline."
    )
