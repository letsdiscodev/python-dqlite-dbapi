"""An async connection belongs to the loop it first ran on."""

from __future__ import annotations

import asyncio

import pytest

from dqlitedbapi import InterfaceError
from dqlitedbapi.aio import AsyncConnection, aconnect

pytestmark = pytest.mark.integration


def test_async_connection_rejects_use_from_another_loop(cluster_address: str) -> None:
    async def open_and_use() -> AsyncConnection:
        conn = await aconnect(cluster_address)
        cur = conn.cursor()
        await cur.execute("SELECT 1")
        assert await cur.fetchone() == (1,)
        return conn

    conn = asyncio.run(open_and_use())

    async def reuse() -> None:
        with pytest.raises(InterfaceError, match="different event loop"):
            await conn.cursor().execute("SELECT 1")

    try:
        asyncio.run(reuse())
    finally:
        conn.force_close_transport()
