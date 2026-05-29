"""A cancel mid-fetch invalidates the connection: the next call on a sibling cursor raises
``InterfaceError`` / ``OperationalError`` (not a hang, not stale rows)."""

from __future__ import annotations

import asyncio

import pytest

from dqlitedbapi.aio import aconnect
from dqlitedbapi.exceptions import InterfaceError, OperationalError

# Large recursive CTE so a tight outer timeout lands inside the await on a continuation read.
_BIG_CTE = """
WITH RECURSIVE seq(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 5000000
)
SELECT n FROM seq
"""


@pytest.mark.integration
class TestAsyncFetchmanyCancel:
    async def test_cancel_during_execute_of_big_select_invalidates_connection(
        self, cluster_address: str
    ) -> None:
        """Cancel during ``execute`` of a multi-frame SELECT invalidates the connection."""
        conn = await aconnect(cluster_address, timeout=2.0)
        try:
            cur = conn.cursor()
            with pytest.raises((asyncio.CancelledError, asyncio.TimeoutError)):
                async with asyncio.timeout(0.05):
                    await cur.execute(_BIG_CTE)

            with pytest.raises((InterfaceError, OperationalError)):
                await cur.execute("SELECT 1")
        finally:
            await conn.close()

    async def test_cancel_during_async_for_invalidates_connection(
        self, cluster_address: str
    ) -> None:
        """Cancel inside an ``async for`` over the cursor invalidates the connection."""
        conn = await aconnect(cluster_address, timeout=2.0)
        try:
            cur = conn.cursor()
            collected: list[int] = []
            with pytest.raises((asyncio.CancelledError, asyncio.TimeoutError)):
                async with asyncio.timeout(0.05):
                    await cur.execute(_BIG_CTE)
                    async for row in cur:
                        collected.append(row[0])

            with pytest.raises((InterfaceError, OperationalError)):
                await cur.execute("SELECT 1")
        finally:
            await conn.close()
