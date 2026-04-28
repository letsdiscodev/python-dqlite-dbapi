"""Pin the async sibling of ``test_returning.py`` zero-row contract.

UPDATE/DELETE ... WHERE no-match RETURNING must leave the async cursor
in a clean PEP 249 post-execute state: ``rowcount == 0``,
``fetchone() is None``, ``fetchall() == []``.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.aio import aconnect


@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_update_returning_zero_rows(cluster_address: str) -> None:
    conn = await aconnect(cluster_address, database="aio_upd_ret_zero")
    try:
        cursor = conn.cursor()
        await cursor.execute("CREATE TABLE rz (id INTEGER PRIMARY KEY, v INT)")
        await cursor.execute("INSERT INTO rz (id, v) VALUES (1, 100)")
        await cursor.execute("UPDATE rz SET v = ? WHERE id = ? RETURNING id, v", (200, 999))
        assert cursor.rowcount == 0
        assert await cursor.fetchone() is None
        assert await cursor.fetchall() == []
        await cursor.execute("DROP TABLE rz")
    finally:
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_delete_returning_zero_rows(cluster_address: str) -> None:
    conn = await aconnect(cluster_address, database="aio_del_ret_zero")
    try:
        cursor = conn.cursor()
        await cursor.execute("CREATE TABLE rz (id INTEGER PRIMARY KEY)")
        await cursor.execute("DELETE FROM rz WHERE id = 999 RETURNING id")
        assert cursor.rowcount == 0
        assert await cursor.fetchone() is None
        assert await cursor.fetchall() == []
        await cursor.execute("DROP TABLE rz")
    finally:
        await conn.close()
