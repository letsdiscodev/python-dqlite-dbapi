"""``arraysize = 0`` is accepted (stdlib sqlite3 parity) and makes ``fetchmany()``
(which defaults its size to ``arraysize``) return ``[]`` without consuming rows."""

from __future__ import annotations

import pytest

import dqlitedbapi


@pytest.mark.integration
def test_sync_arraysize_zero_fetchmany_returns_empty(cluster_address: str) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS arrsz0")
        cur.execute("CREATE TABLE arrsz0 (v INTEGER)")
        cur.executemany("INSERT INTO arrsz0 VALUES (?)", [(1,), (2,), (3,)])
        conn.commit()

        cur.execute("SELECT v FROM arrsz0 ORDER BY v")
        cur.arraysize = 0
        assert cur.fetchmany() == []  # arraysize=0 -> empty chunk, nothing consumed
        assert cur.fetchall() == [(1,), (2,), (3,)]  # rows still available


@pytest.mark.integration
async def test_async_arraysize_zero_fetchmany_returns_empty(cluster_address: str) -> None:
    from dqlitedbapi.aio import aconnect

    conn = await aconnect(cluster_address)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS arrsz0_async")
        await cur.execute("CREATE TABLE arrsz0_async (v INTEGER)")
        await cur.executemany("INSERT INTO arrsz0_async VALUES (?)", [(1,), (2,)])
        await conn.commit()

        await cur.execute("SELECT v FROM arrsz0_async ORDER BY v")
        cur.arraysize = 0
        assert await cur.fetchmany() == []
        assert await cur.fetchall() == [(1,), (2,)]
    finally:
        await conn.close()
