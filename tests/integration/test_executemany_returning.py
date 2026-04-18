"""Integration test for ISSUE-57: executemany must accumulate rows from
every parameter set when the statement has a RETURNING clause."""

import pytest

import dqlitedbapi
from dqlitedbapi.aio import aconnect


@pytest.mark.integration
def test_sync_executemany_returning_accumulates_rows(cluster_address: str) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS emany_ret")
        cur.execute("CREATE TABLE emany_ret (id INTEGER PRIMARY KEY, x TEXT)")
        params = [("a",), ("b",), ("c",)]
        cur.executemany("INSERT INTO emany_ret (x) VALUES (?) RETURNING id", params)
        assert cur.rowcount == 3
        rows = cur.fetchall()
        assert len(rows) == 3
        # ids must be 1, 2, 3 in insert order.
        assert [r[0] for r in rows] == [1, 2, 3]
        conn.rollback()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_executemany_returning_accumulates_rows(
    cluster_address: str,
) -> None:
    conn = await aconnect(cluster_address)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS emany_ret_async")
        await cur.execute("CREATE TABLE emany_ret_async (id INTEGER PRIMARY KEY, x TEXT)")
        params = [("alpha",), ("beta",), ("gamma",), ("delta",)]
        await cur.executemany("INSERT INTO emany_ret_async (x) VALUES (?) RETURNING id, x", params)
        assert cur.rowcount == 4
        rows = await cur.fetchall()
        assert len(rows) == 4
        assert [r[1] for r in rows] == ["alpha", "beta", "gamma", "delta"]
        await conn.rollback()
    finally:
        await conn.close()
