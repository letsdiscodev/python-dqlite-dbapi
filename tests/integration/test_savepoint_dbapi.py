"""SAVEPOINT round-trip pins at the dbapi layer.

ISSUE-701 added SAVEPOINT integration coverage for the SQLAlchemy
adapter. The dbapi layer had no equivalent — a future change to the
SQL classifier or to the in-transaction flag tracking could silently
break SAVEPOINT routing.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import connect
from dqlitedbapi.aio import aconnect


def test_sync_savepoint_roundtrip_partial_rollback(cluster_address: str) -> None:
    conn = connect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_sp_dbapi")
        cur.execute("CREATE TABLE test_sp_dbapi (n INTEGER PRIMARY KEY)")
        cur.execute("BEGIN")
        cur.execute("INSERT INTO test_sp_dbapi (n) VALUES (1)")
        cur.execute("SAVEPOINT sp1")
        cur.execute("INSERT INTO test_sp_dbapi (n) VALUES (2)")
        cur.execute("ROLLBACK TO SAVEPOINT sp1")
        cur.execute("RELEASE SAVEPOINT sp1")
        cur.execute("COMMIT")

        cur.execute("SELECT n FROM test_sp_dbapi ORDER BY n")
        assert cur.fetchall() == [(1,)]
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_async_savepoint_roundtrip_release(cluster_address: str) -> None:
    conn = await aconnect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS test_sp_dbapi_aio")
        await cur.execute("CREATE TABLE test_sp_dbapi_aio (n INTEGER PRIMARY KEY)")
        await cur.execute("BEGIN")
        await cur.execute("INSERT INTO test_sp_dbapi_aio (n) VALUES (10)")
        await cur.execute("SAVEPOINT sp_a")
        await cur.execute("INSERT INTO test_sp_dbapi_aio (n) VALUES (20)")
        await cur.execute("RELEASE SAVEPOINT sp_a")
        await cur.execute("COMMIT")

        await cur.execute("SELECT n FROM test_sp_dbapi_aio ORDER BY n")
        assert await cur.fetchall() == [(10,), (20,)]
    finally:
        await conn.close()
