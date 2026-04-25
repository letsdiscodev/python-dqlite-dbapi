"""PEP 249 §6.1.2: cursors created from the same connection share that
connection's transaction visibility. Pin that two cursors on one
connection see each other's uncommitted writes inside a transaction,
and that ROLLBACK on one cursor is observed by the other.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import connect
from dqlitedbapi.aio import aconnect


def test_sync_two_cursors_share_uncommitted_writes(cluster_address: str) -> None:
    conn = connect(cluster_address, timeout=2.0)
    try:
        setup = conn.cursor()
        setup.execute("DROP TABLE IF EXISTS test_two_cursors")
        setup.execute("CREATE TABLE test_two_cursors (n INTEGER PRIMARY KEY)")
        cur_a = conn.cursor()
        cur_b = conn.cursor()

        cur_a.execute("BEGIN")
        cur_a.execute("INSERT INTO test_two_cursors (n) VALUES (1)")
        cur_b.execute("SELECT n FROM test_two_cursors WHERE n = 1")
        assert cur_b.fetchone() == (1,)
        cur_a.execute("ROLLBACK")
        cur_b.execute("SELECT n FROM test_two_cursors WHERE n = 1")
        assert cur_b.fetchone() is None
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_async_two_cursors_share_uncommitted_writes(
    cluster_address: str,
) -> None:
    conn = await aconnect(cluster_address, timeout=2.0)
    try:
        setup = conn.cursor()
        await setup.execute("DROP TABLE IF EXISTS test_two_cursors_aio")
        await setup.execute("CREATE TABLE test_two_cursors_aio (n INTEGER PRIMARY KEY)")
        cur_a = conn.cursor()
        cur_b = conn.cursor()

        await cur_a.execute("BEGIN")
        await cur_a.execute("INSERT INTO test_two_cursors_aio (n) VALUES (5)")
        await cur_b.execute("SELECT n FROM test_two_cursors_aio WHERE n = 5")
        assert await cur_b.fetchone() == (5,)
        await cur_a.execute("ROLLBACK")
        await cur_b.execute("SELECT n FROM test_two_cursors_aio WHERE n = 5")
        assert await cur_b.fetchone() is None
    finally:
        await conn.close()
