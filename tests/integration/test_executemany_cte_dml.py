"""executemany must admit ``WITH cte AS (...) INSERT/UPDATE/DELETE``
statements. The classifier had been rejecting them at the dbapi gate
because the leading ``WITH`` keyword tripped ``_is_row_returning``
while ``_is_dml_with_returning`` only matched the bare DML prefixes.

Forcing users into a manual loop is also a concurrency-safety
regression: the loop forfeits executemany's atomic op_lock hold and
lets concurrent tasks slip statements between iterations.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import connect
from dqlitedbapi.aio import aconnect


def test_sync_executemany_admits_cte_prefixed_insert(cluster_address: str) -> None:
    conn = connect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_em_cte")
        cur.execute("CREATE TABLE test_em_cte (n INTEGER PRIMARY KEY, k TEXT)")
        cur.executemany(
            "WITH src(n, k) AS (VALUES (?, ?)) INSERT INTO test_em_cte (n, k) SELECT n, k FROM src",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        cur.execute("SELECT n, k FROM test_em_cte ORDER BY n")
        rows = cur.fetchall()
        assert rows == [(1, "a"), (2, "b"), (3, "c")]
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_async_executemany_admits_cte_prefixed_delete(cluster_address: str) -> None:
    conn = await aconnect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS test_em_cte_aio")
        await cur.execute("CREATE TABLE test_em_cte_aio (n INTEGER PRIMARY KEY)")
        await cur.executemany(
            "INSERT INTO test_em_cte_aio (n) VALUES (?)",
            [(1,), (2,), (3,)],
        )
        await cur.executemany(
            "WITH target(n) AS (VALUES (?)) "
            "DELETE FROM test_em_cte_aio WHERE n IN (SELECT n FROM target)",
            [(2,)],
        )
        await cur.execute("SELECT n FROM test_em_cte_aio ORDER BY n")
        rows = await cur.fetchall()
        assert rows == [(1,), (3,)]
    finally:
        await conn.close()


def test_sync_executemany_still_rejects_cte_select(cluster_address: str) -> None:
    """A CTE-prefixed SELECT must still be rejected — only DML
    behind a CTE is admitted."""
    from dqlitedbapi import ProgrammingError

    conn = connect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_em_cte_sel")
        cur.execute("CREATE TABLE test_em_cte_sel (n INTEGER)")
        with pytest.raises(ProgrammingError):
            cur.executemany(
                "WITH src(n) AS (VALUES (?)) SELECT n FROM src",
                [(1,)],
            )
    finally:
        conn.close()
